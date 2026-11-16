use std::net::SocketAddr;
use std::sync::Arc;

use quinn::{ClientConfig, Endpoint, ServerConfig};
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};

use crate::service::{IdentifyRequest, IngestError, IngestResponse, IngestService, Result};

pub const QUIC_OP_INGEST: u8 = 1;
pub const QUIC_OP_IDENTIFY: u8 = 2;
pub const QUIC_OP_REPLAY: u8 = 3;

pub struct QuicIngestHandle {
    pub endpoint: Endpoint,
    pub local_addr: SocketAddr,
    pub cert_der: Vec<u8>,
}

/// Spawns a UDP QUIC listener (`quinn`) sharing the exact same `IngestService` handler as HTTP.
pub fn spawn_quic_listener(
    bind_addr: SocketAddr,
    service: Arc<IngestService>,
) -> Result<QuicIngestHandle> {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()])
        .map_err(|e| IngestError::Quic(format!("rcgen error: {e}")))?;
    let cert_der_bytes = cert.cert.der().to_vec();
    let cert_der = CertificateDer::from(cert_der_bytes.clone());
    let priv_key = PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());

    let mut server_config = ServerConfig::with_single_cert(vec![cert_der], priv_key.into())
        .map_err(|e| IngestError::Quic(format!("tls server config error: {e}")))?;
    if let Some(transport) = Arc::get_mut(&mut server_config.transport) {
        transport.max_concurrent_bidi_streams(256u32.into());
    }

    let endpoint = Endpoint::server(server_config, bind_addr)
        .map_err(|e| IngestError::Quic(format!("quic bind error: {e}")))?;
    let local_addr = endpoint
        .local_addr()
        .map_err(|e| IngestError::Quic(format!("quic local_addr error: {e}")))?;

    let accept_ep = endpoint.clone();
    tokio::spawn(async move {
        while let Some(incoming) = accept_ep.accept().await {
            let svc = service.clone();
            tokio::spawn(async move {
                if let Ok(conn) = incoming.await {
                    while let Ok((mut send, mut recv)) = conn.accept_bi().await {
                        let svc_stream = svc.clone();
                        tokio::spawn(async move {
                            if let Ok(req_bytes) = recv.read_to_end(32 * 1024 * 1024).await {
                                let reply = handle_quic_frame(&svc_stream, &req_bytes).await;
                                let _ = send.write_all(&reply).await;
                                let _ = send.finish();
                            }
                        });
                    }
                }
            });
        }
    });

    Ok(QuicIngestHandle {
        endpoint,
        local_addr,
        cert_der: cert_der_bytes,
    })
}

async fn handle_quic_frame(service: &IngestService, frame: &[u8]) -> Vec<u8> {
    if frame.len() < 7 {
        return br#"{"status":"error","message":"truncated quic frame"}"#.to_vec();
    }
    let op = frame[0];
    let key_len = u16::from_le_bytes([frame[1], frame[2]]) as usize;
    if frame.len() < 3 + key_len + 4 {
        return br#"{"status":"error","message":"invalid quic key length"}"#.to_vec();
    }
    let Ok(api_key) = std::str::from_utf8(&frame[3..3 + key_len]) else {
        return br#"{"status":"error","message":"invalid utf8 api key"}"#.to_vec();
    };
    let pos = 3 + key_len;
    let body_len =
        u32::from_le_bytes([frame[pos], frame[pos + 1], frame[pos + 2], frame[pos + 3]]) as usize;
    if frame.len() < pos + 4 + body_len {
        return br#"{"status":"error","message":"truncated quic body"}"#.to_vec();
    }
    let body = &frame[pos + 4..pos + 4 + body_len];

    match op {
        QUIC_OP_INGEST => match service.ingest_raw(api_key, None, body).await {
            Ok(r) => serde_json::to_vec(&r).unwrap_or_default(),
            Err(e) => serde_json::to_vec(&serde_json::json!({
                "status": "error",
                "message": e.to_string(),
            }))
            .unwrap_or_default(),
        },
        QUIC_OP_IDENTIFY => match serde_json::from_slice::<IdentifyRequest>(body) {
            Ok(req) => match service.identify(api_key, req).await {
                Ok(r) => serde_json::to_vec(&r).unwrap_or_default(),
                Err(e) => serde_json::to_vec(&serde_json::json!({
                    "status": "error",
                    "message": e.to_string(),
                }))
                .unwrap_or_default(),
            },
            Err(e) => serde_json::to_vec(&serde_json::json!({
                "status": "error",
                "message": e.to_string(),
            }))
            .unwrap_or_default(),
        },
        QUIC_OP_REPLAY => match service.ingest_replay(api_key, None, body).await {
            Ok(r) => serde_json::to_vec(&r).unwrap_or_default(),
            Err(e) => serde_json::to_vec(&serde_json::json!({
                "status": "error",
                "message": e.to_string(),
            }))
            .unwrap_or_default(),
        },
        _ => br#"{"status":"error","message":"unknown quic opcode"}"#.to_vec(),
    }
}

/// Helper QUIC client used by SDKs and integration tests to send binary `PLSB` batches over UDP.
pub async fn send_quic_ingest_batch(
    server_addr: SocketAddr,
    cert_der: &[u8],
    api_key: &str,
    payload: &[u8],
) -> Result<IngestResponse> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(CertificateDer::from(cert_der))
        .map_err(|e| IngestError::Quic(format!("root cert add error: {e}")))?;

    let client_cfg = ClientConfig::with_root_certificates(Arc::new(roots))
        .map_err(|e| IngestError::Quic(format!("client cfg error: {e}")))?;
    let mut endpoint = Endpoint::client("127.0.0.1:0".parse().unwrap())
        .map_err(|e| IngestError::Quic(format!("client bind error: {e}")))?;
    endpoint.set_default_client_config(client_cfg);

    let conn = endpoint
        .connect(server_addr, "localhost")
        .map_err(|e| IngestError::Quic(format!("connect init error: {e}")))?
        .await
        .map_err(|e| IngestError::Quic(format!("connect handshake error: {e}")))?;

    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .map_err(|e| IngestError::Quic(format!("open_bi error: {e}")))?;

    let mut frame = Vec::with_capacity(7 + api_key.len() + payload.len());
    frame.push(QUIC_OP_INGEST);
    frame.extend_from_slice(&(api_key.len() as u16).to_le_bytes());
    frame.extend_from_slice(api_key.as_bytes());
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(payload);

    send.write_all(&frame)
        .await
        .map_err(|e| IngestError::Quic(format!("write_all error: {e}")))?;
    send.finish()
        .map_err(|e| IngestError::Quic(format!("finish error: {e}")))?;

    let resp_bytes = recv
        .read_to_end(1024 * 1024)
        .await
        .map_err(|e| IngestError::Quic(format!("read_to_end error: {e}")))?;

    conn.close(0u32.into(), b"done");
    let resp: IngestResponse = serde_json::from_slice(&resp_bytes)?;
    Ok(resp)
}
