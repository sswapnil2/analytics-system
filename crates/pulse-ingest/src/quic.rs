use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use pulse_wire::WireBatch;
use quinn::{ClientConfig, Endpoint, RecvStream, ServerConfig, VarInt};
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use tokio::sync::{Notify, Semaphore};

use crate::service::{IdentifyRequest, IngestError, IngestResponse, IngestService, Result};

pub const QUIC_OP_INGEST: u8 = 1;
pub const QUIC_OP_IDENTIFY: u8 = 2;
pub const QUIC_OP_REPLAY: u8 = 3;

pub const MAX_QUIC_CONNECTIONS: usize = 64;
pub const MAX_QUIC_BIDI_STREAMS: u32 = 8;
pub const MAX_QUIC_KEY_LEN: usize = 256;
pub const MAX_QUIC_BODY_BYTES: usize = 4 * 1024 * 1024;

#[derive(Default)]
struct ActiveTaskCounter {
    active: AtomicUsize,
    idle_notify: Notify,
}

impl ActiveTaskCounter {
    fn enter(self: &Arc<Self>) -> ActiveTaskGuard {
        self.active.fetch_add(1, Ordering::AcqRel);
        ActiveTaskGuard(self.clone())
    }

    async fn wait_zero(&self) {
        loop {
            let notified = self.idle_notify.notified();
            if self.active.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
        }
    }
}

struct ActiveTaskGuard(Arc<ActiveTaskCounter>);

impl Drop for ActiveTaskGuard {
    fn drop(&mut self) {
        if self.0.active.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.idle_notify.notify_waiters();
        }
    }
}

pub struct QuicIngestHandle {
    pub endpoint: Endpoint,
    pub local_addr: SocketAddr,
    pub cert_der: Vec<u8>,
    tasks: Arc<ActiveTaskCounter>,
}

impl QuicIngestHandle {
    /// Closes the QUIC endpoint and waits for all in-flight stream handlers to drain cleanly.
    pub async fn shutdown(&self) {
        self.endpoint.close(0u32.into(), b"shutdown");
        let _ = tokio::time::timeout(Duration::from_secs(5), self.tasks.wait_zero()).await;
    }
}

/// Spawns a bounded UDP QUIC listener (`quinn`) sharing `IngestService`.
///
/// Disabled by default in `pulse-node`; when enabled via `--quic-addr`, enforces connection,
/// stream, window, and idle limits and authenticates the API key BEFORE reading the body.
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
        transport.max_concurrent_bidi_streams(MAX_QUIC_BIDI_STREAMS.into());
        transport.max_concurrent_uni_streams(0u32.into());
        transport.stream_receive_window(VarInt::from_u32(MAX_QUIC_BODY_BYTES as u32 + 4096));
        transport.receive_window(VarInt::from_u32(8 * 1024 * 1024));
        if let Ok(idle) = Duration::from_secs(10).try_into() {
            transport.max_idle_timeout(Some(idle));
        }
    }

    let endpoint = Endpoint::server(server_config, bind_addr)
        .map_err(|e| IngestError::Quic(format!("quic bind error: {e}")))?;
    let local_addr = endpoint
        .local_addr()
        .map_err(|e| IngestError::Quic(format!("quic local_addr error: {e}")))?;

    let conn_sem = Arc::new(Semaphore::new(MAX_QUIC_CONNECTIONS));
    let tasks = Arc::new(ActiveTaskCounter::default());
    let tasks_accept = tasks.clone();
    let accept_ep = endpoint.clone();

    tokio::spawn(async move {
        while let Some(incoming) = accept_ep.accept().await {
            let Ok(conn_permit) = conn_sem.clone().try_acquire_owned() else {
                incoming.refuse();
                continue;
            };
            let svc = service.clone();
            let tasks_conn = tasks_accept.clone();
            tokio::spawn(async move {
                let _conn_permit = conn_permit;
                let conn = match tokio::time::timeout(Duration::from_secs(5), incoming).await {
                    Ok(Ok(c)) => c,
                    _ => return,
                };
                while let Ok((mut send, mut recv)) = conn.accept_bi().await {
                    let svc_stream = svc.clone();
                    let guard = tasks_conn.enter();
                    tokio::spawn(async move {
                        let _guard = guard;
                        let reply = tokio::time::timeout(
                            Duration::from_secs(10),
                            handle_quic_stream(&svc_stream, &mut recv),
                        )
                        .await
                        .unwrap_or_else(|_| {
                            let _ = recv.stop(408u32.into());
                            br#"{"status":"error","message":"quic stream timeout"}"#.to_vec()
                        });
                        let _ = send.write_all(&reply).await;
                        let _ = send.finish();
                    });
                }
            });
        }
    });

    Ok(QuicIngestHandle {
        endpoint,
        local_addr,
        cert_der: cert_der_bytes,
        tasks,
    })
}

async fn read_exact_bounded(recv: &mut RecvStream, len: usize) -> std::result::Result<Vec<u8>, ()> {
    let mut out = Vec::with_capacity(len.min(64 * 1024));
    let mut remaining = len;
    let mut buf = [0u8; 16 * 1024];
    while remaining > 0 {
        let want = remaining.min(buf.len());
        let Some(n) = recv.read(&mut buf[..want]).await.ok().flatten() else {
            return Err(());
        };
        if n == 0 {
            return Err(());
        }
        out.extend_from_slice(&buf[..n]);
        remaining -= n;
    }
    Ok(out)
}

async fn handle_quic_stream(service: &IngestService, recv: &mut RecvStream) -> Vec<u8> {
    // 1. Read ONLY 3-byte opcode + key_len header first
    let mut hdr = [0u8; 3];
    if recv.read_exact(&mut hdr).await.is_err() {
        let _ = recv.stop(400u32.into());
        return br#"{"status":"error","message":"truncated quic header"}"#.to_vec();
    }
    let op = hdr[0];
    let key_len = u16::from_le_bytes([hdr[1], hdr[2]]) as usize;
    if key_len == 0 || key_len > MAX_QUIC_KEY_LEN {
        let _ = recv.stop(400u32.into());
        return br#"{"status":"error","message":"invalid quic key length"}"#.to_vec();
    }

    // 2. Read ONLY the API key and authenticate BEFORE reading a single byte of the body
    let Ok(key_bytes) = read_exact_bounded(recv, key_len).await else {
        let _ = recv.stop(400u32.into());
        return br#"{"status":"error","message":"truncated quic api key"}"#.to_vec();
    };
    let Ok(api_key) = std::str::from_utf8(&key_bytes) else {
        let _ = recv.stop(400u32.into());
        return br#"{"status":"error","message":"invalid utf8 api key"}"#.to_vec();
    };
    let Some(project_id) = service.governance().authenticate_api_key(api_key).await else {
        let _ = recv.stop(401u32.into());
        return br#"{"status":"error","message":"unauthorized api key"}"#.to_vec();
    };

    // 3. Now read 4-byte body_len and enforce MAX_QUIC_BODY_BYTES before buffering body
    let mut len_bytes = [0u8; 4];
    if recv.read_exact(&mut len_bytes).await.is_err() {
        let _ = recv.stop(400u32.into());
        return br#"{"status":"error","message":"truncated quic body length"}"#.to_vec();
    }
    let body_len = u32::from_le_bytes(len_bytes) as usize;
    if body_len > MAX_QUIC_BODY_BYTES {
        let _ = recv.stop(413u32.into());
        return br#"{"status":"error","message":"quic payload exceeds 4 MiB limit"}"#.to_vec();
    }
    let Ok(body) = read_exact_bounded(recv, body_len).await else {
        let _ = recv.stop(400u32.into());
        return br#"{"status":"error","message":"truncated quic body"}"#.to_vec();
    };

    match op {
        QUIC_OP_INGEST => match WireBatch::decode_auto(&body, None, service.dict_registry()) {
            Ok(batch) => match service
                .ingest_for_project(&project_id, batch, Utc::now().timestamp_millis())
                .await
            {
                Ok(r) => serde_json::to_vec(&r).unwrap_or_default(),
                Err(IngestError::Storage(
                    e @ pulse_storage::StorageError::HotSegmentBackpressure { .. },
                )) => serde_json::to_vec(&serde_json::json!({
                    "status": "error",
                    "code": "hot_segment_backpressure",
                    "message": e.to_string(),
                }))
                .unwrap_or_default(),
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
        QUIC_OP_IDENTIFY => match serde_json::from_slice::<IdentifyRequest>(&body) {
            Ok(req) => match service.identify_for_project(&project_id, req).await {
                Ok(r) => serde_json::to_vec(&r).unwrap_or_default(),
                Err(IngestError::Storage(
                    e @ pulse_storage::StorageError::HotSegmentBackpressure { .. },
                )) => serde_json::to_vec(&serde_json::json!({
                    "status": "error",
                    "code": "hot_segment_backpressure",
                    "message": e.to_string(),
                }))
                .unwrap_or_default(),
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
        QUIC_OP_REPLAY => match service
            .ingest_replay_for_project(&project_id, None, &body)
            .await
        {
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
    if let Ok(val) = serde_json::from_slice::<serde_json::Value>(&resp_bytes) {
        if val.get("status").and_then(|s| s.as_str()) == Some("error") {
            return Err(IngestError::Quic(val.to_string()));
        }
    }
    let resp: IngestResponse = serde_json::from_slice(&resp_bytes)?;
    Ok(resp)
}
