use std::net::IpAddr;

use pulse_node::{start_pulse_node, NodeConfig, NodeRole};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,slatedb=warn")),
        )
        .init();

    let mut cfg = NodeConfig::default();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--seed" => {
                cfg.auto_seed = true;
                if cfg.default_project_id == "proj_default" {
                    cfg.default_project_id = "proj_fintech_in".to_string();
                }
            }
            "--no-seed" => {
                cfg.auto_seed = false;
            }
            "--host" if i + 1 < args.len() => {
                if let Ok(ip) = args[i + 1].parse::<IpAddr>() {
                    cfg.http_addr.set_ip(ip);
                }
                i += 1;
            }
            "--port" if i + 1 < args.len() => {
                if let Ok(p) = args[i + 1].parse::<u16>() {
                    cfg.http_addr.set_port(p);
                }
                i += 1;
            }
            "--quic-addr" if i + 1 < args.len() => {
                if let Ok(addr) = args[i + 1].parse() {
                    cfg.quic_addr = Some(addr);
                }
                i += 1;
            }
            "--no-quic" => {
                cfg.quic_addr = None;
            }
            "--project-id" if i + 1 < args.len() => {
                cfg.default_project_id = args[i + 1].clone();
                i += 1;
            }
            "--data-dir" if i + 1 < args.len() => {
                cfg.data_dir = args[i + 1].clone().into();
                i += 1;
            }
            "--web-dir" if i + 1 < args.len() => {
                cfg.web_dist_dir = args[i + 1].clone().into();
                i += 1;
            }
            "--role" if i + 1 < args.len() => {
                if let Some(r) = NodeRole::parse(&args[i + 1]) {
                    cfg.role = r;
                } else {
                    anyhow::bail!(
                        "invalid --role '{}': expected one of all, ingest, query, compactor",
                        args[i + 1]
                    );
                }
                i += 1;
            }
            _ => {}
        }
        i += 1;
    }

    let node = start_pulse_node(cfg).await?;
    info!(
        url = %format!("http://{}:{}", node.http_addr.ip(), node.http_addr.port()),
        "Pulse Analytics Studio & Engine Ready"
    );

    wait_for_shutdown_signal().await;
    info!("Shutting down Pulse Analytics node gracefully...");
    node.shutdown().await;
    Ok(())
}

async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        if let Ok(mut sigterm) = signal(SignalKind::terminate()) {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = sigterm.recv() => {}
            }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}
