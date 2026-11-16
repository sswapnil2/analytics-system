use pulse_node::{start_pulse_node, NodeConfig};
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
            "--no-seed" => {
                cfg.auto_seed = false;
            }
            "--port" if i + 1 < args.len() => {
                if let Ok(p) = args[i + 1].parse::<u16>() {
                    cfg.http_addr.set_port(p);
                }
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
            "--no-quic" => {
                cfg.quic_addr = None;
            }
            _ => {}
        }
        i += 1;
    }

    let node = start_pulse_node(cfg).await?;
    info!(
        url = %format!("http://localhost:{}", node.http_addr.port()),
        "Pulse Analytics Studio & Engine Ready"
    );

    tokio::signal::ctrl_c().await?;
    info!("Shutting down Pulse Analytics node gracefully...");
    node.shutdown().await;
    Ok(())
}
