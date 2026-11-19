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
        let arg = args[i].as_str();
        let next_val = |flag: &str, idx: &mut usize| -> anyhow::Result<&str> {
            *idx += 1;
            args.get(*idx)
                .map(|s| s.as_str())
                .ok_or_else(|| anyhow::anyhow!("missing value for {flag}"))
        };
        match arg {
            "-h" | "--help" => {
                print_usage();
                return Ok(());
            }
            "-V" | "--version" => {
                println!("pulse-node {}", env!("CARGO_PKG_VERSION"));
                return Ok(());
            }
            "--seed" => {
                cfg.auto_seed = true;
                if cfg.default_project_id == "proj_default" {
                    cfg.default_project_id = "proj_fintech_in".to_string();
                }
            }
            "--no-seed" => {
                cfg.auto_seed = false;
            }
            "--host" => {
                let raw = next_val("--host", &mut i)?;
                let ip: IpAddr = raw
                    .parse()
                    .map_err(|e| anyhow::anyhow!("invalid --host '{raw}': {e}"))?;
                cfg.http_addr.set_ip(ip);
            }
            "--port" => {
                let raw = next_val("--port", &mut i)?;
                let p: u16 = raw
                    .parse()
                    .map_err(|e| anyhow::anyhow!("invalid --port '{raw}': {e}"))?;
                cfg.http_addr.set_port(p);
            }
            "--quic-addr" => {
                let raw = next_val("--quic-addr", &mut i)?;
                let addr = raw
                    .parse()
                    .map_err(|e| anyhow::anyhow!("invalid --quic-addr '{raw}': {e}"))?;
                cfg.quic_addr = Some(addr);
            }
            "--no-quic" => {
                cfg.quic_addr = None;
            }
            "--project-id" => {
                cfg.default_project_id = next_val("--project-id", &mut i)?.to_string();
            }
            "--data-dir" => {
                cfg.data_dir = next_val("--data-dir", &mut i)?.into();
            }
            "--web-dir" => {
                cfg.web_dist_dir = next_val("--web-dir", &mut i)?.into();
            }
            "--role" => {
                let raw = next_val("--role", &mut i)?;
                if let Some(r) = NodeRole::parse(raw) {
                    cfg.role = r;
                } else {
                    anyhow::bail!(
                        "invalid --role '{raw}': expected one of all, ingest, query, compactor"
                    );
                }
            }
            other => {
                anyhow::bail!("unknown argument '{other}' (run with --help for usage)");
            }
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

fn print_usage() {
    println!(
        "pulse-node {} — Single-binary product analytics, replay, flags & experimentation\n\
         \n\
         USAGE:\n\
         \tpulse-node [OPTIONS]\n\
         \n\
         OPTIONS:\n\
         \t--host <IP>          HTTP bind address (default: 127.0.0.1, env: PULSE_HOST)\n\
         \t--port <PORT>        HTTP bind port (default: 8080, env: PORT)\n\
         \t--data-dir <PATH>    Data directory (default: ./pulse_data, env: PULSE_DATA_DIR)\n\
         \t--web-dir <PATH>     Studio dist directory (default: ./web/dist, env: PULSE_WEB_DIST_DIR)\n\
         \t--project-id <ID>    Default project ID (default: proj_default, env: PULSE_DEFAULT_PROJECT)\n\
         \t--role <ROLE>        Node role: all | ingest | query | compactor (env: PULSE_NODE_ROLE)\n\
         \t--quic-addr <ADDR>   Enable QUIC ingest listener on <IP:PORT> (env: PULSE_QUIC_ADDR)\n\
         \t--no-quic            Disable QUIC ingest listener (default)\n\
         \t--seed               Seed demo dataset on startup (env: PULSE_SEED=1)\n\
         \t--no-seed            Disable demo dataset seeding (default)\n\
         \t-h, --help           Print help information\n\
         \t-V, --version        Print version information",
        env!("CARGO_PKG_VERSION")
    );
}
