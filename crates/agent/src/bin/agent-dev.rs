//! Temporary dev entry point until crates/r-drbd wires the agent role.
//! Env: NODE_NAME (required), POD_NAMESPACE, CSI_ENDPOINT, R_DRBD_RES_DIR,
//! R_DRBD_HOST_EXEC_PREFIX (space separated), POD_NAME.

use std::path::PathBuf;

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_owned())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .compact()
        .init();
    let cfg = agent::Config {
        node: std::env::var("NODE_NAME").map_err(|_| "NODE_NAME is required")?,
        namespace: env_or("POD_NAMESPACE", model::keys::DEFAULT_NAMESPACE),
        csi_endpoint: env_or("CSI_ENDPOINT", "unix:///csi/csi.sock"),
        res_dir: PathBuf::from(env_or("R_DRBD_RES_DIR", model::keys::RES_DIR)),
        host_exec_prefix: std::env::var("R_DRBD_HOST_EXEC_PREFIX")
            .ok()
            .map(|p| p.split_whitespace().map(str::to_owned).collect::<Vec<_>>())
            .filter(|p| !p.is_empty()),
        pod: std::env::var("POD_NAME").ok(),
    };
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?
        .block_on(agent::run(cfg))?;
    Ok(())
}
