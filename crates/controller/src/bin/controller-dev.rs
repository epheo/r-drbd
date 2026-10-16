//! Temporary entry point until crates/r-drbd wires the controller role.

use std::process::ExitCode;

fn env(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_owned())
}

fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_target(false)
        .init();
    // Two controllers must never share an identity, so no default.
    let identity = std::env::var("POD_NAME")
        .ok()
        .or_else(|| {
            std::fs::read_to_string("/etc/hostname")
                .ok()
                .map(|h| h.trim().to_owned())
        })
        .filter(|i| !i.is_empty());
    let Some(identity) = identity else {
        eprintln!("no identity: set POD_NAME or provide /etc/hostname");
        return ExitCode::FAILURE;
    };
    let cfg = controller::Config::new(
        env("POD_NAMESPACE", model::keys::DEFAULT_NAMESPACE),
        env("CSI_ENDPOINT", "unix:///csi/csi.sock"),
        identity,
    );
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("tokio runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    match rt.block_on(controller::run(cfg)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            tracing::error!(error = %e, "controller stopped");
            ExitCode::FAILURE
        }
    }
}
