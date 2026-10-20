//! r-drbd: one binary, two roles. The controller runs the reconcilers and the
//! CSI controller plugin; the agent runs the node reconciler and the CSI node
//! plugin. `crds` prints the API for the manifests.

use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "r-drbd", version)]
struct Cli {
    #[command(subcommand)]
    role: Role,
}

#[derive(Subcommand, Debug)]
enum Role {
    /// Reconcile Pools, Volumes and Snapshots; serve the CSI controller plugin.
    Controller {
        #[arg(long, env = "CSI_ENDPOINT", default_value = "unix:///csi/csi.sock")]
        csi_endpoint: String,
        #[arg(long, env = "POD_NAMESPACE", default_value = model::keys::DEFAULT_NAMESPACE)]
        namespace: String,
        /// Lease holder identity; defaults to the hostname (the pod name).
        #[arg(long, env = "POD_NAME")]
        identity: Option<String>,
    },
    /// Reconcile LVM and DRBD on this node; serve the CSI node plugin.
    Agent {
        #[arg(long, env = "NODE_NAME")]
        node: String,
        #[arg(long, env = "CSI_ENDPOINT", default_value = "unix:///csi/csi.sock")]
        csi_endpoint: String,
        #[arg(long, env = "POD_NAMESPACE", default_value = model::keys::DEFAULT_NAMESPACE)]
        namespace: String,
        #[arg(long, env = "R_DRBD_RES_DIR", default_value = model::keys::RES_DIR)]
        res_dir: std::path::PathBuf,
        /// Read at shutdown on hosts without systemd.
        #[arg(long, env = "POD_NAME")]
        pod: Option<String>,
    },
    /// Print the CRDs as a JSON List for kubectl apply.
    Crds,
    /// Run a command inside another mount namespace (the agent's way to use
    /// the host's tools from a scratch image). Not for users.
    #[command(hide = true)]
    HostExec {
        /// Namespace directory of the host init, normally /proc/1/ns.
        ns: String,
        #[arg(last = true, required = true)]
        argv: Vec<String>,
    },
}

/// The image carries only static binaries: each host command (lvm, mkfs,
/// mount) re-execs this binary, which moves into the host's IPC and mount
/// namespaces and execs the host's tool. The agent process itself stays in the
/// container namespace so the in-cluster credentials remain readable.
fn host_exec(ns_dir: &str, argv: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    use rustix::thread::{LinkNameSpaceType, move_into_link_name_space};
    use std::os::fd::AsFd;
    use std::os::unix::process::CommandExt;
    // IPC first: LVM's udev sync waits on a semaphore the host udevd must
    // see, otherwise lvcreate stalls until its udev timeout.
    let ipc = std::fs::File::open(format!("{ns_dir}/ipc"))?;
    let mnt = std::fs::File::open(format!("{ns_dir}/mnt"))?;
    move_into_link_name_space(
        ipc.as_fd(),
        Some(LinkNameSpaceType::InterProcessCommunication),
    )?;
    move_into_link_name_space(mnt.as_fd(), Some(LinkNameSpaceType::Mount))?;
    rustix::process::chdir("/")?;
    // PATH of the container image is meaningless on the host.
    let err = std::process::Command::new(&argv[0])
        .args(&argv[1..])
        .env("PATH", "/usr/sbin:/usr/bin:/sbin:/bin")
        .exec();
    Err(Box::new(err))
}

/// The prefix the agent puts in front of every command when
/// R_DRBD_HOST_NS is set.
fn host_exec_prefix() -> Option<Vec<String>> {
    let ns = std::env::var("R_DRBD_HOST_NS").ok()?;
    Some(agent::exec::host_exec_prefix("/proc/self/exe", &ns))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    if let Role::HostExec { ns, argv } = &cli.role {
        return host_exec(ns, argv);
    }
    if let Role::Crds = cli.role {
        let list = serde_json::json!({
            "apiVersion": "v1",
            "kind": "List",
            "items": model::crds(),
        });
        println!("{}", serde_json::to_string_pretty(&list)?);
        return Ok(());
    }
    // The fmt layer writes to stdout and colours unconditionally; pod logs keep escapes verbatim.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
                .from_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_ansi(std::io::IsTerminal::is_terminal(&std::io::stdout()))
        .with_target(false)
        .compact()
        .init();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    rt.block_on(async move {
        match cli.role {
            Role::Controller {
                csi_endpoint,
                namespace,
                identity,
            } => {
                let identity = match identity {
                    Some(id) => id,
                    None => rustix::system::uname()
                        .nodename()
                        .to_string_lossy()
                        .into_owned(),
                };
                controller::run(controller::Config::new(namespace, csi_endpoint, identity))
                    .await
                    .map_err(Into::into)
            }
            Role::Agent {
                node,
                csi_endpoint,
                namespace,
                res_dir,
                pod,
            } => agent::run(agent::Config {
                node,
                namespace,
                csi_endpoint,
                res_dir,
                host_exec_prefix: host_exec_prefix(),
                pod,
            })
            .await
            .map_err(Into::into),
            Role::Crds | Role::HostExec { .. } => unreachable!(),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_exec_prefix_parses_as_host_exec() {
        let mut argv = agent::exec::host_exec_prefix("/r-drbd", "/proc/1/ns");
        argv.extend(["lvs".to_owned(), "--noheadings".to_owned()]);
        match Cli::try_parse_from(argv).unwrap().role {
            Role::HostExec { ns, argv } => {
                assert_eq!(ns, "/proc/1/ns");
                assert_eq!(argv, ["lvs", "--noheadings"]);
            }
            r => panic!("host-exec expected: {r:?}"),
        }
    }
}
