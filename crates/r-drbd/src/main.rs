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

/// The agent image is a bare static binary: each external command re-execs
/// this binary, which moves into the host's IPC and mount namespaces and
/// execs the host's lvm, drbd-utils or mount. The agent process itself stays in the
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
    Some(vec![
        "/proc/self/exe".to_owned(),
        "host-exec".to_owned(),
        ns,
        "--".to_owned(),
    ])
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
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
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
