//! CSI v1 gRPC bindings, generated at build time from proto/csi.proto
//! (container-storage-interface/spec v1.11.0, Apache-2.0).
//!
//! The controller serves Identity and Controller; the agent serves Identity
//! and Node. Both bind a Unix socket given by the kubelet or the sidecars.

#![forbid(unsafe_code)]

use std::os::unix::fs::FileTypeExt;
use std::os::unix::net::UnixListener;
use std::path::Path;

#[allow(
    clippy::pedantic,
    clippy::all,
    unreachable_pub,
    missing_debug_implementations
)]
pub mod v1 {
    tonic::include_proto!("csi.v1");
}

pub use tonic;

/// `unix:///csi/csi.sock`, `unix:/csi/csi.sock` or a bare path.
pub fn socket_path(endpoint: &str) -> &Path {
    Path::new(
        endpoint
            .strip_prefix("unix://")
            .or_else(|| endpoint.strip_prefix("unix:"))
            .unwrap_or(endpoint),
    )
}

/// Binds the endpoint's socket, creating its directory.
/// A leftover socket from a previous run is replaced; any other file at the
/// path is a configuration error and is never deleted.
/// The listener is non-blocking, as `tokio::net::UnixListener::from_std` needs.
pub fn bind_unix(endpoint: &str) -> std::io::Result<UnixListener> {
    let path = socket_path(endpoint);
    let ctx = |what: &str, e: std::io::Error| {
        std::io::Error::new(e.kind(), format!("{what} {}: {e}", path.display()))
    };
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).map_err(|e| ctx("create", e))?;
    }
    match std::fs::symlink_metadata(path) {
        Ok(m) if m.file_type().is_socket() => {
            std::fs::remove_file(path).map_err(|e| ctx("remove stale", e))?;
        }
        Ok(_) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("CSI endpoint {} exists and is not a socket", path.display()),
            ));
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(ctx("stat", e)),
    }
    let listener = UnixListener::bind(path).map_err(|e| ctx("bind", e))?;
    listener.set_nonblocking(true)?;
    Ok(listener)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoints() {
        assert_eq!(
            socket_path("unix:///csi/csi.sock"),
            Path::new("/csi/csi.sock")
        );
        assert_eq!(socket_path("unix:/tmp/x.sock"), Path::new("/tmp/x.sock"));
        assert_eq!(socket_path("/tmp/x.sock"), Path::new("/tmp/x.sock"));
    }

    #[test]
    fn bind_replaces_only_sockets() {
        let dir = std::env::temp_dir().join(format!("csi-bind-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let file = dir.join("not-a-socket");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(&file, b"keep").unwrap();
        assert!(bind_unix(file.to_str().unwrap()).is_err());
        assert_eq!(std::fs::read(&file).unwrap(), b"keep");

        let sock = format!("unix://{}", dir.join("sub/csi.sock").display());
        drop(bind_unix(&sock).unwrap());
        assert!(
            socket_path(&sock).exists(),
            "a dropped listener leaves the file behind"
        );
        drop(bind_unix(&sock).unwrap());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
