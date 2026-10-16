//! CSI v1 gRPC bindings, generated at build time from proto/csi.proto
//! (container-storage-interface/spec v1.11.0, Apache-2.0).
//!
//! The controller serves Identity and Controller; the agent serves Identity
//! and Node. Both bind a Unix socket given by the kubelet or the sidecars.

#![forbid(unsafe_code)]
#![allow(
    clippy::pedantic,
    clippy::all,
    unreachable_pub,
    missing_debug_implementations
)]

pub mod v1 {
    tonic::include_proto!("csi.v1");
}

pub use tonic;
