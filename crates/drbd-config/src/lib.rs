//! DRBD resource files with the product's fixed option set, and internal
//! metadata sizing. Pure: no IO, no commands.

#![forbid(unsafe_code)]

mod render;
mod size;

pub use render::{
    Disk, Features, HEADER_PREFIX, Host, PEER_DISK, RenderError, Rendered, Resource, VOLUME,
    header_hash, render,
};
pub use size::{gross_bytes, metadata_bytes, usable_bytes};
