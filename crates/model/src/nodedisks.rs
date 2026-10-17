//! NodeDisks: the whole disks of one node, as its agent sees them.

use kube::{CustomResource, KubeSchema};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::pool::DeviceState;

// Named after its node and owned by the Node object. Nothing in it claims
// a disk: a Pool lists the disks it uses.
#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, Default, PartialEq, JsonSchema)]
#[kube(
    group = "r-drbd.io",
    version = "v1alpha1",
    kind = "NodeDisks",
    plural = "nodedisks",
    shortname = "nd",
    status = "NodeDisksStatus",
    derive = "PartialEq",
    printcolumn = r#"{"name":"Blank","type":"integer","jsonPath":".status.blank"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
/// Empty: the agent of the node writes the status only.
pub struct NodeDisksSpec {}

#[derive(KubeSchema, Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NodeDisksStatus {
    /// Disks in state Blank; a printer column cannot count.
    pub blank: u32,
    /// Set when the list is incomplete: rows capped, or the scan ran out
    /// of time before every disk was checked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// How the agent learns of disk changes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub watch: Option<DiskWatch>,
    #[serde(default)]
    #[x_kube(merge_strategy = ListMerge::Map(vec!["path".into()]))]
    pub disks: Vec<DiskStatus>,
}

#[derive(JsonSchema, Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DiskWatch {
    /// Kernel uevents and mount table changes: rows follow within seconds,
    /// with a full check every hour.
    Uevent,
    /// No uevent socket: a full check every 5 minutes.
    Poll,
}

/// One whole disk of the node.
#[derive(JsonSchema, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DiskStatus {
    /// The path a Pool lists: the `wwn-` by-id link, else the bus one;
    /// `/dev/<name>` when the disk has no by-id link.
    pub path: String,
    /// Kernel name; it may change at the next boot.
    pub name: String,
    /// Every by-id link of the disk.
    #[serde(default)]
    pub links: Vec<String>,
    pub state: DeviceState,
    /// The Pool whose VG holds the disk, for `Member`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub serial: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}
