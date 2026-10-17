//! The generated CRDs carry what the controller and agents rely on:
//! status subresources, list-map keys for server-side apply, the CEL
//! rules and the printer columns.

use kube::CustomResourceExt;
use model::{NodeDisks, Pool, Snapshot, Volume, VolumeSpec};
use serde_json::{Value, json};

fn schema(crd: &Value) -> &Value {
    &crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]
}

fn to_json<T: serde::Serialize>(t: &T) -> Value {
    serde_json::to_value(t).expect("serialize")
}

#[test]
fn four_cluster_scoped_crds_with_status() {
    let crds = model::crds();
    let names: Vec<_> = crds
        .iter()
        .map(|c| c.metadata.name.clone().unwrap())
        .collect();
    assert_eq!(
        names,
        [
            "pools.r-drbd.io",
            "volumes.r-drbd.io",
            "snapshots.r-drbd.io",
            "nodedisks.r-drbd.io"
        ]
    );
    for crd in &crds {
        assert_eq!(crd.spec.scope, "Cluster");
        let v = &crd.spec.versions[0];
        assert_eq!(v.name, "v1alpha1");
        assert!(
            v.subresources
                .as_ref()
                .and_then(|s| s.status.as_ref())
                .is_some()
        );
        assert!(
            v.additional_printer_columns
                .as_ref()
                .is_some_and(|c| c.len() >= 2)
        );
    }
}

#[test]
fn per_node_lists_are_maps_keyed_by_node() {
    let crd = to_json(&Volume::crd());
    let status = &schema(&crd)["properties"]["status"]["properties"];
    assert_eq!(status["replicas"]["x-kubernetes-list-type"], "map");
    assert_eq!(
        status["replicas"]["x-kubernetes-list-map-keys"],
        json!(["node"])
    );
    assert_eq!(
        status["conditions"]["x-kubernetes-list-map-keys"],
        json!(["type"])
    );
    let assignment = &schema(&crd)["properties"]["spec"]["properties"]["assignment"]["properties"];
    assert_eq!(
        assignment["replicas"]["x-kubernetes-list-map-keys"],
        json!(["node"])
    );
    let attachments = &schema(&crd)["properties"]["spec"]["properties"]["attachments"];
    assert_eq!(attachments["x-kubernetes-list-type"], "set");

    let snap = to_json(&Snapshot::crd());
    let acks = &schema(&snap)["properties"]["status"]["properties"]["acks"];
    assert_eq!(acks["x-kubernetes-list-map-keys"], json!(["node"]));
}

#[test]
fn cel_rules_present() {
    let crd = to_json(&Volume::crd());
    let s = serde_json::to_string(&crd).unwrap();
    for rule in [
        "self >= oldSelf",
        "!has(oldSelf.assignment) || has(self.assignment)",
        "minor is immutable",
    ] {
        assert!(s.contains(rule), "missing rule {rule}");
    }
    let pool = serde_json::to_string(&to_json(&Pool::crd())).unwrap();
    assert!(pool.contains("thinPool is immutable"));
}

#[test]
fn volume_defaults_and_camel_case() {
    let spec: VolumeSpec =
        serde_json::from_value(json!({"sizeBytes": 1_073_741_824, "tier": "nvme"})).unwrap();
    assert_eq!(spec.replicas, 1);
    assert_eq!(spec.attachments, Vec::<String>::new());
    assert_eq!(spec.access_mode, model::AccessMode::ReadWriteOnce);
    assert_eq!(spec.evict, Vec::<String>::new());
    assert!(spec.assignment.is_none());
    let back = to_json(&spec);
    assert_eq!(
        back,
        json!({"sizeBytes": 1_073_741_824, "replicas": 1, "tier": "nvme", "accessMode": "ReadWriteOnce", "attachments": [], "evict": []})
    );
}

#[test]
fn roles_and_phases_serialize_as_documented() {
    assert_eq!(to_json(&model::Role::Tiebreaker), json!("tiebreaker"));
    assert_eq!(
        to_json(&model::VolumePhase::Provisioning),
        json!("Provisioning")
    );
    assert_eq!(
        to_json(&model::SnapshotPhase::Suspending),
        json!("Suspending")
    );
    let crd = to_json(&Volume::crd());
    let role = &schema(&crd)["properties"]["spec"]["properties"]["assignment"]["properties"]["replicas"]
        ["items"]["properties"]["role"];
    assert_eq!(role["enum"], json!(["diskful", "diskless", "tiebreaker"]));
}

#[test]
fn condition_set_keeps_transition_time_when_status_unchanged() {
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
    use model::condition::{self, TRUE, types};
    let t0 = Time(k8s_openapi::jiff::Timestamp::from_second(1_000).unwrap());
    let t1 = Time(k8s_openapi::jiff::Timestamp::from_second(2_000).unwrap());
    let mut conds = Vec::new();
    condition::set(
        &mut conds,
        condition::new(
            types::READY,
            TRUE,
            "AllReplicasUpToDate",
            "",
            Some(1),
            t0.clone(),
        ),
    );
    condition::set(
        &mut conds,
        condition::new(
            types::READY,
            TRUE,
            "AllReplicasUpToDate",
            "",
            Some(2),
            t1.clone(),
        ),
    );
    assert_eq!(conds.len(), 1);
    assert_eq!(conds[0].last_transition_time, t0);
    assert_eq!(conds[0].observed_generation, Some(2));
    condition::set(
        &mut conds,
        condition::new(
            types::READY,
            condition::FALSE,
            "ReplicaMissing",
            "n2",
            Some(2),
            t1.clone(),
        ),
    );
    assert_eq!(conds[0].last_transition_time, t1);
    assert!(!condition::is_true(&conds, types::READY));
}

#[test]
fn pool_capacity_policy_defaults_ranges_and_mutability() {
    let spec: model::PoolSpec = serde_json::from_value(
        json!({"node": "n1", "tier": "fast", "vg": "vg", "thinPool": "thin"}),
    )
    .unwrap();
    assert_eq!(spec.overcommit_percent, 100);
    assert_eq!(spec.reserve_percent, 5);

    let crd = to_json(&Pool::crd());
    let props = &schema(&crd)["properties"]["spec"]["properties"];
    let over = &props["overcommitPercent"];
    assert_eq!(over["default"], 100);
    assert_eq!(over["minimum"].as_f64(), Some(100.0));
    assert_eq!(over["maximum"].as_f64(), Some(1000.0));
    let reserve = &props["reservePercent"];
    assert_eq!(reserve["default"], 5);
    assert_eq!(reserve["maximum"].as_f64(), Some(50.0));
    for field in [over, reserve] {
        assert!(field.get("x-kubernetes-validations").is_none());
    }
    assert_eq!(reserve["minimum"].as_f64(), Some(0.0));
    let required = schema(&crd)["properties"]["spec"]["required"]
        .as_array()
        .unwrap();
    for optional in ["overcommitPercent", "reservePercent"] {
        assert!(!required.iter().any(|r| r == optional), "{optional}");
    }
}

#[test]
fn pool_devices_mode_schema() {
    let spec: model::PoolSpec = serde_json::from_value(json!({
        "node": "n1", "tier": "fast", "vg": "vg", "thinPool": "thin",
        "devices": ["/dev/disk/by-id/nvme-a"], "chunkSize": "128Ki", "reclaimPolicy": "Delete",
    }))
    .unwrap();
    assert_eq!(spec.reclaim(), model::ReclaimPolicy::Delete);
    assert_eq!(
        spec.chunk_size.as_ref().map(|q| q.0.as_str()),
        Some("128Ki")
    );
    // Absent stays absent: the agent applies the defaults.
    let bare: model::PoolSpec = serde_json::from_value(
        json!({"node": "n1", "tier": "fast", "vg": "vg", "thinPool": "thin"}),
    )
    .unwrap();
    assert_eq!(bare.reclaim(), model::ReclaimPolicy::Retain);
    let back = to_json(&bare);
    for f in ["devices", "chunkSize", "reclaimPolicy"] {
        assert!(back.get(f).is_none(), "{f}");
    }

    let crd = to_json(&Pool::crd());
    let spec = &schema(&crd)["properties"]["spec"];
    let devices = &spec["properties"]["devices"];
    assert_eq!(devices["x-kubernetes-list-type"], "set");
    assert_eq!(devices["maxItems"], 64);
    assert_eq!(devices["items"]["pattern"], "^/dev/disk/by-id/[^/]+$");
    assert_eq!(
        spec["properties"]["reclaimPolicy"]["enum"],
        json!(["Retain", "Delete"])
    );
    assert!(spec["properties"]["chunkSize"].get("default").is_none());
    let rules = serde_json::to_string(&spec["x-kubernetes-validations"]).unwrap();
    for m in [
        "devices mode is fixed at creation",
        "devices is append only",
        "chunkSize needs devices",
        "reclaimPolicy needs devices",
        "chunkSize is immutable",
    ] {
        assert!(rules.contains(m), "missing rule {m}");
    }
    let rows = &schema(&crd)["properties"]["status"]["properties"]["devices"];
    assert_eq!(rows["x-kubernetes-list-map-keys"], json!(["path"]));
    assert_eq!(
        rows["items"]["properties"]["state"]["enum"],
        json!(["Missing", "NotADisk", "Member", "Busy", "NotEmpty", "Blank"])
    );
}

#[test]
fn nodedisks_rows_keyed_by_path_with_blank_column() {
    let crd = to_json(&NodeDisks::crd());
    let disks = &schema(&crd)["properties"]["status"]["properties"]["disks"];
    assert_eq!(disks["x-kubernetes-list-map-keys"], json!(["path"]));
    let required = &disks["items"]["required"];
    assert_eq!(required, &json!(["name", "path", "state"]));
    let cols = &crd["spec"]["versions"][0]["additionalPrinterColumns"];
    assert_eq!(cols[0]["jsonPath"], ".status.blank");
    assert_eq!(cols[1]["name"], "Age");

    let status = model::NodeDisksStatus {
        blank: 1,
        disks: vec![model::DiskStatus {
            path: "/dev/disk/by-id/wwn-0x5000".into(),
            name: "sdb".into(),
            links: vec!["/dev/disk/by-id/wwn-0x5000".into()],
            state: model::DeviceState::Member,
            pool: Some("p1".into()),
            size_bytes: Some(1 << 30),
            model: None,
            serial: None,
            message: None,
        }],
    };
    assert_eq!(
        to_json(&status),
        json!({"blank": 1, "disks": [{
            "path": "/dev/disk/by-id/wwn-0x5000", "name": "sdb",
            "links": ["/dev/disk/by-id/wwn-0x5000"], "state": "Member",
            "pool": "p1", "sizeBytes": 1_073_741_824u64,
        }]})
    );
}
