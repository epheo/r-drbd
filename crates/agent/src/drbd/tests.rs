use super::*;
use crate::events2::Tracker;

#[test]
fn command_lines() {
    let j = |v: Vec<String>| v.join(" ");
    assert_eq!(
        j(argv::create_md("r", 6)),
        "drbdadm create-md --force --max-peers=6 r/0"
    );
    assert_eq!(j(argv::adjust_dry_run("r")), "drbdadm --dry-run adjust r");
    assert_eq!(
        j(argv::validate("/d/.r.candidate", "/d/r.res", "r")),
        "drbdadm --config-to-test=/d/.r.candidate --config-to-exclude=/d/r.res dump r"
    );
    assert_eq!(
        j(argv::start_without_sync("r")),
        "drbdadm new-current-uuid --clear-bitmap r/0"
    );
    assert_eq!(
        j(argv::start_as_source("r")),
        "drbdadm new-current-uuid --force-resync r/0"
    );
    assert_eq!(j(argv::resize("r")), "drbdadm resize --assume-clean r/0");
    assert_eq!(j(argv::del_peer("r", 2)), "drbdsetup del-peer r 2");
    assert_eq!(j(argv::forget_peer("r", 2)), "drbdsetup forget-peer r 2");
    assert_eq!(
        j(argv::connect_discard("r")),
        "drbdadm connect --discard-my-data r"
    );
    assert_eq!(j(argv::connect_peer("r", "n2")), "drbdadm connect r:n2");
    assert_eq!(j(argv::status_json("r")), "drbdsetup status --json r");
}

#[test]
fn features_from_help_texts() {
    let f = features([
        "    [--bitmap={yes|no}] [--peer-tiebreaker={yes|no}]",
        "    [--rs-discard-granularity=(0 ... 1048576)] [--bitmap={yes|no}]",
        "    [--on-suspended-primary-outdated={disconnect|force-secondary}]",
    ]);
    assert!(f.tiebreaker && f.discard_granularity && f.suspended_primary_outdated);
    assert_eq!(features(["", "", ""]), Features::default());
}

#[test]
fn ownership_tags() {
    let t = md_tag("0b6a5e2c-1f7d-4a51-9c3e-7a2d0e6f1b42", 2);
    assert_eq!(t, "r-drbd.md.0b6a5e2c-1f7d-4a51-9c3e-7a2d0e6f1b42.2");
    assert!(is_md_tag(&t));
    assert!(!is_md_tag("r-drbd.new") && !is_md_tag("r-drbd.mdx"));
}

#[test]
fn restart_schedule() {
    let s: Vec<u64> = (0..8).map(|n| restart_delay(n).as_secs()).collect();
    assert_eq!(s, [1, 2, 4, 8, 16, 30, 30, 30]);
    assert_eq!(restart_delay(u32::MAX).as_secs(), 30);
}

fn tracked(lines: &[&str]) -> Tracker {
    let mut t = Tracker::default();
    for l in lines {
        t.feed(l).unwrap();
    }
    t.feed("exists -").unwrap();
    t
}

#[test]
fn derivation_into_status() {
    let t = tracked(&[
        "exists resource name:r role:Primary suspended:no",
        "exists connection name:r peer-node-id:1 conn-name:b connection:Connected role:Secondary",
        "exists connection name:r peer-node-id:2 conn-name:c connection:Connecting role:Unknown",
        "exists device name:r volume:0 minor:50000 disk:Inconsistent client:no quorum:yes",
        "exists peer-device name:r peer-node-id:1 conn-name:b volume:0 replication:SyncTarget peer-disk:UpToDate done:42.97",
        "exists peer-device name:r peer-node-id:2 conn-name:c volume:0 replication:Off peer-disk:DUnknown",
    ]);
    let facts = Facts {
        minor: 50000,
        diskful: true,
        allocated_bytes: Some(1_052_672 * 1024),
        observed_generation: Some(4),
        last_error: None,
    };
    let e = entry("a", t.resource("r"), &facts, Some(2_104_816), None);
    assert!(e.present && e.in_use);
    assert_eq!(e.disk_state.as_deref(), Some("Inconsistent"));
    assert_eq!(e.sync_percent, Some(42));
    assert_eq!(
        e.peers,
        BTreeMap::from([
            ("b".into(), "Connected".into()),
            ("c".into(), "Connecting".into())
        ])
    );
    assert_eq!(e.quorum, Some(true));
    assert_eq!(e.device_path.as_deref(), Some("/dev/drbd50000"));
    assert_eq!(e.size_bytes, Some(2_104_816 * 512));
    assert_eq!(e.allocated_bytes, Some(1_052_672 * 1024));
    assert_eq!(e.observed_generation, Some(4));

    let t = tracked(&[
        "exists resource name:r role:Secondary suspended:quorum",
        "exists device name:r volume:0 minor:50000 disk:Diskless client:yes quorum:no",
    ]);
    let facts = Facts {
        diskful: false,
        allocated_bytes: None,
        ..facts
    };
    let e = entry("c", t.resource("r"), &facts, Some(8), None);
    assert!(e.present && !e.in_use);
    assert_eq!(e.disk_state.as_deref(), Some("Diskless"));
    assert_eq!((e.sync_percent, e.quorum), (None, Some(false)));
    assert!(e.allocated_bytes.is_none() && e.peers.is_empty());

    let e = entry("c", None, &facts, Some(8), None);
    assert!(!e.present && e.device_path.is_none() && e.size_bytes.is_none());
}

#[test]
fn sync_target_without_done_uses_the_fallback() {
    let t = tracked(&[
        "exists resource name:r role:Secondary",
        "exists connection name:r peer-node-id:1 conn-name:b connection:Connected",
        "exists device name:r volume:0 minor:1 disk:Inconsistent",
        "exists peer-device name:r conn-name:b volume:0 replication:SyncTarget",
    ]);
    let d = derive(t.resource("r"));
    assert_eq!((d.sync_percent, d.sync_unknown), (None, true));
    let facts = Facts {
        minor: 1,
        diskful: true,
        allocated_bytes: Some(1),
        observed_generation: None,
        last_error: None,
    };
    let e = entry("a", t.resource("r"), &facts, Some(8), Some(37));
    assert_eq!(e.sync_percent, Some(37));
    let json = r#"[{"name":"r","connections":[{"name":"b","peer_devices":[
        {"volume":0,"replication-state":"SyncTarget","percent-in-sync":37.42},
        {"volume":0,"replication-state":"Established","percent-in-sync":100.00}]}]}]"#;
    assert_eq!(json_sync_percent(json), Some(37));
    assert_eq!(json_sync_percent("not json"), None);
}

fn replica(node: &str, id: u8, role: Role) -> AssignedReplica {
    AssignedReplica {
        node: node.into(),
        pool: (role == Role::Diskful).then(|| "p".to_owned()),
        node_id: id,
        port: Some(11000 + u16::from(id)),
        role,
    }
}

fn assignment() -> Assignment {
    Assignment {
        replicas: vec![
            replica("node-c", 2, Role::Tiebreaker),
            replica("node-b", 1, Role::Diskful),
            replica("node-a", 0, Role::Diskful),
            replica("node-d", 3, Role::Diskless),
        ],
        minor: Some(50000),
        secret: Some("s3cr3t".into()),
        initial_up_to_date_node: Some("node-a".into()),
    }
}

fn addresses() -> BTreeMap<String, IpAddr> {
    [
        ("node-a", "10.0.0.11"),
        ("node-b", "10.0.0.12"),
        ("node-c", "10.0.0.13"),
        ("node-d", "fd00::4"),
    ]
    .into_iter()
    .map(|(n, ip)| (n.to_owned(), ip.parse().unwrap()))
    .collect()
}

const ALL: Features = Features {
    tiebreaker: true,
    discard_granularity: true,
    suspended_primary_outdated: true,
};

#[test]
fn resource_as_each_node_sees_it() {
    let (a, addr) = (assignment(), addresses());
    let tb = FileInputs {
        node: "node-c",
        volume: "pvc-1",
        assignment: &a,
        backing: None,
        addresses: &addr,
        chunk_bytes: Some(65536),
        access_mode: model::AccessMode::ReadWriteOnce,
    };
    let r = resource(&tb).unwrap();
    // The tiebreaker connects to the diskful nodes only.
    let peers: Vec<(&str, Disk<'_>)> = r.peers.iter().map(|p| (p.name, p.disk)).collect();
    assert_eq!(peers, [("node-b", Disk::Peer), ("node-a", Disk::Peer)]);
    assert_eq!(r.local.disk, Disk::None);
    assert!(r.local.tiebreaker);
    let text = drbd_config::render(&r, ALL).unwrap().text;
    assert!(text.contains("host \"node-c\" address ipv4 10.0.0.13:11002;"));
    assert!(!text.contains("fd00::4"));

    let diskful = FileInputs {
        node: "node-a",
        backing: Some("/dev/vg/pvc-1"),
        ..tb.clone()
    };
    let r = resource(&diskful).unwrap();
    assert_eq!(r.local.disk, Disk::Local("/dev/vg/pvc-1"));
    assert_eq!(r.peers.len(), 3);
    let client = r.peers.iter().find(|p| p.name == "node-d").unwrap();
    assert!(!client.tiebreaker && client.disk == Disk::None);
    assert!(!r.two_primaries);
    let text = drbd_config::render(&r, ALL).unwrap().text;
    assert!(text.contains("host \"node-d\" address ipv6 [fd00::4]:11003;"));
    assert!(text.contains("tiebreaker no;"));

    let rwx = FileInputs {
        access_mode: model::AccessMode::ReadWriteMany,
        ..diskful.clone()
    };
    assert!(resource(&rwx).unwrap().two_primaries);

    let no_lv = FileInputs {
        backing: None,
        ..diskful.clone()
    };
    assert_eq!(
        resource(&no_lv).unwrap_err(),
        "diskful replica without a backing LV"
    );
    let mut missing = addr.clone();
    missing.remove("node-b");
    let gap = FileInputs {
        addresses: &missing,
        ..diskful
    };
    assert_eq!(resource(&gap).unwrap_err(), "node node-b has no InternalIP");
}

#[test]
fn files_candidate_install_remove() {
    let dir = std::env::temp_dir().join(format!("agent-resfiles-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let f = ResFiles::new(&dir);
    let (a, addr) = (assignment(), addresses());
    let inputs = FileInputs {
        node: "node-c",
        volume: "pvc-1",
        assignment: &a,
        backing: None,
        addresses: &addr,
        chunk_bytes: None,
        access_mode: model::AccessMode::ReadWriteOnce,
    };
    let r = drbd_config::render(&resource(&inputs).unwrap(), ALL).unwrap();
    assert!(!f.installed("pvc-1", &r.hash).unwrap());
    let c = f.write_candidate("pvc-1", &r.text).unwrap();
    assert_eq!(c, dir.join(".pvc-1.candidate"));
    assert!(!f.exists("pvc-1"));
    f.install("pvc-1").unwrap();
    assert!(f.installed("pvc-1", &r.hash).unwrap() && !c.exists());
    // A hand edit is a change.
    let edited = r.text.replace("11002", "11009");
    std::fs::write(f.path("pvc-1"), edited).unwrap();
    assert!(!f.installed("pvc-1", &r.hash).unwrap());
    f.write_candidate("pvc-1", "x").unwrap();
    f.remove("pvc-1").unwrap();
    f.remove("pvc-1").unwrap();
    assert!(!f.exists("pvc-1") && !c.exists());
    std::fs::remove_dir_all(&dir).unwrap();
}

/// A fake events2 through the prefix: `sh -c <script> -- drbdsetup events2 all`.
#[tokio::test]
async fn events2_process_dump_then_changes_then_restart() {
    let script = "echo 'exists resource name:r role:Secondary'; \
        echo 'exists -'; sleep 0.3; \
        echo 'change resource name:r role:Primary'; sleep 0.3";
    let prefix = vec![
        "sh".to_owned(),
        "-c".to_owned(),
        script.to_owned(),
        "--".to_owned(),
    ];
    let tracker = Arc::new(Mutex::new(Tracker::default()));
    let notify = Notify {
        changes: Arc::new(Trigger::default()),
        pass: Arc::new(Trigger::default()),
    };
    let task = tokio::spawn(events2(Some(prefix), tracker.clone(), notify.clone()));
    let wait = |want: fn(&Tracker) -> bool| {
        let tracker = tracker.clone();
        async move {
            for _ in 0..100 {
                if want(&tracker.lock().unwrap()) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            panic!("events2 state not reached");
        }
    };
    wait(|t| t.is_available() && t.resource("r").is_some()).await;
    tokio::time::timeout(Duration::from_secs(1), notify.pass.changed())
        .await
        .unwrap();
    wait(|t| t.resource("r").is_some_and(Resource::is_primary)).await;
    tokio::time::timeout(Duration::from_secs(1), notify.changes.changed())
        .await
        .unwrap();
    // The process exits: state is cleared until the next dump.
    wait(|t| !t.is_available() && t.resources().is_empty()).await;
    task.abort();
}

#[test]
fn slot_holders_from_the_superblock() {
    let out = r#"*** confirmation forced via --force option ***
{
  "current_uuid": "0x0000000000000004",
  "peers": [
    { "bitmap_uuid": "0x0", "flags": "0x0", "bitmap_index": -1 },
    { "bitmap_uuid": "0x0", "flags": "0x0", "bitmap_index": 1 },
    { "bitmap_uuid": "0x0", "flags": "0x0", "bitmap_index": -1 },
    { "bitmap_uuid": "0x0", "flags": "0x0", "bitmap_index": 0 }
  ],
  "bm_max_peers": 6,
  "node_id": 0
}"#;
    assert_eq!(slot_holders(out).unwrap(), [1, 3]);
    assert!(slot_holders("no json").is_err());
    assert_eq!(
        argv::dump_superblock(50000, "/dev/vg/pvc-1").join(" "),
        "drbdmeta --force 50000 v09 /dev/vg/pvc-1 internal dump-superblock --output-format=json"
    );
}
