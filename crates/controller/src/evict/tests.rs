use super::*;
use model::Resolve;

const G: u64 = 1 << 30;

fn rep(node: &str, id: u8, port: u16, role: Role) -> AssignedReplica {
    AssignedReplica {
        node: node.into(),
        pool: (role == Role::Diskful).then(|| format!("p-{node}")),
        node_id: id,
        port: Some(port),
        role,
    }
}

/// n1 and n2 diskful, n3 tiebreaker.
fn two() -> Assignment {
    Assignment {
        replicas: vec![
            rep("n1", 0, 7000, Role::Diskful),
            rep("n2", 1, 7000, Role::Diskful),
            rep("n3", 2, 7000, Role::Tiebreaker),
        ],
        minor: Some(1000),
        secret: Some("s".into()),
        gi: Some("AA".into()),
        initial_up_to_date_node: Some("n1".into()),
    }
}

fn view(node: &str, free: u64) -> placer::PoolView {
    placer::PoolView {
        name: format!("p-{node}"),
        node: node.into(),
        tier: "fast".into(),
        node_ready: true,
        available: true,
        total_bytes: 100 * G,
        free_bytes: free,
        reserved_bytes: 0,
        volume_count: 0,
    }
}

fn spec(evict: &[&str]) -> VolumeSpec {
    VolumeSpec {
        size_bytes: G,
        replicas: 2,
        tier: "fast".into(),
        pool: None,
        node: None,
        access_mode: model::AccessMode::ReadWriteOnce,
        attachments: Vec::new(),
        evict: evict.iter().map(|s| (*s).to_owned()).collect(),
        source: None,
        resolve: None,
        assignment: None,
    }
}

fn up(node: &str, in_use: bool) -> ReplicaStatus {
    ReplicaStatus {
        node: node.into(),
        present: true,
        disk_state: Some("UpToDate".into()),
        in_use,
        quorum: Some(true),
        ..ReplicaStatus::default()
    }
}

fn tb(node: &str) -> ReplicaStatus {
    ReplicaStatus {
        node: node.into(),
        present: true,
        disk_state: Some("Diskless".into()),
        ..ReplicaStatus::default()
    }
}

struct Case {
    spec: VolumeSpec,
    a: Assignment,
    entries: Vec<ReplicaStatus>,
    initialized: bool,
    split_brain: bool,
    snapshot: Option<&'static str>,
    stale: Vec<&'static str>,
    /// Nodes whose Node object is gone.
    gone: Vec<&'static str>,
    views: Vec<placer::PoolView>,
    ready: Vec<String>,
    used: Used,
}

impl Case {
    fn new(evict: &[&str]) -> Self {
        let a = two();
        let used = Used::from_assignments([&a]);
        Self {
            spec: spec(evict),
            entries: vec![up("n1", true), up("n2", false), tb("n3")],
            a,
            initialized: true,
            split_brain: false,
            snapshot: None,
            stale: Vec::new(),
            gone: Vec::new(),
            views: vec![view("n1", 50 * G), view("n2", 50 * G), view("n4", 50 * G)],
            ready: ["n1", "n2", "n3", "n4", "n5"]
                .iter()
                .map(|s| (*s).to_owned())
                .collect(),
            used,
        }
    }

    fn decide(&self) -> Step {
        let stale = self.stale.clone();
        let fresh = move |n: &str| !stale.contains(&n);
        let gone = self.gone.clone();
        let exists = move |n: &str| !gone.contains(&n);
        decide(&Input {
            name: "vol",
            spec: &self.spec,
            assignment: &self.a,
            entries: &self.entries,
            initialized: self.initialized,
            split_brain: self.split_brain,
            snapshot_in_progress: self.snapshot,
            fresh: &fresh,
            node_exists: &exists,
            views: &self.views,
            ready_nodes: &self.ready,
            used: &self.used,
        })
    }
}

fn message(s: &Step) -> &str {
    match s {
        Step::Blocked { message, .. } => message,
        other => panic!("not blocked: {other:?}"),
    }
}

fn moved(s: Step) -> (Assignment, Option<(String, String)>, String) {
    match s {
        Step::Move {
            next,
            diskful,
            event,
            ..
        } => (next, diskful, event),
        other => panic!("not a move: {other:?}"),
    }
}

#[test]
fn idle_clear_and_wait() {
    assert_eq!(Case::new(&[]).decide(), Step::Idle);
    // Not in the assignment, not attached: leaves the list; only the
    // smallest node is looked at.
    assert_eq!(
        Case::new(&["n9", "n7"]).decide(),
        Step::Clear {
            node: "n7".into(),
            rest: vec!["n9".into()]
        }
    );
    // Attached without an entry, or diskless: converge and unpublish first.
    let mut c = Case::new(&["n7"]);
    c.spec.attachments = vec!["n7".into()];
    assert_eq!(c.decide(), Step::Wait { node: "n7".into() });
    let mut c = Case::new(&["n7"]);
    c.a.replicas.push(rep("n7", 3, 7000, Role::Diskless));
    assert_eq!(c.decide(), Step::Wait { node: "n7".into() });
}

#[test]
fn blocked_reasons() {
    let base = || Case::new(&["n2"]);
    let mut c = base();
    c.initialized = false;
    // The text names the way out.
    assert_eq!(
        message(&c.decide()),
        "eviction of n2 blocked: status.initializedAt is not set: the volume was never Ready, or its status was lost; an operator may set it once an UpToDate replica holds the data"
    );
    let mut c = base();
    c.snapshot = Some("snap-1");
    assert_eq!(
        message(&c.decide()),
        "eviction of n2 blocked: snapshot snap-1 in progress"
    );
    let mut c = base();
    c.spec.resolve = Some(Resolve {
        survivor: "n1".into(),
    });
    assert!(message(&c.decide()).contains("spec.resolve"));
    let mut c = base();
    c.split_brain = true;
    assert!(message(&c.decide()).contains("split brain"));
    // The other diskful replica is stale, or not UpToDate.
    let mut c = base();
    c.stale = vec!["n1"];
    let quorum = "eviction of n2 blocked: no UpToDate replica with quorum outside n2; if n2 is dead for good, delete its Node object";
    assert_eq!(message(&c.decide()), quorum);
    let mut c = base();
    c.entries[0].disk_state = Some("Inconsistent".into());
    assert_eq!(message(&c.decide()), quorum);
    // UpToDate without quorum is no survivor.
    for q in [Some(false), None] {
        let mut c = base();
        c.entries[0].quorum = q;
        assert_eq!(message(&c.decide()), quorum, "{q:?}");
        // N's Node deleted: the operator's proof of death waives quorum,
        // never UpToDate.
        c.gone = vec!["n2"];
        assert!(matches!(c.decide(), Step::Move { .. }), "{q:?}");
        c.entries[0].disk_state = Some("Outdated".into());
        assert_eq!(message(&c.decide()), quorum, "{q:?}");
    }
    // replicas 3: one peer UpToDate, the other still syncing.
    let mut c = base();
    c.a.replicas.push(rep("n5", 3, 7000, Role::Diskful));
    c.entries.push(ReplicaStatus {
        disk_state: Some("Inconsistent".into()),
        ..up("n5", false)
    });
    let m = message(&c.decide()).to_owned();
    assert!(m.contains("not UpToDate on n5"), "{m}");
    assert!(m.starts_with("eviction of n2 blocked: "), "{m}");
    // A stale lagging peer does not block.
    c.stale = vec!["n5"];
    assert!(matches!(c.decide(), Step::Move { .. }));
    // In use on a reachable node; a stale one may still read in use.
    let mut c = Case::new(&["n1"]);
    assert_eq!(message(&c.decide()), "eviction of n1 blocked: in use on n1");
    c.stale = vec!["n1"];
    assert!(matches!(c.decide(), Step::Move { .. }));
    // Attached on a fresh node, even when not in use; a stale one may go.
    let mut c = base();
    c.spec.attachments = vec!["n2".into()];
    assert_eq!(
        message(&c.decide()),
        "eviction of n2 blocked: attached to n2; unpublish it first"
    );
    c.stale = vec!["n2"];
    assert!(matches!(c.decide(), Step::Move { .. }));
    // Pins.
    let mut c = base();
    c.spec.node = Some("n2".into());
    assert!(message(&c.decide()).contains("pinned to n2"));
    let mut c = base();
    c.spec.pool = Some("p-n2".into());
    assert!(message(&c.decide()).contains("pinned to n2"));
    // A node pin elsewhere is the other replica's: no effect.
    let mut c = base();
    c.spec.node = Some("n1".into());
    assert!(matches!(c.decide(), Step::Move { .. }));
    // Nowhere to go.
    let mut c = base();
    c.views.retain(|v| v.node != "n4");
    c.ready.retain(|n| n != "n3");
    assert_eq!(
        message(&c.decide()),
        "eviction of n2 blocked: no pool for tier fast outside n2"
    );
}

#[test]
fn candidate_filter() {
    let a = two();
    let views = vec![
        view("n1", G),
        view("n2", G),
        view("n3", G),
        view("n4", G),
        view("n5", G),
        view("n6", G),
    ];
    let fresh = |n: &str| n != "n5";
    // n7 left the assignment but its agent has not dropped the entry yet.
    let mut views = views;
    views.push(view("n7", G));
    let entries = vec![up("n1", false), up("n2", false), tb("n3"), up("n7", false)];
    let got: Vec<String> = candidates(&views, &a, &entries, &["n2".into(), "n6".into()], &fresh)
        .into_iter()
        .map(|p| p.node)
        .collect();
    // n3 holds only the tiebreaker: allowed, its entry is in the assignment.
    assert_eq!(got, ["n3", "n4"]);
}

#[test]
fn departing_nodes_are_no_target() {
    // n4 still reports an entry from an earlier replica: no diskful there,
    // and no tiebreaker either; the text names it.
    let mut c = Case::new(&["n2"]);
    c.entries.push(up("n4", false));
    assert_eq!(
        message(&c.decide()),
        "eviction of n2 blocked: no pool for tier fast outside n2 (departing: n4)"
    );
    let mut c = Case::new(&["n3"]);
    c.entries.push(tb("n5"));
    let (_, _, event) = moved(c.decide());
    assert_eq!(event, "tiebreaker of vol moved from n3 to n4");
}

#[test]
fn node_ids_never_reuse_the_evicted_one() {
    let a = two();
    assert_eq!(free_node_id(&a, 0), Some(3));
    let mut gap = two();
    gap.replicas.retain(|r| r.node != "n1");
    // Id 0 is free in the assignment but was n1's.
    assert_eq!(free_node_id(&gap, 0), Some(3));
    assert_eq!(free_node_id(&gap, 5), Some(0));
    let full = Assignment {
        replicas: (0..=NODE_ID_MAX)
            .map(|id| rep(&format!("x{id}"), id, 7000, Role::Diskless))
            .collect(),
        ..two()
    };
    assert_eq!(free_node_id(&full, 0), None);
}

#[test]
fn diskful_to_a_new_node() {
    let mut c = Case::new(&["n2"]);
    // n4's port 7000 is taken by another volume.
    c.used.add(&Assignment {
        replicas: vec![rep("n4", 0, 7000, Role::Diskful)],
        ..two()
    });
    let (next, diskful, event) = moved(c.decide());
    assert_eq!(diskful, Some(("n4".into(), "p-n4".into())));
    assert_eq!(event, "replica of vol moved from n2 to n4");
    let got: Vec<_> = next
        .replicas
        .iter()
        .map(|r| {
            (
                r.node.as_str(),
                r.node_id,
                r.port,
                r.role,
                r.pool.as_deref(),
            )
        })
        .collect();
    assert_eq!(
        got,
        [
            ("n1", 0, Some(7000), Role::Diskful, Some("p-n1")),
            ("n3", 2, Some(7000), Role::Tiebreaker, None),
            ("n4", 3, Some(7001), Role::Diskful, Some("p-n4")),
        ]
    );
    // The DRBD parameters stay.
    assert_eq!(
        (next.minor, &next.secret, &next.gi),
        (c.a.minor, &c.a.secret, &c.a.gi)
    );
}

#[test]
fn tiebreaker_takes_over() {
    // Only the tiebreaker's node has a free pool.
    let mut c = Case::new(&["n2"]);
    c.views = vec![view("n1", G), view("n2", G), view("n3", G)];
    c.ready = vec!["n1".into(), "n2".into(), "n3".into()];
    let (next, diskful, event) = moved(c.decide());
    assert_eq!(diskful, Some(("n3".into(), "p-n3".into())));
    assert_eq!(event, "replica of vol moved from n2 to n3");
    let got: Vec<_> = next
        .replicas
        .iter()
        .map(|r| (r.node.as_str(), r.node_id, r.port, r.role))
        .collect();
    // n3 keeps its id and port; no node is left for a tiebreaker.
    assert_eq!(
        got,
        [
            ("n1", 0, Some(7000), Role::Diskful),
            ("n3", 2, Some(7000), Role::Diskful),
        ]
    );
    // With n5 ready and empty, it becomes the new tiebreaker; the
    // evicted n2 never does.
    c.ready.push("n5".into());
    let (next, _, event) = moved(c.decide());
    assert_eq!(
        event,
        "replica of vol moved from n2 to n3; tiebreaker on n5"
    );
    let t = next.replicas.last().unwrap();
    assert_eq!(
        (t.node.as_str(), t.node_id, t.port, t.role),
        ("n5", 3, Some(7000), Role::Tiebreaker)
    );
    // A stale n5 is not taken.
    c.stale = vec!["n5"];
    let (next, _, _) = moved(c.decide());
    assert!(!next.replicas.iter().any(|r| r.role == Role::Tiebreaker));
}

#[test]
fn tiebreaker_moves_or_goes() {
    let mut c = Case::new(&["n3"]);
    let (next, diskful, event) = moved(c.decide());
    assert_eq!(diskful, None);
    // n5 holds no pool of the tier: preferred over n4.
    assert_eq!(event, "tiebreaker of vol moved from n3 to n5");
    let t = next.replicas.last().unwrap();
    assert_eq!((t.node.as_str(), t.node_id), ("n5", 3));
    c.ready.retain(|n| n != "n4" && n != "n5");
    let (next, _, event) = moved(c.decide());
    assert_eq!(
        event,
        "tiebreaker of vol removed from n3; no node for a new one"
    );
    assert_eq!(next.replicas.len(), 2);
    // No data on it: neither initializedAt nor a survivor is needed.
    c.initialized = false;
    c.entries[0].disk_state = Some("Inconsistent".into());
    c.entries[1].quorum = Some(false);
    assert!(matches!(c.decide(), Step::Move { .. }));
    // Attached or in use on a fresh node blocks the tiebreaker too; a stale
    // one may go.
    c.spec.attachments = vec!["n3".into()];
    assert_eq!(
        message(&c.decide()),
        "eviction of n3 blocked: attached to n3; unpublish it first"
    );
    c.stale = vec!["n3"];
    assert!(matches!(c.decide(), Step::Move { .. }));
    c.stale.clear();
    c.spec.attachments.clear();
    c.entries[2].in_use = true;
    assert_eq!(message(&c.decide()), "eviction of n3 blocked: in use on n3");
    c.entries[2].in_use = false;
    // Resolve, split brain and a snapshot round still block.
    c.split_brain = true;
    assert!(message(&c.decide()).contains("split brain"));
    c.split_brain = false;
    c.snapshot = Some("s");
    assert!(message(&c.decide()).contains("snapshot s in progress"));
    c.snapshot = None;
    c.spec.resolve = Some(Resolve {
        survivor: "n1".into(),
    });
    assert!(message(&c.decide()).contains("spec.resolve"));
}

#[test]
fn orphans() {
    let mut a = two();
    a.replicas.retain(|r| r.node != "n2");
    let entries = vec![up("n1", false), up("n2", true), tb("n3"), up("n4", false)];
    let fresh = |n: &str| n != "n2";
    let exists = |n: &str| n != "n4";
    let o = orphan_entries(&a, &entries, &fresh, &exists);
    assert_eq!(o, [(1, "n2".to_owned()), (3, "n4".to_owned())]);
    // A fresh agent outside the assignment drops its own entry.
    assert_eq!(orphan_entries(&a, &entries[..3], &|_| true, &|_| true), []);
    assert_eq!(
        orphan_patch(&o),
        serde_json::json!([
            { "op": "test", "path": "/status/replicas/3/node", "value": "n4" },
            { "op": "remove", "path": "/status/replicas/3" },
            { "op": "test", "path": "/status/replicas/1/node", "value": "n2" },
            { "op": "remove", "path": "/status/replicas/1" },
        ])
    );
}
