# controller

The `r-drbd controller` role: Pool, Volume and Snapshot reconcilers, DRBD allocation, Lease leader election and the CSI controller plugin.
One leader, one reflector per kind, one reconcile loop that walks every object in name order and writes only what changed.

## Modules

- `lib`: `Config`, `Error`, `run`, `run_with_client`, `run_until`, `valid_name`, `valid_volume_name`; the reconcile loop and the finalizer and status-apply helpers.
- `leader`: acquire, renew and release the `r-drbd-controller` Lease; expiry judged from the contender's own observation.
- `store`: reflectors for Pool, Volume, Snapshot, core Node and the namespace's Leases (agent heartbeats), plus a change counter waiters block on; Lease renewals wake the loop only when a node's freshness changes.
- `wait`: bounded waits on store state for the CSI calls.
- `alloc`: pure allocation of node ids, minors, ports and the shared secret.
- `pool`: Pool finalizer and `status.volumes`.
- `volume`: placement (replicated restore from the snapshot holders), assignment write (no fileSystem seed on restores), diskless entries converged to `spec.attachments`, status aggregation (Ready, Degraded, QuorumLost, SplitBrain, NodeUnreachable), `spec.resolve`, deletion.
- `snapshot`: the snapshot round state machine and deletion.
- `csi`: CSI Identity and Controller services on the sidecars' Unix socket.

## Volume status: notes for consumers

- `status.replicas` entries belong to the agents; the controller never rewrites them.
- A halted node's entry stays as last written: it may still read UpToDate, inUse true, peers Connected.
- Cross-check `status.replicas` against the Degraded condition: reason NodeUnreachable names those nodes as "unknown (agent unreachable)"; treat their entries as unknown.
- Controller-derived fields already leave unreachable nodes out: `primary`, `usableBytes`, QuorumLost, SplitBrain and resolve completion.
- `spec.resolve` set while SplitBrain is False: Ready reason ResolveWithoutSplitBrain (Ready status unchanged); the agents ignore it, remove it.
- After a controller restart mid-resolve the Ready reason may show ResolveWithoutSplitBrain; the user then removes `spec.resolve`.
- A restored Volume has no `status.fileSystem` until its agent writes it (the adopt path gives the XFS a new UUID); `Snapshot.status.fileSystem` is informational.

## Dependencies

- `model`: the CRD types this controller reconciles.
- `placer`: placement and tiebreaker rules, kept pure in their own crate.
- `lvm`: LV names and the checks LVM makes on them, so a bad snapshot or volume name fails at admission.
- `csi`: generated CSI v1 service traits and messages, tonic and the socket bind.
- `kube` (client, runtime, rustls-tls, ring): API access, watchers and reflectors without openssl or aws-lc.
- `k8s-openapi`: core types (Node, Lease, PVC) and metav1 time.
- `tokio`: the async runtime; signal for SIGTERM, net for the Unix socket.
- `tokio-stream` (net): turns the Unix listener into the stream tonic serves.
- `prost-types`: the Timestamp in CSI snapshot responses.
- `futures`: stream combinators over the watchers.
- `serde`, `serde_json`: patch bodies built as JSON so server-side apply claims only controller fields.
- `thiserror`: error types with context.
- `tracing`: structured logs.
- `getrandom`: the DRBD shared secret, straight from the OS.
- `base64`: encoding of the shared secret.

Dev-dependencies: `tempfile` for envtest directories, `tracing-subscriber` for test logs.

## Tests

- Unit: allocation, status aggregation, DRBD conditions, resolve completion, attachment convergence, snapshot rounds, names, CSI parameters, Lease expiry.
- tests/support `node()` also creates the node's agent Lease and renews it every second; `agent_lease(.., paused)` stops the renewals. Lease tests shorten `Config::lease_stale`.
- envtest (tests/reconcile.rs, tests/replication.rs, tests/leader.rs, tests/csi.rs): a real etcd and kube-apiserver per test from `KUBEBUILDER_ASSETS`; skipped with a message when the binaries are missing.
  At most 4 apiservers run at once per test binary (a semaphore in tests/support); on a slower or busier machine lower it further with `cargo test -p controller -- --test-threads=2`.
- csi-sanity: `tools/harness/csi-sanity.sh`.
