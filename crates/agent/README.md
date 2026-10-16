# agent

Node-side reconciler of r-drbd, one per node: thin LVM pools, local volumes (replicas 1), replicated volumes (replicas 2 and 3, DRBD), snapshots with the suspend and resume rounds, restore, status reporting, heartbeat Lease, Node annotations, the CSI node plugin.

Entry point: `agent::run(Config)`; `src/bin/agent-dev.rs` (feature `dev-bin`) is a temporary env-driven launcher until crates/r-drbd wires the agent role.

Modules:
- `exec`: tokio process runner behind `Exec` (stdin null, Timeout mode with SIGTERM then SIGKILL).
- `storage`: thin LVM through the lvm crate argv builders and parsers, PV filter per VG with the fresh-filter retry.
- `node`: tool probe, Node annotations, heartbeat Lease.
- `watch`: reflectors for Pool, Volume, Snapshot and every Node (peers' InternalIPs are DRBD addresses); a trigger that coalesces changes over 250 ms and fires every 30 s.
- `reconcile`: one pass (pools, volumes by name, snapshots), host work first, then server-side apply of this node's status entries only, skipped when unchanged; `reconcile::replicated` is the DRBD path, and `refresh` rewrites the entries of volumes events2 touched without a full pass.
- `events2`: line parser and tracked state, with the initial dump gate; cleared on every stream restart, so resources destroyed while the stream was down do not linger.
- `drbd`: command lines, generation ids, resource files and their backups, state derivation into the status contract, the `drbdsetup events2 all` process.
- `csi`: Identity and Node services on the Unix socket.

Rules:
- Every external command goes through `Exec`; unit tests script outputs with `ScriptedExec`. The one exception is the long running events2 stream, a tokio child with the same host-exec prefix.
- An LV is removed only when its Volume has a deletionTimestamp, its replicated Volume no longer assigns this node, or its Snapshot is Deleting; unknown LVs are logged once and left alone.
- No DRBD work before the initial events2 dump; a restart re-adopts from `lvs`, the resource files and events2.
- Only the initialUpToDateNode creates a fresh current generation id, and only while no replica (its own included) has reported data; otherwise it takes the day-0 id like the others, so a rebuilt replica never claims newer data than a peer.
- A restored diskful replica recreates its metadata until it first reports present: the snapshot carried the source volume's metadata.

Tests:
- `cargo test -p agent`: unit tests with scripted commands.
- `tests/host.rs`: real LVM, mounts and DRBD against the VG `r-drbd-test` (resources pvc-test-*, minors 1900 to 1999, ports 7900 to 7999, the replicated test uses a peer "ghost" that never answers); ignored unless `R_DRBD_HOST_TEST=1` and `--ignored`.
- `tests/csi_client.rs`: CSI client over the Unix socket for the end-to-end demo; ignored unless `CSI_SOCKET` is set.

Dependencies:
- csi, model, lvm, drbd-config: workspace crates.
- kube (client, runtime, rustls-tls, ring) and k8s-openapi: API access, reflectors, Lease and Node types.
- tokio, tokio-stream (net): runtime, processes, Unix socket listener for tonic.
- futures (std only): stream and future combinators for the watches.
- tracing: logs; tracing-subscriber only behind `dev-bin`.
- serde, serde_json: apply bodies, `drbdsetup status --json`.
- thiserror, async-trait: errors and the `Exec` trait.
- rustix (fs): statvfs for NodeGetVolumeStats.
- dev only: tonic (channel), hyper-util, tower for the test CSI client; tokio test-util for paused-time tests.
