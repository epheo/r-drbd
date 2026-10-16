# agent

Node-side reconciler of r-drbd, one per node: thin LVM pools, local volumes (replicas 1), replicated volumes (replicas 2 and 3, DRBD), snapshots with the suspend and resume rounds, restore, status reporting, heartbeat Lease, Node annotations, the CSI node plugin.

Entry point: `agent::run(Config)`; `src/bin/agent-dev.rs` (feature `dev-bin`) is a temporary env-driven launcher until crates/r-drbd wires the agent role.

Modules:
- `exec`: tokio process runner behind `Exec` (stdin null, Timeout mode with SIGTERM then SIGKILL).
- `storage`: thin LVM through the lvm crate argv builders and parsers; separate timeouts for reports and changes; remove waits a bounded time for an open LV.
- `node`: tool probe (versions, and the optional DRBD settings `drbdsetup help` lists), Node annotations, heartbeat Lease.
- `watch`: reflectors for Pool, Volume, Snapshot and every Node (peers' InternalIPs are DRBD addresses); a trigger that coalesces changes over 250 ms and fires every 30 s.
- `reconcile`: one pass (pools, volumes by name, snapshots), host work first, then server-side apply of this node's status entries only, skipped when unchanged; `reconcile::replicated` is the DRBD path, and `refresh` rewrites the entries of volumes events2 touched without a full pass.
- `events2`: line parser and tracked state, built from the initial dump only (a change before its end restarts the stream); cleared on every restart, so resources destroyed while the stream was down do not linger.
- `drbd`: command lines, ownership tags, resource files (checked by drbdadm, then renamed into place), state derivation into the status contract, the `drbdsetup events2 all` process with its restart backoff.
- `csi`: Identity and Node services on the Unix socket.

Rules:
- Every external command goes through `Exec`; unit tests script outputs with `ScriptedExec`. The one exception is the long running events2 stream, a tokio child with the same host-exec prefix.
- An LV is removed only when its Volume has a deletionTimestamp, its replicated Volume no longer assigns this node, or its Snapshot is Deleting; unknown LVs are logged once and left alone.
- No DRBD work before the initial events2 dump; a restart re-adopts from `lvs`, the resource files and events2.
- Adjust runs only when `drbdadm --dry-run adjust` plans commands, and never during a split brain (the helper call, carried over an events2 restart while still StandAlone, or the Volume's SplitBrain condition), a snapshot round's suspend or a resolve in flight. A plan that comes back after a successful adjust is retried on a doubling wait (30 s to 10 min) and reported in lastError.
- Every new diskful replica gets just-created metadata. Before initializedAt the initialUpToDateNode makes the first data current: without a sync once every diskful peer is connected and Inconsistent, or after a bounded wait when DRBD reports quorum, no replica was rebuilt and no status ever showed data; as the sync source for a restore, whose LV it restores again if lost before initializedAt.
- Bitmap slots of node ids no longer assigned are freed with forget-peer, read from the superblock once per spec generation and retried until clean.
- Metadata ownership is the LVM tag `r-drbd.md.<volume uid>.<node id>`; `r-drbd.new` marks an LV that never held data; anything else is stale or refused, never adopted.

Tests:
- `cargo test -p agent`: unit tests with scripted commands.
- `tests/host.rs`: real LVM and mounts against a test VG set at build time by `R_DRBD_TEST_VG`, `R_DRBD_TEST_THIN` and `R_DRBD_TEST_DIR` (LVM tests use LVs rw-l-*; the guest signature test brings up rw-l-d on minor 99989); `tests/host/drbd.rs`: DRBD with resources rw-*, minors 99990 to 99999, ports 11990 to 11999, replicas of one volume as separate resources on loopback addresses; ignored unless `R_DRBD_HOST_TEST=1` and `--ignored`.
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
