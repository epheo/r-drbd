# Data safety

When r-drbd is unsure, it stops and reports, instead of guessing.
These are the guarantees.

## File systems

- A device is formatted only when blkid finds no signature at all.
- A partition table, or any signature blkid cannot name, refuses to mount; it is never formatted.
- An XFS file system the Volume has no record of (for example after its status was lost) is adopted and mounted, never formatted.
- mkfs writes a temporary label and clears it once it succeeds; only that label proves a format was cut short and may be redone.
- A Volume that records a file system but whose device is empty refuses to mount: a fresh file system would hide the loss.
- A restored volume never gets a new file system.

## Lost data stays visible

- A local volume whose LV disappeared after it was reported is not recreated.
  Its replica reports `present: false` and a `lastError` until you delete the Volume.
- A replicated volume rebuilds a lost LV as an empty one with fresh DRBD metadata, then DRBD copies everything from a healthy peer.
  A rebuilt replica never claims to hold data it does not have.
- A restore whose snapshot LV is missing fails; it never creates an empty volume in its place.
- A snapshot that failed on any node ends Failed; it is never reported Ready.

## LVM

- r-drbd touches only LVs inside the thin pool of a Pool.
  An LV with the expected name elsewhere in the VG is an error, never resized or removed.
- LVs are deactivated before removal, so an LV still in use fails loudly instead of being torn away.
- The DRBD metadata on an LV belongs to the Volume and node named in an LVM tag on that LV.
  A later volume reusing the name never takes over an old LV's metadata.
- Thin pool zeroing is turned on, so a new LV never exposes blocks of a deleted one.
- Values from the API (VG and thin pool names, Volume and Snapshot names) are validated and can never reach an LVM or DRBD command as an option.

## Replication

- Writes are synchronous (DRBD protocol C): acknowledged only once every connected replica has them.
- A new volume skips the first sync only when all its replicas are new and connected; a replica that joins later is copied in full.
- Quorum is a majority: a replica that loses quorum suspends IO instead of diverging.
- Nothing is evicted, re-placed or failed over automatically; moving a replica (`spec.evict`) or resolving a split brain (`spec.resolve`) is always your decision.
- `spec.evict` refuses to move a replica while it could lose data; see [Operations](operations.md).

## Bounded waits

- Every CSI call and every snapshot step has a time bound.
- A bound that expires becomes a condition or an error with a reason, never a silent hang.
