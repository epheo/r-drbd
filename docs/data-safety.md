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
- A volume or snapshot LV is removed only when its Volume or Snapshot is deleted, or when the agent created it in the same pass and it never held data.
- Before removal the agent reads the LV report and waits while the LV is open; lvremove itself refuses an LV in use.
- That lvremove passes `--yes`: it answers LVM's prompt for an active LV, not the in-use refusal.
- The DRBD metadata on an LV belongs to the Volume and node named in an LVM tag on that LV.
  A later volume reusing the name never takes over an old LV's metadata.
- Thin pool zeroing is turned on, so a new LV never exposes blocks of a deleted one.
- Values from the API (VG and thin pool names, Volume and Snapshot names) are validated and can never reach an LVM or DRBD command as an option.

## Disks

- A disk is written only when a Pool lists it under `devices`, and only after the same pass found it blank: no signature, no partition, not held, not a multipath path, and an exclusive open succeeded.
- One wrong path stops the whole Pool before anything is created.
- The commands that build or remove a Pool from `devices` run without `--yes` or `--force`, so LVM's own refusal of a used disk stays in force.
- A volume group without the Pool's tag is never adopted, touched or removed.
- Nothing is ever wiped; a disk with old data stays as it is until you wipe it yourself.
- NodeDisks only reads; a disk in use is never opened by the inventory.
- Every read of a disk and every wait on udev has a time bound.
- See [Disks](disks.md) for what runs and every corner case.

## Replication

- Writes are synchronous (DRBD protocol C): acknowledged only once every connected replica has them.
- A new volume skips the first sync only when all its replicas are new and connected; a replica that joins later is copied in full.
- A replicated restore skips the first sync only when every replica restored the same snapshot from its own copy, at the snapshot's size.
  The proof is on disk and in status: each backing LV is tagged with the snapshot it came from at creation, the first node's LV has the size of its snapshot LV, each replica reports that snapshot in `restoredFrom`, and the Snapshot reached Ready only after every holder took its copy with IO suspended.
  Without that proof on every replica, one replica is the source and the others are copied in full.
- A restore larger than its snapshot grows after that first step; the grown area always syncs, because it holds each node's former DRBD metadata.
  The volume is Ready, and its claim usable, only once it has its full size and every replica is in sync.
- Quorum is a majority: a replica that loses quorum suspends IO instead of diverging.
- Nothing is evicted, re-placed or failed over automatically; moving a replica (`spec.evict`) or resolving a split brain (`spec.resolve`) is always your decision.
- `spec.evict` refuses to move a replica while it could lose data; see [Operations](operations.md).

## Bounded waits

- Every CSI call and every snapshot step has a time bound.
- A bound that expires becomes a condition or an error with a reason, never a silent hang.
