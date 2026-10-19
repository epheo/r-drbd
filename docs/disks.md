# Disks

Two features touch disks r-drbd did not make: Pools built from `devices`, and NodeDisks.
[Pools](pools.md) says how to use them.
This page is the contract: exactly what runs, the limits, and what happens in each corner case.

## What runs on a disk

Reads, on every listed path and on every whole disk of the node:

- the by-id link is resolved; sysfs gives partitions, holders, the read-only flag, size, model and serial;
- the host's udev database (`/run/udev/data`) gives the multipath and ready flags, and whether udev has processed the disk;
- `wipefs --no-act --json` lists signatures and partition tables; it never writes;
- an exclusive read-only open, closed at once; the kernel refuses it while anything holds the disk;
- `pvs`, `vgs` and `lvs` reports.

Writes, only for a Pool with `devices`, only on paths it lists, only after the checks pass in the same pass, under one per-node lock shared with the disk inventory:

| Step | Runs when | Command |
|---|---|---|
| Create the VG | No VG of that name; every listed path is `Blank` | `vgcreate --addtag r-drbd.pool.<pool>` on all of them |
| Create the thin pool | The VG carries the Pool's tag and has no thin pool | `lvcreate --type thin-pool` with the chunk, the metadata size and zeroing on |
| Add a disk | An appended path is `Blank` | `vgextend` |
| Grow | Free extents in the VG | `lvextend` of the metadata, then of the data |
| Remove | `reclaimPolicy: Delete`, Pool deleted, no Volume left, nothing else in the VG | deactivate, `lvremove`, `vgremove`, `pvremove` on the paths the status showed as `Member` |

Never:

- `--yes`, `--force` or `-ff` on any LVM command; LVM's own refusal of a used disk stays in force;
- `wipefs -a`, `pvcreate` on its own, `vgreduce`, `pvmove`;
- any command on a VG without the Pool's tag, beyond reports;
- any write on a disk only NodeDisks knows about: the inventory reads.

## Limits

| What | Limit |
|---|---|
| `devices` | At most 64 paths, all under `/dev/disk/by-id/`, no duplicates; append only; the mode (with or without `devices`) is fixed at creation |
| `chunkSize` | A power of two from 64Ki to 1Gi; default 64Ki; fixed at creation |
| Metadata | `data / chunk * 64 B + 64 MiB`; LVM caps it near 15.81 GiB; at creation the chunk doubles until the rule fits; on growth the chunk stays and data stops at what the capped metadata covers |
| Steps per pass | At most 8; the next pass continues |
| Exclusive open | Returns within 10 s or the disk reads `Busy`; one open per disk at a time |
| `wipefs` | 10 s in the inventory; never run on a `Busy` disk |
| udev | A disk without a udev entry reads `Busy` for up to 60 s, checked every 5 s; then the other checks decide |
| Inventory | Changes coalesce for 2 s; a full pass at start and hourly (every 5 min under `watch: poll`); a pass stops after 120 s and names what it skipped; at most 512 rows |
| Inventory writes | Only on change; a failed write is retried after 30 s, doubling to 5 min, without rescanning |

## Not in this release

- Removing a disk from a Pool, or replacing a failed one in place.
- RAID inside a node: one disk failure loses that node's replicas; the other nodes keep the data.
- Selecting disks by rule (model, size, label) or a template that makes Pools on many nodes.
- Partitions, multipath maps, encrypted devices.
- Wiping anything.

## Corner cases

Each row says what the agent does and what you see.
"Blocked" means Available False with the reason named, and nothing created.

### Listing a path

| Case | What happens | What to do |
|---|---|---|
| A partition, dm, md, loop, RAM, zram, DRBD, nbd, RBD, ZFS volume, null block, floppy or optical device, an eMMC boot area | `NotADisk`; blocked | List the whole disk's by-id link |
| A disk with a file system, an LVM label or a partition table | `NotEmpty`, naming each signature and its offset; blocked | If the data is unwanted, `wipefs -a` it yourself; r-drbd never does |
| A mounted disk, or one held by dm or md | `Busy`, naming the mount or the holder; blocked | Unmount or free it |
| A multipath path (`DM_MULTIPATH_DEVICE_PATH` set), or a device udev marks not ready | `Busy`, never opened | Multipath is not supported; list a plain disk |
| A disk the kernel just announced, before udev wrote its entry | `Busy`, "udev not settled", rechecked every 5 s | Wait; after 60 s without an entry the other checks decide |
| A read-only disk | `NotADisk`, "read-only" | |
| A disk with no medium (size 0) | `NotADisk`, "no medium", never opened | |
| Two by-id links of the same disk (its `wwn-` and its bus link) | The second reads `NotADisk`, naming the first; blocked | See "a wrong path" below |
| A path that does not resolve | `Missing`; blocked before the build | Check the spelling; plug the disk |
| A wrong path, before the VG exists | Blocked; nothing was created, and `devices` cannot shrink | Delete the Pool and recreate it with the right list |
| A wrong path appended after the build | Left out; the Pool keeps its capacity and stays Available; the row says why; one Warning Event `DeviceNotUsable` per change of its state | Nothing is needed; the row stays as a reminder |
| The udev database is unreadable from the agent | One warning at start; a multipath path can read `Blank` | Do not list multipath paths on that host |
| Another program claims a blank disk at the same instant | At most one side gets `EBUSY`: r-drbd reports `Busy`, or its LVM step fails and the next pass retries; r-drbd writes nothing | |
| r-drbd's instant open lands while another program claims the disk | That program's exclusive open can fail once; the open is read-only and closed at once | Retry the other program |

### Building

| Case | What happens | What to do |
|---|---|---|
| Two Pools on one node list the same disk | Pools run one at a time: the first builds; the second reads the disk `NotEmpty` (an LVM label, "PV of VG ...") and is blocked | Fix the second list |
| A disk visible from several nodes (a shared LUN) listed by Pools on two nodes | Both agents can find it `Blank` at the same moment and both create a VG on it | Never list a shared disk |
| Two Pools on one node use the same `vg` | The second finds the VG tagged for the first: `VolumeGroupNotOwned` | Give each Pool its own VG |
| A VG of that name made by hand | `VolumeGroupNotOwned`; never adopted, never touched | Use a Pool without `devices` for it |
| A Pool recreated under its old name after `Retain` | The tagged VG is adopted with its data | |
| A Pool recreated with fewer paths than the VG has disks | The VG is complete, so the Pool runs; the unlisted disks are missing from `status.devices`, and NodeDisks shows them `Member` of the Pool | List all of them |
| A Pool recreated under a new name over a retained VG | The tag names the old Pool: `VolumeGroupNotOwned` | r-drbd adopts the VG once its tag names the new Pool (`vgchange --deltag`, `--addtag`); that is your call |
| The agent restarts between two steps | The next pass resumes from what is on the disks | |
| A crash inside `vgcreate` itself | LVM labels without a VG; the disks read `NotEmpty` (`LVM2_member`) | `pvremove` them; the build goes on |
| `use_devicesfile = 1`, no devices file, and the host has PVs | Refused before `vgcreate` or `vgextend`, reason `Building`, naming the PVs: that command would create the file with only the new disks and hide them | `vgimportdevices <vg>` per VG on the node, never `--all` (it would also import PVs found on DRBD devices) |
| `use_devicesfile = 1`, no devices file, and the host has no PV | Built: the file `vgcreate` makes lists the new disks, which are then the host's only PVs | Nothing |
| The host's LVM filter excludes the listed disks | `vgcreate` fails; reason `Building` with the LVM error | Fix the filter or the devices file |
| An LVM command fails before the thin pool exists | Reason `Building`, with the step and the error; retried every pass | Read the message |
| A build takes more than 8 steps in one pass | "still building after 8 steps"; the next pass continues | |

### Growing

| Case | What happens | What to do |
|---|---|---|
| A `Blank` path appended | `vgextend`, metadata grown to the rule, data grown into the free space; Volumes keep running | |
| A growth step fails (lock timeout, no room) | The Pool stays Available at its size; the `Available` message holds the error; one Warning Event `PoolNotGrown`; retried every pass | Read the message |
| The metadata is at LVM's maximum | Data grows only to what the metadata covers by the rule; the rest of the VG stays out; the message and a `PoolNotGrown` Event say how much | For larger Pools, a new Pool with a bigger `chunkSize` |
| The thin pool reports a chunk size of 0 | Growth is skipped; the message says so | |
| A by-id link changes its name (firmware, udev rules) while the VG is complete | The row reads `Missing`; the VG and the Pool keep running, because LVM finds the disk by its label | Append the new link: it reads `Member`, nothing runs |

### Running

| Case | What happens | What to do |
|---|---|---|
| A member disk fails or disappears | The VG is partial: `DeviceMissing`, Available False; DRBD detaches the replicas on IO errors and their Volumes turn Degraded; the LVs stay reachable for teardown | Move the replicas off with `spec.evict`, then delete the Pool |
| An LVM report fails once on a built Pool | The usual probe decides, as for a Pool without `devices` | |
| An LVM report fails on a Pool that never had a member | The Pool is not used until the reports work: its VG might not be the Pool's | |
| Someone makes another LV in the VG | The Pool runs; r-drbd touches only its thin pool | Under `Delete`, that LV blocks the removal |
| Someone removes the Pool's tag from the VG | `VolumeGroupNotOwned`: the Pool is unavailable and no new work runs there | `vgchange --addtag r-drbd.pool.<pool>` |
| A host without lvm2 (a diskless tiebreaker) | No Pool with `devices` can be built there; the inventory runs without LVM reports and cannot name `Member` rows | |

### Deleting

| Case | What happens | What to do |
|---|---|---|
| Volumes still placed here | The finalizer waits, as always; nothing removed | Empty the Pool first |
| `Retain`, or no `devices` | The finalizer goes; the VG stays, tagged | |
| `Delete` | Thin pool deactivated and removed, VG removed, labels removed from the `Member` disks; `Released` True (`Removed`); then the finalizer goes; the disks are blank | |
| `Delete` with another LV in the VG | `Released` False (`RemoveBlocked`) naming the LV; nothing removed | Remove the LV, or switch to `Retain` |
| `Delete` with a missing disk | `Released` False (`RemoveBlocked`); nothing removed | Switch to `Retain`; clean up by hand (`vgreduce --removemissing`, `vgremove`, `pvremove`) |
| `Delete` on a VG without the tag | `Released` False (`RemoveBlocked`); nothing removed | |
| `pvremove` fails | `Released` False (`RemoveFailed`), retried; the rows keep `Member`, "pvremove pending" | Read the message |
| A crash after `vgremove` | Labels left on the disks; cleared only on the paths the status showed as `Member`; any other labelled disk is never touched | |
| The node is gone for good | Nothing can run there | Switch to `Retain`; the finalizer goes; the disks keep the VG |
| Switching from `Delete` to `Retain` while deleting | The finalizer goes at once; nothing more is removed | |

### NodeDisks

| Case | What happens | What to do |
|---|---|---|
| A disk plugged, unplugged, partitioned, formatted, wiped or labelled, then closed | Shown within seconds (the kernel reports it) | |
| A mount or an unmount | Shown within seconds (the mount table reports it) | |
| A disk made a PV and held by its LVs before the inventory read the label | `NotEmpty` naming the VG within seconds: LVM's reports are read again once the disk is held | |
| A writer that never closes the disk (a VM, a raw database, a long `dd`) | Shown at the hourly check | |
| A holder leaving no trace in sysfs (an iSCSI target, an exclusive open) | `Busy` only when opened; kept until the disk's next event or the hourly check | |
| A disk without any by-id link | `path` is `/dev/<name>`; a Pool cannot list it | Give the disk a serial or WWN |
| A disk of a Pool without `devices` | `Member`, naming the Pool whose `vg` holds it; several such Pools: the first by name, the others in the message | |
| A disk of a VG tagged for a Pool not on this node, or for two Pools | `NotEmpty`, saying so | |
| A disk of any other VG (the root VG) | `NotEmpty`, naming the VG | |
| Hidden NVMe multipath paths, floppies, eMMC boot areas, zvols, RBD, null block | Left out, or `NotADisk` | |
| More than 512 disks | The list is cut; `status.message` says so | |
| A pass over 120 s (a sick disk) | The disks not reached are named in `status.message`, read again 30 s later | |
| No uevent socket, or the socket fails later | A full check every 5 min; `status.watch` is `poll`; one warning | |
| A burst of events overflows the socket | One full pass; the watch goes on | |
| The write fails (RBAC missing, API down) | Retried after 30 s, doubling to 5 min; scans are unaffected; the object is stale until then | Give the agent `get, create` on `nodedisks` and `patch` on `nodedisks/status` |
| The object is deleted by hand, or the Node is recreated | It comes back at the next change, or within the hour | |
| The Pool's exclusive open and the inventory's | Both take the node's lock: they never overlap | |

## Host LVM after a build

- The VG r-drbd makes is an ordinary VG: `pvs` and `vgs` on the host show it, and boot activates it like any other.
- The tag `r-drbd.pool.<pool>` says whose it is; do not remove the VG by hand while the Pool exists.
- With a devices file, `vgcreate` and `vgextend` add the disks to it, so the host's LVM keeps seeing them.
- The host's LVM must still not scan DRBD devices or LVs; see [Install](install.md).
