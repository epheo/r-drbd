# Golden images

Status: the target architecture; what is built is in section 3.

r-drbd as the storage plane of a KubeVirt hypervisor: VM disks created from golden images in seconds, sharing the image's blocks on every node, with local reads and the same data-safety rules as any replicated volume.

## 1. Summary

- A golden image is a VolumeSnapshot of a replicated volume, held in full by its 2 or 3 holder nodes.
- Any other node can take a full local copy of an image when it needs one: a warm copy. It is a cache, bounded by a budget, evicted when idle.
- Every VM disk replica is a thin snapshot of a local copy of its image, holder or warm. Creating a disk copies nothing; DRBD skips the first sync.
- A disk larger than its image starts at the full size: the copy's old metadata area is zeroed before DRBD metadata is created, so every replica is image plus zeros, and no grow or resync ever runs.
- A replica added later (a repair, an eviction) is cut from a local image copy too and joins with a checksum resync, so only the disk's own blocks cross the network and the disk stays thin.
- An image outlives its VolumeSnapshot while any disk references it.
- Locality at birth comes from WaitForFirstConsumer: the first replica is born on the VM's node. Locality after a migration comes from the VM manager: it sets a node preference from the replica nodes r-drbd publishes.
- The scheduler is never steered: no webhook, no extender, no node-name affinity on a replicated volume; only native signals.
- Everything uses stock DRBD 9 and LVM thin; no new kernel target; nothing on the data path.

## 2. Goals

| Goal | Statement |
|---|---|
| Immediate | A VM from an image is usable at once; only its own writes exist on disk. |
| Local | Reads and writes happen on the VM's node when a replica lives there. |
| Once | A VM reading a remote disk never reads the same byte twice over the network. |
| Bounded | Images are not on every node; copies exist where VMs of the image run, within a budget. |
| Native | Work with Kubernetes scheduling, not against it: capacity, topology and labels are welcome, a second scheduler or overriding placements are not. |
| Migration | Live migration (ReadWriteMany block, dual Primary) keeps working. |
| Safe | Synchronous replication, quorum and the rules of data-safety.md hold. |
| Portable | Generic KubeVirt and CDI settings only. |
| Free | Nothing is in production; fields, tags and on-disk layout change without migration. |

Under protocol C a write waits for a peer's flush wherever the VM runs. A local replica removes one network copy of each write, not that flush. Locality therefore pays for reads; the read-latency measurement (section 14) sets how much.

Not in this design (section 13): backing chains or qcow2, asynchronous replication, a partial block cache, storage following a migrated VM, a node health label.

## 3. Roadmap and status

| Step | What | State |
|---|---|---|
| Preconditions | The snapshot LV location, sizes and copy digests in the acks; a synced resize for every grow. | done |
| Status recovery | Snapshot LV tags and the recovery of a lost Snapshot status from them (6.7). | planned |
| Restore at full size | Section 7 and the pages it changes; its own change after the preconditions, independent of the rest. | planned |
| Capacity | Image-based replicas charged at full size (10.2). | planned |
| Measurements | Section 14, read latency first. | planned |
| Image lifecycle | Section 6. | planned |
| Repairs and warm copies | Sections 8 and 9. | planned |
| Placement | Section 10: the `capacity` parameter, labels, the manager contract, the KubeVirt guide. | planned |

- The state column is the part of this document that changes as steps land; the rest describes the target.
- The snapshot round keeps its drain and per-node acks; holder copies are checked by digest, never assumed identical (6.8).
- If the read-latency measurement shows local reads are not clearly faster, the copy rank at birth and `capacity: node` lose their reason; the rest stands.
- Measured numbers in this document come from one small test bed and give orders of magnitude.

## 4. Concepts

- Image: a Snapshot (the r-drbd kind behind a VolumeSnapshot) used as the source of disks; immutable for its life; never written.
- Holder: a node whose successful Taking ack carries `takenPool` for the image; its snapshot LV is the holder copy. Under live buffered writes holder copies can differ; they count as identical only when their digests match (6.8).
- Warm copy: a full copy of an image in a Pool of a non-holder node, made by a warm Volume; tagged `r-drbd.warm.<snapshot uid>`.
- Warm Volume: an internal Volume with `spec.purpose: WarmCopy` that makes one warm copy and is then deleted, leaving the copy behind.
- Local image copy: a holder copy or a warm copy in the Pool where a replica is placed.
- Copy replica: a disk replica cut as a thin snapshot of a local image copy, carrying the from tag and the proof below.
- Fallback replica: a disk replica that starts empty (no copy, budget refused, failed warm) and syncs in full; marked positively, never inferred.
- Seed: a replica joining an initialized disk, cut from a local image copy; it proves nothing and always syncs in full, with checksums.
- Initial node: the replica that performs the first data step.
- S: the image's net size (the source volume's size at the take).
- L0: the size of the local image copy's LV on a node (a holder copy has the holder's extent rounding; a warm copy is exactly the holder's `lvBytes`).
- la_size: the source volume's DRBD device size, read from the copy's own superblock before any change.
- Dead tail: the range [la_size, L0) of a copy replica before create-md: the source's old DRBD metadata, never data.
- L_img: the size floor of a replica's LV: the copy's L0 for a copy replica, the smallest holder `lvBytes` for a fallback, the preferred holder's `lvBytes` for both nodes of a warm Volume.
- G: a replica's gross LV size: the larger of the gross size for the requested size and L_img, rounded to its node's extent; never below the image.
- No-sync start: the first data made with `drbdadm new-current-uuid --clear-bitmap`, valid only when the image's copies are proven identical and every assigned diskful peer is connected and a proven copy.
- Proven: a copy replica whose proof (section 7.3) holds on a live read of the API server.
- Lineage: the identity of an image across its versions: the annotation `r-drbd.io/lineage` on the Snapshot, set by the VM manager or an admin; absent, the Snapshot uid is its own lineage.

## 5. Invariants

- X1 A no-sync start trusts only an image whose holder copies are proven identical by content: every holder's digest over [0, la_size) matches (`status.copiesIdentical`). Any other image starts from one copy, and every other copy joins with a checksum resync. A synced resize for every grow is in place before any no-sync start ships.
- X2 r-drbd writes an LV's data only through: lvcreate of a thin snapshot, write-zeroes of a dead tail, create-md, and DRBD itself. lvextend and tag changes alter no data. An image copy, holder or warm, is never written after its tag says so.
- X3 The no-sync start runs only when every assigned diskful peer is connected and proven on a live read, so no replica can be declared in sync without its proof, and none can connect after the decision.
- X4 Every count that keeps an image alive is read from Volume specs, never from status or memory.
- X5 r-drbd never writes an object it does not own: no PV annotations, no VM specs. It reports in its own status, and labels its own nodes only when a setting says so.
- X6 Every fallback from a fast path to a slow one (a full sync, a synced resize, a refused warm copy) emits an Event naming the values that failed.
- X7 A Snapshot is bound to its source Volume by uid, a Volume to its image by uid; names never bind.
- X8 Every wait has a bound and ends in a condition or an Event, never a silent hang.
- X9 Every status field derived from LV tags (`copyOf`, `zeroedFrom`, `seededFrom`, `fallback`) is derived on every pass, never remembered; of these only `copyOf` and `zeroedFrom` prove anything.
- X10 Every proving LV tag carries the Volume uid, so an LV left by a deleted Volume of the same name never passes for a new one's.
- X11 Every snapshot LV carries its Snapshot's uid in a tag from its lvcreate. Only an LV with that tag is adopted, rediscovered or counted as a holder copy; an LV tagged with another uid is never adopted or removed.
- X12 A lost Snapshot status is rebuilt from the snapshot LVs, never by a new round: new data never appears under a VolumeSnapshot the user may have seen ready.

## 6. Images and their lifecycle

### 6.1 Binding by uid

- CreateSnapshot reads the source Volume live and records its uid in `spec.volumeUid`, immutable.
- CreateVolume from a snapshot records the Snapshot's uid in `spec.source.snapshotUid`, immutable with `source`.
- A Volume restores only from the Snapshot with that uid. A Snapshot acts on its source Volume only when the uid matches; a same-name Volume with another uid counts as absent, and only the acks count.
- A Failed Snapshot is deleted and recreated under the same name with a new uid; nothing bound to the old uid can bind to it.

### 6.2 Where a holder copy lives

- The agent's successful Taking ack records `takenPool` (the Pool that holds the snapshot LV), `lvBytes` (its size) and `allocatedBytes` (its thin allocation). Every later ack copies the three forward unchanged, and `digest` and `digestBytes` once they exist.
- After that ack, `takenPool` alone decides where the node's snapshot LV lives for restores, holder lookups and deletes; the replica's current Pool never does. Before that ack the local replica's Pool is used: the round is still taking.
- A delete on a node without `takenPool` (a lost ack) searches the thin pools of the node's Pools for the exact LV name; an LV of that name outside a Pool's thin pool is an error, never removed. The delete removes an LV with this Snapshot's uid tag or with no snapshot tag (left before tags existed); one tagged with another uid is an error, never removed.
- The controller records `status.sourceReplicas` on the way into Taking: the source's replica count survives the source Volume.

### 6.3 Deleting an image

- DeleteSnapshot deletes the Snapshot object and returns success once it carries a deletion timestamp. It never waits for LV removal: with retention that wait could last months and the snapshot controller would retry forever.
- On each pass for a Snapshot with a deletion timestamp:
  1. Phase not yet Retired or Deleting: write phase `Retired`, Ready False with reason `Retired`, one `ImageRetired` Event.
  2. List Volumes live (a linearizable read); count references: any Volume object, whatever its phase or deletion timestamp, with `purpose: Disk` and this uid in `spec.source.snapshotUid`; write `status.referencingDisks`. On every other pass the controller keeps `status.referencingDisks` current from its cache, for visibility; only the release decision recounts from a live list.
  3. Above zero: stay Retired; the next pass comes from Volume events.
  4. Zero: phase `Deleting`; the holders remove their snapshot LVs; the finalizer goes when every holder acked.
- A Retired Snapshot never returns to Ready, never starts a new disk and never gets a new warm copy. Repairs still use its copies.
- Warm Volumes are never references. An uninitialized restore is a reference.
- The creation race is closed by order. CreateVolume writes the Volume, then re-reads the Snapshot live; if it is gone, deleting, Retired or of another uid, it deletes the Volume and answers NotFound. The controller writes Retired, then lists live, then releases. Either the list sees the Volume or the re-read sees Retired: a Volume can never reference a released image. An idempotent retry that finds its Volume runs the re-read first; it may delete a Volume the list had already counted, which is harmless: an unpublished Volume holds no data and the claim is provisioned again.
- `Pool.status.retainedImageBytes`: allocated bytes of holder copies and warm copies of Retired images in the Pool, not reclaimable while referenced.

### 6.4 A Volume whose image is gone

- A Volume with a source, no first data and no assignment, whose Snapshot is Retired, deleting or gone: one live GET confirms it, then phase Failed with reason `SnapshotRetired`; never placed. A uid that differs gives `SnapshotUidMismatch`.
- It stays a reference until deleted, and `referencingDisks` shows why.

### 6.5 Orphan report

- Each agent reports, once per LV, an LV in its Pools whose from, seed, zeroing or zeroed tag names a Volume uid that no Volume has: Event `ImageOrphanLv` on the Pool, naming the LV and the tag.
- Warm-tagged LVs are skipped: they belong to their Pool and are evicted by tag.
- The same report covers a snapshot LV whose snapshot tag names a uid that no Snapshot has, for example after objects were restored from a backup with new uids.
- Only after the caches synced and only for LVs older than one pass. Nothing is removed or retagged.

### 6.6 Images without their source

- With CDI's snapshot source format, the import PVC is deleted once the VolumeSnapshot is ready, so almost every image lives without its source Volume. That is the main case, not an edge.
- A Ready Snapshot stays Ready without its Volume; Volume deletion removes only the volume LV by exact name; restores and deletes find the snapshot LV through `takenPool`; the source name is used only to build the LV name.

### 6.7 Crashes and lost state

- The controller dies between the Retired write and the live list: the next pass lists again; Retired is already written, so the ordering of 6.3 holds. It dies after a zero count and before Deleting: the next pass lists live again before moving.
- An ack lost (an API error after the lvcreate): no `takenPool`; a delete takes the search path; a restore on that node finds no holder copy and the replica falls back with `NoLocalCopy`, until rediscovery: a Ready Snapshot with fewer holder acks than `sourceReplicas` runs the discovery search below on the nodes without an ack, and a copy found is acked again.
- A holder node down during Deleting: the finalizer waits for its ack.
- The source Volume deleted and recreated under its name, maybe in another Pool: the uid differs, so it is ignored, and the old image's LVs are found and removed through `takenPool` on every holder.
- A holder's snapshot LV lost by hand: restores on that node fall back; the delete acks not found.

Snapshot LV tags:
- The take creates the snapshot LV with the tag `r-drbd.snap.<snapshot uid>.<size bytes>.<source replicas>` in the same lvcreate: the Snapshot's `sizeBytes` and `sourceReplicas`, both known on the way into Taking.
- The take adopts an existing LV of the name only when it carries this uid; an LV without the tag or with another uid is an error, and the round fails without touching it.
- A round that ends Failed removes its LVs on every node that took one: a Failed Snapshot is never restored, and a partial copy left behind would pass for an image after a status loss.

Detecting a lost status:
- The controller writes the first status (phase Pending) before it adds its finalizer.
- A Snapshot with the finalizer and no status has lost its status; a new Snapshot never looks like one, so it pays nothing. Backup tools that restore objects usually keep finalizers and drop status, the case to catch.
- Without a deletion timestamp the controller writes phase `Discovering`; with one, the delete path below applies.

Discovery:
- Every node with a Pool takes part, not only the source's nodes: in the main case (6.6) the source Volume is gone and its nodes are unknown.
- Each agent searches the thin pools of its node's Pools for the exact LV name with this uid tag and acks either the copy (`takenPool`, `lvBytes`, `allocatedBytes`, the size and replica count from the tag, the LV's creation time) or none.
- The controller decides only when every node with a Pool and a Node object has answered: an agent that is down could hold a copy. After 10 minutes in `Discovering`, Ready False with reason `DiscoveryIncomplete` and one Event naming the silent nodes, and it keeps waiting. A node whose Node object is deleted no longer counts.
- One or more copies: phase Ready, with `sizeBytes`, `sourceReplicas` and `createdAt` rebuilt from the tags and LV times; copies whose tags disagree are an error, phase Failed with reason `SnapshotLost`. The digests then rebuild `copiesIdentical` (6.8).
- No copy on any node: phase Failed with reason `SnapshotLost` and one Event; never a new round. The user takes a new snapshot.
- Fewer holders than `sourceReplicas`: replicated restores of the image are refused with `UnsupportedRestore` until fallback replicas (7.4) ship; then they fall back and sync.
- A round never runs on a Snapshot in discovery, so old copies never mix with fresh ones of another moment.
- `fileSystem` is not rebuilt: a restore never makes a new file system, and an empty device is refused at stage.

Deleting after a lost status:
- The deletion timestamp still marks the retire: the next pass rewrites Retired and recounts (6.3).
- With no acks and no `sizeBytes`, the holder set is unknown, so the finalizer stays until every node with a Pool and a Node object has searched and acked: removed, or none found.
- The search removes per 6.2: this uid's tag or no snapshot tag, never another uid's.

### 6.8 Proving holder copies identical

- Why: a page rewritten while it is being written out reaches the local disk and the peer in two versions; the writer's next writeback heals it. A writer that dies first (a killed VM, an O_DIRECT buffer changed in flight) leaves the split for good, and no property of the writer or of the round rules it out. Only content does.
- After the round ends Ready, each holder reads its snapshot LV over [0, la_size), la_size from that LV's own superblock, and acks `digest` (a sha256 root over 4 MiB chunks) and `digestBytes`. Background, idle IO priority, one snapshot at a time per node; unmapped thin ranges read as zeros without disk IO, and a chunk that reads all zeros counts as the known hash of a zero chunk, so the cost follows the allocated bytes, not the virtual size.
- Once every holder acked, the controller writes `status.copiesIdentical` once: true when all pairs match; false otherwise, with `ImageCopiesDiffer` naming the holders. Absent while pending.
- While the digests are pending, a restore waits for them up to 2 minutes, and the holders hash that snapshot first. False, or still pending after the bound: the restore takes the sync-source path with checksums (7.5), correct and thin, not instant. Golden images, imported and detached before their snapshot, are expected to match and become instant once the digests are in.
- Warm copies need no digest: a completed DRBD full sync from the preferred holder makes the copy identical to it.

## 7. A disk from an image

Governs every replicated restore: plain restores, VM disks and the warm Volume's own fill.

### 7.1 Sizes

- CreateVolume sizes the Volume at the request, at least S. Every replica starts at that size: a restore never resizes its DRBD device before its first data.
- A replica's LV is G: never below L_img. Why: a replica whose usable size falls below la_size cuts the image's tail, where the file system end and a GPT backup header live. The source's la_size is at most the usable size of its smallest LV, so an LV never smaller than L_img holds it.
- A warm Volume's two LVs are exactly the holder's `lvBytes`.

### 7.2 Preparing a copy replica (agent)

Runs when the Volume is restoring and the replica's LV is missing in its Pool; it resumes on later passes from the LV's tags and size alone.

1. lvcreate a thin snapshot of the local image copy, with `r-drbd.new`, `r-drbd.from.<volume uid>.<snapshot uid>` and `r-drbd.seed.<volume uid>.<snapshot uid>` in the same command. Its size is L0.
2. Unless a zeroing or zeroed tag exists: read the superblock at L0 with `drbdmeta --force <unused minor> v09 <lv> internal dump-md`, read-only. `--force` because a snapshot of a live device carries an unclean activity log; never apply-al, never create-md here. Parse la_size and the metadata size. Checks, the first that fails makes the replica a fallback with `ProofFailed`, or stops the restore on the initial node:
   - C1 dump-md succeeded and the LV is still L0;
   - C2 S is at most la_size;
   - C3 la_size plus the metadata size is at most L0;
   - C4 L0 is at most G (true by construction; kept as an assertion).
   A la_size that differs from the size formula of drbd-config emits `MetadataSizeDiffers` once; it gates nothing, since that formula is a bound, not DRBD's value.
3. Tag `r-drbd.zeroing.<volume uid>.<la_size>`: la_size is pinned before anything changes and never re-read, because the lvextend moves the device end and the zeroing destroys the superblock.
4. lvextend to G when the LV is smaller.
5. Write zeroes over [la_size, L0) of the LV device with BLKZEROOUT. Never below la_size; never at or past L0; [L0, G) has no thin mappings and reads zero unwritten. Never a discard: on dm-thin a discard of part of a chunk keeps the old bytes.
6. fdatasync the LV device: dm-thin commits new mappings only on a flush, and an LVM tag commit does not flush the pool.
7. One lvchange: add `r-drbd.zeroed.<volume uid>.<la_size>`, drop the zeroing tag.
8. create-md at G and the metadata tag, after the resource file is written.

Resume table, from tags and size only:

| On the LV | Next |
|---|---|
| from tag, no zeroing or zeroed tag, size L0 | step 2 |
| zeroing tag, size L0 | step 4 |
| zeroing tag, size G | steps 5 to 7 |
| zeroed tag, no metadata tag | step 8 |
| metadata tag | done |
| from tag, no zeroing or zeroed tag, another size | fallback, `ProofFailed` |

Guards: only an LV with no metadata tag and no attached DRBD disk is ever zeroed; the image copy is never written; the zeroing lands on the replica's own thin mappings.

### 7.3 The proof of a copy replica

A replica's entry reports `copyOf` (the Snapshot uid) and `zeroedFrom` (la_size, from the zeroed tag only, never the intent tag) while all hold: the from tag, the zeroed tag, the LV at G. The no-sync start requires the image's `copiesIdentical` to be true and, on a live read, every assigned diskful peer connected and proven with the same `zeroedFrom` as the initial node. Equal `zeroedFrom` is the property the start needs: the same image end and the same zero start everywhere; it also catches a holder snapshotted mid-resize.

A parser test on captured real dump-md output pins the fields: a value parsed wrong but inside the accepted range passes the checks, and only that test catches it.

### 7.4 Fallback replicas

- A peer becomes a fallback when its Pool has no local copy and no warm copy in progress (`NoLocalCopy`, `CopyNotUsable`), the lvcreate failed (`RestoreFailed`), a check failed (`ProofFailed`), or the Snapshot is gone or of another uid (`SnapshotNotFound`). After a restart the reason reads `Unknown`. The entry carries `fallback: <reason>`, derived every pass from the tags: an LV in its Pool with no from tag for this Volume and Snapshot. A missing field means unknown, never fallback.
- None of these makes the initial node a fallback: its restore does not start, and the error names the cause.
- Its LV: a new empty thin LV at G with `r-drbd.new`; a warm Volume's target at exactly `lvBytes`; after a failed check, every tag of this design except `r-drbd.new` is dropped and the LV extended to G; the full sync overwrites the content.
- The marked entry is written before any other step of the replica: it lets the initial node end its wait at once on a connected fallback, while an unmarked connected peer is waited for as a possibly lagging entry (7.5).
- A fallback comes up like any replica: resource file, create-md at G, up; DRBD syncs it in full from an UpToDate peer, zero chunks as discards. It is never declared in sync: the no-sync start requires every assigned diskful peer connected and proven, so a fallback forces the sync source (7.5). One `RestoreFullSync` Event names the replica and its reason.

### 7.5 First data (initial node)

- The initial node is a copy replica, never a fallback; the controller picks it among replicas whose Pool can give a copy: a holder copy, then a warm copy, then a Pool being warmed; ties by placement order.
- Peer classes, for each assigned diskful peer: proven (connected, Inconsistent, its entry present with its device path, `copyOf` and the initial node's `zeroedFrom`; the device path guards against a report from before a lost LV); warming (no LV yet, a warm Volume exists for its Pool); connected fallback (connected with a disk, its entry marked); connected unproven (connected with a disk, not proven, not marked: possibly an entry that lags the connect); absent (not connected).
- Once its own proof holds, the initial node's bounds all start at that pass and run side by side: 60 s for each assigned peer to connect and prove, the warm Volume's bound for a warming peer, 2 minutes for pending digests. It decides at once, as the sync source, on `copiesIdentical` false or a connected fallback. Otherwise it decides when every assigned peer is connected and proven or past its bound, and `copiesIdentical` is true or past its bound: the wait is the longest bound still open, never their sum. An unmarked connected peer not yet proven is waited for within its bound, since a status entry can lag the connect.
- Then it reads the Volume live; a failed live read waits. If `copiesIdentical` is true and every assigned diskful peer is connected and proven with its `zeroedFrom`, it runs `new-current-uuid --clear-bitmap`: no sync. Otherwise it runs `new-current-uuid --force-resync`: every connected peer syncs; copy replicas carry the seed tag, so their connections run with checksums and only differing blocks move, keeping the thin share; a peer that connects later joins the same way. `RestoreFullSync` names the peers and the cause.
- Because the no-sync start needs every assigned diskful peer connected and proven, no replica can connect after the decision and be declared in sync.
- Proven peers lose nothing to a forced start but a checksum read pass, run alongside any other sync; the claim waits for the slowest (decision 12).
- The first Ready needs every diskful replica UpToDate at full size, so a fallback delays the claim by its sync (decision 12).

### 7.6 Growth after first data

- A restore never grows before its first data. Every DRBD resize of an in-use volume syncs: the old metadata area turns into data and syncs; `--assume-clean` is never passed.
- A size raised before the first data: the LVs keep their G; the synced resize runs after.
- An LV grows only before its metadata exists (the preparation's lvextend, or a new LV) or under an attached disk, where DRBD moves its metadata online.

### 7.7 Crashes and lost state

- After each preparation step the next pass continues from the tags: a crash before the zeroing tag re-reads an intact superblock; after it, the tag's value drives the repeat; a power loss between the zeroing and the flush loses zeroes, and the missing zeroed tag repeats them.
- Once the zeroed tag exists no zero write ever repeats: a crash after create-md and before the metadata tag reruns create-md only. When G equals L0 the new metadata sits inside [la_size, L0), and a repeated zero write would destroy it.
- Status lost: `copyOf`, `zeroedFrom`, `fallback` come back from the tags; preparation never touches an LV with a metadata tag; first data still refuses while any replica holds data.
- Image copy lost after the lvcreate: the replica keeps its blocks (dm-thin refcounts); before: `NoLocalCopy`, or a new warm copy.
- A peer down before the first data: the initial node waits 60 s, then starts as the sync source with the connected peers; the peer joins later, with checksums when it is a copy.
- Snapshot status lost: the acks' `lvBytes` go with it until the holders are rediscovered (6.7); meanwhile a fallback sizes its LV without the floor, and a too-small LV is refused by DRBD with the error shown.
- The initial node down before the first data: nothing starts until it returns; moving the initial node is a later liveness item.

### 7.8 Snapshots of local volumes

A replicated restore from the snapshot of a single-replica volume is refused with `UnsupportedRestore` (`sourceReplicas` is 1, or absent): its LV has no metadata tail, its data can span the whole LV, and it has no second copy. Golden images are imported through the replicated VM class.

## 8. Warm copies

### 8.1 When

- Placement chose a Pool for a replica of an image-based disk before its first data, and the Pool holds no copy: no ack has `takenPool` there and `status.warmCopies` has no entry for the image.
- The controller creates a warm Volume only when all five checks hold:
  1. the Snapshot is Ready, not Retired, with `sourceReplicas` 2 or 3;
  2. the preferred holder (the lowest node name among holders with a fresh agent, chosen at creation) has a fresh agent and an ack with `lvBytes`;
  3. the Pool's extent size divides `lvBytes`;
  4. the Pool's `warmBytes` plus the image's cost (the ack's `allocatedBytes`, else S) stays within `warmBudgetPercent` of its size;
  5. no warm Volume for that image and Pool exists.
- Otherwise `ImageWarmRefused` names the failed check and the replica takes the fallback path.
- A replica placed in a Pool without a copy has exactly two outcomes: a warm Volume exists and the disk's Ready reason is `WaitingForImageCopy` until the copy is warm; or no warm Volume (any refusal, or a warm Volume that fails later) and a fallback replica with `NoLocalCopy`.
- Repairs and evictions never ask for a warm copy: a join uses a copy that exists in its Pool, else it is an empty join. A warm copy before one join moves the same bytes in two passes and keeps the volume degraded longer; it pays only when more disks of the image land in that Pool, which is placement's call.

### 8.2 The warm Volume

- Name: `warm-` plus the first 16 hex digits of the sha256 of `<snapshot uid>/<pool>`: deterministic, so a restarted controller finds it; never collides with CSI names.
- Spec: `purpose: WarmCopy`, replicas 2, ReadWriteOnce, the Snapshot's size and uid, the Pool's tier, and the assignment written in the same create: the preferred holder as the initial node in its `takenPool`, the target in its Pool, no tiebreaker. The controller writes the assignment itself: the placer has one Pool pin for every replica and cannot express two. Capacity for the target is the budget check; the holder side is a thin snapshot and needs none.
- The resource file keeps the fixed option set. Quorum holds where it matters: the target is the holder's only peer and a fallback, so it comes up at once and the holder becomes the sync source with 2 of 2 connected. A disconnect only pauses the sync: nothing ever writes a warm Volume.
- Both LVs are exactly `lvBytes`: the holder side is a thin snapshot of the snapshot LV; the target creates its LV at `lvBytes` from the ack, never at its own rounding, which on a smaller extent could land below the holder's device size and make DRBD refuse the connection.
- `ImageWarming` on the Snapshot names the Pool.
- Never Primary, never attached, never counted in `committedBytes`; every CSI call refuses a WarmCopy Volume; charged once to the target Pool's warm budget.

### 8.3 Down and keep

- The controller deletes the warm Volume when the target's entry is UpToDate and connected, or when the Volume failed or made no sync progress for 10 minutes (`ImageWarmFailed`).
- The target, if UpToDate, connected to the holder, and with a device size at least the holder entry's `zeroedFrom` (so a short copy is never tagged): add `r-drbd.warm.<snapshot uid>` and `r-drbd.idle.<now>`; `drbdadm down`; drop the metadata, from, new, seed, zeroing and zeroed tags; `ImageWarmed`. Otherwise: down and remove the LV; a device below `zeroedFrom` also gives `ImageWarmFailed`. DRBD's metadata stays on the warm LV: a disk cut from it reads la_size there.
- The holder: down, then remove its temporary thin snapshot; its snapshot LV is untouched.
- A warm copy whose device size exceeds the holders' la_size (mixed extent sizes at the source) is complete, since the bytes past la_size are synced zeroes; a disk mixing holder and warm copies of that image then fails the `zeroedFrom` equality and starts as the sync source, which is rare and safe.
- Once tagged, the warm LV belongs to its Pool's agent for its life: removed only by eviction, never by a Volume or Snapshot deletion, never adopted by a later Volume of the same name. data-safety.md gains this third owner.

### 8.4 A replica waiting for a copy

The replica's agent reads the warm Volume live: present and not Failed, it waits with `WaitingForImageCopy`; absent, Failed, or deleting without the warm tag appearing, it takes the fallback path. The warm Volume is created before the assignment the agent reacts to, so one live read is enough.

### 8.5 Status, idle clock and eviction

- `Pool.status.warmCopies[]`: one entry per warm-tagged LV with the Snapshot uid, LV name, size, allocated bytes and `idleSince`; `warmBytes` is their allocated sum; `extentBytes` is the VG extent size. Rebuilt from tags after a status loss.
- The agent adds `r-drbd.idle.<unix seconds>` when no Volume of that image has a replica in the Pool, and drops it when one has. Restart safe.
- Eviction: idle for the grace (24 h), or over budget (the admin lowered it) and idle at least 10 minutes, longest idle first. The minimum protects against a cache lag that marks a copy idle while a replica for it is on its way. Removing a copy never touches the disks made from it: dm-thin keeps their shared blocks. `ImageEvicted` goes on the Pool, since the Snapshot may be gone.
- An eviction racing a new placement is safe: the new replica finds no copy and no warm Volume, and falls back.

### 8.6 Crashes and lost state

- The target's agent dies between the warm tag and the down: on restart the Volume is deleting, so it runs the down and the tag cleanup; the tag stays valid, since nothing writes a warm Volume. It dies between the down and the cleanup: the cleanup finishes (warm tag present, Volume gone). It dies before the warm tag while the Volume is deleting: the disk is not UpToDate and connected on restart, so the LV is removed and the waiting replica falls back. It dies mid-sync: DRBD resumes.
- The holder's agent dies: its temporary LV is removed on restart through the deletion path; the snapshot LV is untouched.
- The controller dies after creating the warm Volume and before the assignment: the warm Volume completes or fails on its own; an unused copy goes idle and is evicted.
- The preferred holder is down: no new warm Volume; a running one stalls and fails after the 10-minute bound; waiting replicas fall back.
- A warm LV lost by hand: its entry leaves `warmCopies`, new replicas fall back, existing disks keep their data.
- Pool status lost: `warmCopies` comes back from the tags on the next pass.

## 9. Repairs from an image copy

### 9.1 Which joins seed

A join is a diskful replica that comes up with just-created metadata on an initialized Volume: a rebuilt LV, a replica placed by an eviction, a copy replica that missed the no-sync start. It is never declared in sync; it always syncs in full from an UpToDate peer. A seed only changes how many bytes move and how many chunks get allocated, never what ends on disk.

The agent seeds when: the Volume is initialized and image-based; its Snapshot exists (always true for a referenced image; only a uid mismatch fails); the Pool holds a usable copy of that image; the copy's LV is L0 and G is at least L0. Otherwise an empty join; for an image-based Volume `ReplicaFullSync` names the missing condition, and a plain volume's rebuild raises nothing.

A copy replica that missed the no-sync start already carries the seed tag from its creation; its entry shows `seededFrom` beside `copyOf` and it joins as any seed. A copy proven at the start loses its seed tag within one pass.

### 9.2 Preparation

The steps of 7.2 with the seed tag and no from tag; G from the current size, which a grow may have raised; a failed check drops every tag but `r-drbd.new`, extends the LV and joins empty; the dead tail is zeroed all the same so checksums send nothing for it; S for the second check is the Snapshot's size, and with that status lost the check fails and the join is empty; `ReplicaSeeded` names the node and the copy.

### 9.3 Checksums on the joining connections

- `csums-alg sha256` is rendered inside the connection block, on both ends, only while a connection has a seed at one end: the joining node on all its connections while its LV carries the seed tag, every peer on its connection to a node whose entry shows `seededFrom`. DRBD may refuse a connect whose ends disagree, so a seed not connected to an UpToDate peer within 10 minutes drops its tag and joins empty, with `ReplicaFullSync`. The peers' setting follows the joining node's status, so it lags one status write and one pass; if adjust reconnects a live connection to add the option, a seeded sync may restart once at its start, which is harmless.
- Resync rate options (c-max-rate, c-plan-ahead, c-fill-target, c-min-rate) are set only on the joining node's peer devices while its first sync is pending; the values come from the measurements of section 14. DRBD's resync controller runs on the sync target, so the joining node's settings count.
- `verify-alg sha256` goes into every replicated resource, so `drbdadm verify` can run anywhere. It changes the fixed option set: an agent upgrade rewrites every resource file. If DRBD refuses a connect whose ends disagree, a rolling upgrade degrades every volume between an upgraded and a not-yet-upgraded node until both are upgraded; the first rollout is free, and later changes of the option set then need a two-phase rollout or a maintenance window.

### 9.4 The sync and its end

With checksums, both ends read each block and only differing blocks cross the network and are written; matching blocks stay shared with the image; zero chunks travel as discards. An interrupted sync resumes from DRBD's bitmap. When the disk is UpToDate the agent drops the seed tag within one pass; `seededFrom` leaves the entry and every end drops the checksum and rate options on its next render.

### 9.5 Where an eviction joins, and quorum

The eviction's placement ranks Pools with a copy first, then by room; a Pool without a copy gives an empty join. Quorum rules: one move per Volume and pass, refused while no other diskful replica is UpToDate with quorum, or a previous rebuild still runs. After every seeded join in the tests, `drbdadm verify` on the joining connection must end with zero out-of-sync blocks; a difference fails the test and is never resynced silently.

## 10. Placement and scheduling

### 10.1 CreateVolume

1. Resolve the Snapshot; answer NotFound for a Retired or deleting one, with no Volume written; refuse a single-replica source with `UnsupportedRestore`.
2. Size: the request, at least S; the disk starts at its full size.
3. First-replica node: the PVC pin if set; else the scheduler's node (`preferred[0]` under WaitForFirstConsumer) for an image-based replicated volume or a local one; else none.
4. Write the Volume with the source name and uid, then re-read the Snapshot live (6.3).
5. Placement runs in the controller pass; CreateVolume answers ResourceExhausted on Insufficient, never a final error, so the provisioner drops the selected node and the scheduler picks again.

### 10.2 Placement

- Candidate Pools: usable (node live, Pool Available) for the tier. Order: the first-replica node first; then Pools with a local copy (an ack with `takenPool` there, or an entry in `warmCopies`); then warmable Pools (every warm-copy check except existence would pass); then room descending; then names. One Pool per node; never partial.
- Every image-based replica is charged to `committed` at the disk's full size. There is no exemption for restores: charging only the growth beyond the snapshot would let one Pool take unlimited instances at golden-image scale. Until warm copies exist, a busy image stops instancing at the holders' cap with ResourceExhausted.
- The initial node is picked among the chosen replicas as 7.5 says; a placement in which no chosen replica can get a copy is Insufficient, never a restore that cannot start. This rule is for births; a repair onto a cold Pool is an empty join.
- A Pool chosen without a copy gets a warm Volume and the disk waits with `WaitingForImageCopy`; a Pool chosen without a copy and not warmable gives a fallback replica.
- A birth whose first replica is not on the scheduler's node emits `PlacedOffSchedulerNode` with the node and the reason.
- Warm Volumes are never placed by the placer and never summed into `committed`.

### 10.3 Capacity seen by the scheduler

- A new StorageClass parameter `capacity`: `cluster` (default) or `node`; `node` is accepted only with replicas 2 or 3, and the class parse answers InvalidArgument otherwise, at CreateVolume and at GetCapacity.
- With `node`, GetCapacity answers on each node the largest size whose whole placement fits with that node as the first replica, and 0 when none fits; the scheduler then never sends a birth to a node without a Pool or with a full Pool. With `cluster`, every node reports the cluster-wide value.
- The coupling applies at birth only: the scheduler reads capacity for unbound WaitForFirstConsumer claims; restarts and migrations of bound volumes are never gated by it.
- Capacity objects are published with a lag, so births can still race a change; ResourceExhausted plus a reschedule stays the recovery. The pinned answer removes the systematic loop, not every race.

### 10.4 Replica nodes for the VM manager

- The diskful nodes of a volume are `spec.assignment.replicas[]` with role diskful, also listed comma-separated in `status.nodes`; each node's current disk state is `status.replicas[].diskState`. Prefer UpToDate. This is the documented contract; no new field is needed.
- The VM manager, or a VM owner by hand, sets a preferred node affinity on `kubernetes.io/hostname` for those nodes on each VM. r-drbd never writes it. After an eviction, a repair onto another Pool or any other change of the set, the manager re-sets it from status.
- With replicas 2, a drain of one replica node then prefers the other: locality survives upgrades with no data moved. The preferred node needs room for the VMs of both; a soft preference falls through when it is full, those VMs run over the network until their next migration, and a VM that fell through keeps the preference and heals at its next drain.
- At birth the preference cannot help: under WaitForFirstConsumer the replicas are placed after the scheduler chose the node; birth locality is 10.1 to 10.3.
- Placement orders Pools by most room first, so the other replicas of many disks sit on the largest Pools and a drain sends many VMs toward the same nodes; the drain test (section 15) measures it before any spreading change.
- A manager reading the contract needs get, list and watch on volumes.r-drbd.io; the guide shows that Role.

### 10.5 Image labels on nodes

- Off by default; `R_DRBD_IMAGE_LABELS=true` on the agent turns them on.
- When on, each agent writes one label `image.r-drbd.io/<lineage hash>` per image lineage present on its node as a holder or warm copy, valued with the current version's Snapshot uid; the hash is the first 16 hex digits of the sha256 of the lineage value. Written only when a copy turns warm, is evicted, appears or goes; never per pass; at most 32 per node, beyond which the least recently used lineages carry no label and `ImageLabelsCapped` says so once. Every label change is a Node update seen by every Node watcher, which is why the cap and the transition rule exist.
- A VM owner may prefer such nodes at birth in its own spec; r-drbd never reads the label.

## 11. API changes

Volume:
- `spec.source.snapshotUid` (controller, required with a source, immutable).
- `spec.purpose`: `Disk` (default) or `WarmCopy` (controller, immutable; WarmCopy needs a source, replicas 2 and ReadWriteOnce). Why not `role`: it already names replica roles.
- `spec.assignment` of a WarmCopy Volume is written by the controller at creation.
- `status.replicas[].copyOf`, `zeroedFrom`, `fallback` (values `NoLocalCopy`, `CopyNotUsable`, `RestoreFailed`, `ProofFailed`, `SnapshotNotFound`, `Unknown`), `seededFrom`; all agent-written from tags. A replica entry names its restore source only by `copyOf`, the Snapshot uid; there is no name-based source field.
- Ready reasons `WaitingForImageCopy`, `SnapshotRetired`, `SnapshotUidMismatch`; `UnsupportedRestore` also covers a single-replica source.

Snapshot:
- `spec.volumeUid` (controller, immutable); phase `Retired` with Ready reason `Retired`; phase `Discovering` with Ready reasons `DiscoveryIncomplete` and `SnapshotLost`; the first status written before the finalizer; `status.referencingDisks`; `status.sourceReplicas`; `status.acks[].takenPool`, `lvBytes`, `allocatedBytes` (agent, written once, copied forward); `status.acks[].digest` and `digestBytes` (holders, written once after Ready); `status.copiesIdentical` (controller, written once).
- Annotation `r-drbd.io/lineage` (manager or admin). A manager finds the Snapshot from a VolumeSnapshot through the VolumeSnapshotContent's snapshot handle, which is the Snapshot name.

Pool:
- `spec.warmBudgetPercent` (0 to 100, default 20; 0 disables warm copies); `status.warmCopies[]` keyed by Snapshot uid with `lv`, `sizeBytes`, `allocatedBytes`, `idleSince`; `status.warmBytes`; `status.extentBytes`; `status.retainedImageBytes`.

StorageClass parameters: `capacity` (`cluster`, `node`). Agent settings: `R_DRBD_IMAGE_LABELS`; `R_DRBD_WARM_EVICT_GRACE_SECS` for tests.

LV tags (next to the existing `r-drbd.new`, `r-drbd.md.<volume uid>.<node id>`, `r-drbd.from.<volume uid>.<snapshot uid>`): `r-drbd.snap.<snapshot uid>.<size bytes>.<source replicas>` on every snapshot LV, `r-drbd.zeroing.<volume uid>.<la bytes>`, `r-drbd.zeroed.<volume uid>.<la bytes>`, `r-drbd.seed.<volume uid>.<snapshot uid>`, `r-drbd.warm.<snapshot uid>`, `r-drbd.idle.<unix seconds>`. Any path that makes an LV new again, or leaves it behind, drops every tag of this design except the warm tag. Uids are 36-character UUIDs.

Events: `RestoreFullSync`, `MetadataSizeDiffers`, `ReplicaSeeded`, `ReplicaFullSync`, `PlacedOffSchedulerNode` (Volume); `ImageRetired`, `ImageCopiesDiffer`, `ImageDiscoveryIncomplete`, `ImageLost`, `ImageWarming`, `ImageWarmed`, `ImageWarmFailed`, `ImageWarmRefused` (Snapshot); `ImageOrphanLv`, `ImageEvicted` (Pool); `ImageLabelsCapped` (Node). One per transition; the per-replica ones once per replica at its check.

Constants: copy digest chunk 4 MiB; digest wait bound 2 min; warm eviction grace 24 h; minimum idle before an over-budget eviction 10 min; warm Volume progress bound 10 min; seed connect bound 10 min; `csums-alg` sha256 on joining connections; `verify-alg` sha256 everywhere; image label cap 32 per node.

RBAC: the agent patches Nodes and creates Events; the controller creates, patches and deletes Volumes and writes Snapshot status; a manager reading the replica-node contract needs get, list and watch on volumes.

## 12. Decisions

1. Order of work: the preconditions, the status recovery, then the restore at full size, the capacity rule, the measurements, the image lifecycle, repairs with warm copies, placement (section 3).
2. An image outlives its VolumeSnapshot while any disk references it, through the Retired phase. Blocking the deletion instead would make tools that rotate image versions retry forever and would full-sync every repair of a disk whose image is gone.
3. Full warm copies per Pool, no partial cache. No stock kernel target gives copy-on-read with the guarantees needed, and a userspace cache would put r-drbd on the data path; a full copy costs the image's allocated size once per Pool and shares with every disk there. A partial cache stays a later option if warm copies prove too costly.
4. Warm copy budget 20 percent of a Pool by default.
5. sha256 for the checksum resync and for verify; a faster hash only if the measurements justify it.
6. No node health label. A node whose DRBD or LVM is broken while kubelet is healthy already raises a Node Event; adding a topology term later costs nothing.
7. Storage following a migrated VM is not part of this design. With the manager's node preference it would serve only VMs that fall through on every drain, at the cost of reading the whole disk on two nodes per move; revisited after the measurements of section 14.
8. Image node labels off by default; the lineage rule is specified so turning them on is a switch.
9. The restore at full size: every replica starts at the requested size with its dead tail zeroed, proven by tags, a flush and equal zero starts. A restore at the image size followed by a synced grow would walk the grown area at the resync rate, about 6 s per GiB measured, so a 2 GiB cloud image into a 40 GiB disk would wait about 4 minutes; sizing images at the disk size would multiply copies by size class. Grows of in-use volumes are synced.
10. Per-node capacity is built as the class parameter `capacity`, default `cluster`. The VM class sets `node` once the read-latency measurement shows that locality at birth is worth keeping new VMs off nodes with full Pools.
11. A `purpose` field on Volume rather than a separate kind: one store, one allocator, one placement path.
12. A restored claim is usable only after every replica's first sync. In the designed steady state every replica is a proven copy and the claim is immediate anyway; the early form would add a Degraded-from-birth state whose quorum behaviour is unmeasured, and it can become a class parameter later.
13. Holder copies are proven identical by a content digest, not assumed from the round or from a quiet window. Under live buffered writes a page can reach the local disk and the peer in two versions, and a writer that dies first leaves the split for good; a quiet window bounds only the host's writeback. The digest costs one read of the allocated bytes per holder and catches every cause.
14. No fallback isolation: a replica without a copy comes up like any other, and the no-sync start runs only when every assigned replica is connected and proven; otherwise the initial node is the sync source with checksums on every copy connection. Isolation with a relaxed start rule was the alternative; the strict rule is race-free without a gate, and a forced start no longer thickens proven replicas, so isolation would only save them a checksum read pass.
15. A lost Snapshot status is rebuilt from tags on the snapshot LVs, detected by a finalizer without a status, and never answered with a new round. A label on the Snapshot would be a second, editable record that can disagree with the disks; a new round would put new data under a VolumeSnapshot the user may have seen ready, and could mix old copies with fresh ones.

## 13. Not in this design

- A partial block cache (decision 3).
- Storage following a migrated VM (decision 7). If it ever comes: a class parameter, add a seeded replica on the VM's node on publish, trim one unattached replica after a dwell, never below the replica count, never during a migration, one move per node at a time, best effort; it would be the one exception to the rule that r-drbd never moves replicas on its own.
- A node health label required by replicated volumes (decision 6). If it comes: never from NodeGetInfo, removed only on a positive failure, with hysteresis; no taint, since a taint repels every pod.
- A scheduler extender, a second scheduler, a pod-mutating webhook (it rewrites specs owned by the VM manager or by GitOps), node-name affinity on replicated volumes (hard and immutable, stale after any move, and it stalls a drain when both replica nodes are unavailable), a PV annotation mirroring the replica nodes (PVs belong to the external provisioner; a manager needs one read rule instead).
- A per-volume node label (one label per disk on every node), a list of copies on the Snapshot (warm copies are Pool contents), a retained flag (the Retired phase says it), a DRBD device size in the acks (a later relaxation that would let holders with mixed extent sizes take the no-sync path).
- Limiting a join's resync to the disk's changed ranges by crafting DRBD metadata; rejected. Using thin_delta to drive a range-limited resync is a study item with the default answer no.
- Moving the initial node of a restore to another copy replica while it is down: a later liveness item.
- Fallback isolation: keeping a replica without a copy disconnected until the proven replicas have started, which allowed a start rule that ignores unconnected peers. Dropped together with that rule for the strict rule of 7.5, which is race-free without a gate; it can return as an optimization if the checksum pass proves costly on large disks.

## 14. Open measurements

Each result goes into this document before the step it gates; the list shrinks as results land.

- Read latency inside a VM, local replica against a network attachment: random 4k and sequential 1 MiB reads at p50 and p99, a boot and a package install, fixed guest memory, cold and warm. KubeVirt runs QEMU with cache none on block, so the guest page cache is the only cache and a re-read crosses the network only on a guest miss. Decides the value of locality, the copy rank at birth, the `capacity` default and whether follow is ever revisited. The bar: p50 read latency at least twice lower, or a VM-like load at least 20 percent shorter, cold.
- The checksum resync: a 10 GiB image plus a 500 MiB delta, a seed cut from a copy, bytes on the wire, time, allocation on the target, verify clean, at DRBD's default and a raised rate limit, VM latency during it, sha256 against crc32c; whether DRBD refuses a connect whose ends disagree on `csums-alg` or `verify-alg`, and whether a live change needs a reconnect; which configuration section carries the per-peer-device rate options and which end's values the resync uses.
- The no-grow restore: a sweep of image sizes and extent sizes comparing the formula's usable size with DRBD's la_size (how often the diagnostic Event fires); dump-md with `--force` on a snapshot of a live device writes nothing (LV checksum before and after); whether `new-current-uuid` is accepted without quorum; how drbdmeta takes a minor with no resource configured, else the agent reserves one; what quorum a replica entry reports before the first data with every disk Inconsistent; BLKZEROOUT then fdatasync then a hard reset reads zero; a 2 GiB image into a 40 GiB disk on two replicas is Ready with no resync bytes and verify clean.
- The checksum pass of a forced start: its rate at DRBD's default and raised controller settings; whether unmapped zero ranges count against the rate limit; the source's CPU and read amplification with two checksum targets; digest bytes on the wire per GiB. From the measured 8 GiB zero sync at about 170 MiB/s: minutes per 40 GiB at the default pace, tens of seconds at a raised pace bound by hashing.
- The holder digest: time for a 10 GiB image with 3 GiB allocated and for 100 GiB with 30 GiB; the 2-minute wait bound against it.
- Divergence under live writes: compare holder copies only with IO suspended on every node or through verify, never by reading the two nodes at different moments; the same round under a buffered writer inside a VM; whether any difference outlives one minute without a writer.
- Live migration onto a replica mid checksum resync: the VM keeps running, a writer inside loses nothing, verify clean.
- The warm copy path: a 10 GiB image with 3 GiB allocated copied through a two-replica restore by hand; time and bytes (within 10 percent of the allocated size), data areas equal, the holder's snapshot LV untouched, the sync completing without a tiebreaker.
- Quorum with three diskful replicas, one Inconsistent, plus a tiebreaker: who keeps quorum under each partition. Only needed if follow is ever revisited.
- KubeVirt affinity updates: a running VM accepts a changed preferred affinity and the next migration uses it; which rollout strategy propagates it; a drain of one replica node with N VMs, counting how many land on the other replica node, and a second drain showing the fallen-through VMs healing. Needs a multi-node KubeVirt cluster.
- CDI on the version in use: the import PVC is deleted with the snapshot source format; a snapshot clone under WaitForFirstConsumer binds only when the VM's pod is scheduled; a larger clone gets its size from CDI without an expand step on block; a source PVC name is never reused for the same digest.

## 15. Acceptance

Every step ships only with these green on the image built from its merge commit, with the host suite green. Each test records its evidence (sizes, times, bytes received, verify output). No test writes to an image copy. Every verify in this section runs with the writer stopped for at least 60 s, or with IO suspended: under live buffered writes verify reports transient differences.

Preconditions:
- A round on a volume with no writer gives matching digests on every holder, over ten rounds.
- A round under a buffered writer may give differing digests: then `copiesIdentical` is false with `ImageCopiesDiffer`, its restore takes the sync-source path with checksums, and verify after the writer stopped is clean.
- The digest of a 10 GiB image with 3 GiB allocated completes in the background; a holder copy altered by hand gives false and the Event.
- Snapshot LV location: evict a replica off a node and back into another Pool on it, delete an older snapshot; the LV is gone on every holder, no orphan reported.
- A grow of an in-use volume syncs: both LVs identical over the old metadata range after the resync; no `--assume-clean` in the agent's commands.

Status recovery:
- A Ready Snapshot whose status is cleared, with its source Volume deleted and with it present: Ready again with the same holders, `sizeBytes`, `sourceReplicas` and `copiesIdentical`; no round, no IO suspended; a restore matches the source.
- The same with one holder's agent stopped: `Discovering` until it returns, `DiscoveryIncomplete` after 10 minutes, never a round.
- The same with every copy removed by hand: Failed with `SnapshotLost`, no round.
- An LV of the snapshot's name with another uid or no tag on a node: the take fails the round and leaves it untouched; the orphan report names an LV whose uid has no Snapshot.
- A round failed at Taking: the LVs it took are gone on every node.
- A delete after a cleared status: every copy gone, the finalizer removed only after every node with a Pool searched.

No-grow restore:
- Host tests: the dump-md parser on captured output; the preparation sequence with a crash injected after each step, converging without touching live metadata; an LV with a metadata tag never zeroed; the peer classes and the first-data decision from cached and live entries; G never below the image; the seed tag removed only at the first UpToDate.
- A 2 GiB image into a 40 GiB disk, two replicas: Ready within 10 s, under 8 MiB received per replica, no resync, verify clean, every replica equal to the image over [0, la_size) and zero past it, the image copy's checksum unchanged.
- Fallback with one copy removed: the restore starts as the sync source; the proven peers keep an allocation near the image's share (checksums), the fallback reaches the image's share, verify clean, the Event names the reason.
- A copy that connects before its entry shows its proof is waited for, and the start is the clear-bitmap.
- Every peer proven with digests pending: the start waits up to 2 minutes, the clear-bitmap on true, else the sync source; false gives the sync source at once.
- A holder copy altered by hand: its restore moves only the altered chunk.
- A single-replica snapshot refused as an image.

Capacity: a restore whose full size exceeds a holder Pool's room is refused with ResourceExhausted; an accepted one counts at full size.

Image lifecycle:
- Delete the source PVC after its snapshot is Ready; a restore and a repair from it work; after the VolumeSnapshot is deleted the Snapshot shows Retired with its referencing count; the VolumeSnapshotContent goes at once; holder LVs stay until the last disk is deleted; retained bytes drop to zero after.
- Recreate the source under the same name in another Pool, delete the old snapshot: the old LV goes on every holder, the new volume untouched.
- A CreateVolume racing a retire never leaves a Volume referencing a released image.
- An orphan LV is reported once and never deleted.

Repairs and warm copies:
- A disk placed in a cold Pool: the Pool warms with `ImageWarming` then `ImageWarmed`, the LV is tagged and listed, the warm Volume is gone, the holder's snapshot LV checksum unchanged, data areas equal, bytes received within 10 percent of the allocated size; a second disk of the image in that Pool starts at once.
- Repair after a node loss, and after an eviction, onto a Pool with a copy: the new replica is a thin snapshot of the copy, bytes on the wire under twice the delta, allocation near the delta's share, verify clean; afterwards the seed tag is gone from every LV and `csums-alg` absent from every resource file.
- A late copy replica joins with checksums and keeps its thin share.
- An eviction onto a Pool without a copy starts an empty join at once with `ReplicaFullSync` and creates no warm Volume.
- A warm copy idle past the grace is evicted with `ImageEvicted` and a later disk warms again; a budget at its cap refuses a new image's copy or falls back, never above the budget; eviction while VMs of the image run elsewhere leaves their data untouched.
- An agent upgrade with mixed versions: one node on each version, a forced reconnect; whether DRBD refuses it until both match, and how long volumes stay Degraded during a rolling update.

Placement:
- Instancing under WaitForFirstConsumer with pods pinned to a holder node, a warm node and a cold node: holder and warm Ready within 5 s, cold after its warm copy, the first replica on the pod's node in all three.
- A larger disk at birth through the same path.
- Live migration with a writer onto a node without a replica: the writer loses nothing, verify clean after.
- Eviction of a warm copy while VMs run on other replicas: no pod affected.
- A verify on every instanced volume: all clean.
- With `capacity: node`: capacity objects per node, zero for a node without a Pool and for a full one, a claim never scheduled to a Pool-less node, cluster-wide classes unchanged.
- On a multi-node KubeVirt cluster: N VMs on one replica node with the manager's preference set; drain; count local landings; drain again; count heals; no VM loses its disk.

## 16. Documentation

- snapshots.md and data-safety.md: a replicated restore starts at its requested size with no grow and no sync; the proof in plain words (each replica's copy is tagged at creation, the old metadata area is zeroed and flushed, the replicas agree on where the image ends); a replica that cannot use its copy starts empty and is copied in full, and the copies then sync with checksums, so they stay thin; every later grow syncs; a warm copy as the third owner of an LV; deleting a VolumeSnapshot keeps the image while disks use it.
- pools.md: no restore exemption; warm copies, their budget and eviction; retained bytes.
- volumes.md: the `capacity` parameter.
- operations.md: repairs and evictions from an image copy.
- reference.md: every field, Event and reason of section 11.
- A new KubeVirt guide: the VM StorageClass (WaitForFirstConsumer, ReadWriteMany block), the VolumeSnapshotClass, the CDI StorageProfile settings (snapshot clone, snapshot source format), golden images imported through the replicated class, the replica-node contract and the manual affinity form, thin pool metadata sizing for snapshot-heavy pools, overcommit guidance with alerting on free bytes.
