# Performance

Memory, CPU, provisioning latency and data path of r-drbd, measured on one small test bed.
The numbers are from one measurement session, on one day; they show the order of magnitude, not a guarantee.

## Targets

- Controller: under 50 MiB RSS idle, its CSI controller plugin included (it runs in the same process).
- Agent: under 30 MiB RSS, its CSI node plugin included.
- Provisioning: within the acceptance bounds this project holds itself to: a replicated claim Bound within 600 s, its pod Running within 300 s more, both replicas UpToDate within 300 s, a local claim Bound within 300 s.
- Data path: DRBD's own; r-drbd adds nothing on it.

Result: both memory targets are met (controller peak 7.9 MiB, agent peak 11.4 MiB).

## Bed

- Three virtual machines on one physical host, joined into one MicroShift cluster.
- Data node A: 4 vCPU, 5 GiB allocated, control plane, data and the r-drbd controller.
- Data node B: 2 vCPU, 3.5 GiB allocated, data.
- Node C: 2 vCPU, 4 GiB allocated, diskless tiebreaker, no Pool.
- Each data node: one 20 GiB virtio disk, itself a thin LV on an NVMe thin pool of the host.
- Inside each data node: one VG on that disk, one thin pool (64 KiB chunks, zeroing on), one Pool.
- One virtual L2 between the machines; ping round trip with 4 KiB payload 0.196 ms on average.
- Guest kernel 6.12 (EL10), DRBD 9.3.4, LVM 2.03.41.
- Release build, static musl binary of 9.8 MiB; image 12.3 MiB (the binary plus drbd-utils).

## Method

- Memory: for every r-drbd pod, from the node's `/proc`: VmRSS and RssAnon of every process in the r-drbd container, and `memory.current` of the container cgroup.
- The target is checked against VmRSS of the `r-drbd` process.
- Helper processes (`drbdsetup events2`, and in the burst `drbdadm`, `lvs`, `mkfs.xfs`) are reported apart as "helpers".
- CSI sidecars are upstream images, not counted in the target: four in the controller pod (provisioner, attacher, resizer, snapshotter), the node driver registrar in each agent pod; the pod totals with them are given once.
- Points: idle 10 min after start with no Volume; 20 Volumes (10 local, 10 with 2 replicas), each with a running pod, 5 min after the last pod started; peak during the burst that created them (20 claims and 20 pods at once, sampled every 2 s until all Running); 5 min after deleting everything.
- Provisioning: claim and pod created together, polled every 0.2 s; time to claim Bound and to pod Running.
- StorageClasses use `volumeBindingMode: WaitForFirstConsumer`, so Bound includes pod scheduling.
- Pods are pinned to a data node with a nodeSelector: local claims on node B, replicated claims on node A (the Primary).
- Why: when this was measured the scheduler could place an unpinned local claim on the node without a Pool, where it stayed Pending; the scheduler now sees per-node capacity and does not pick that node (see volumes.md, Scheduling).
- 10 sequential runs per class, each claim deleted before the next; p50 and p95 are nearest-rank.
- Snapshot, restore, resize and delete: 3 runs on the replicated class, median reported.
- Data path: fio 3.27 from one container image, 30 s runs, two runs each, the second reported.
- 4k random write: `--rw=randwrite --bs=4k --iodepth=1 --direct=1 --ioengine=libaio`.
- 1 MiB sequential write: `--rw=write --bs=1M --iodepth=8 --direct=1 --ioengine=libaio`.
- All runs on the first 1 GiB of the device; the sequential runs go first, so the random runs hit allocated thin chunks.
- Local and replicated: fio in a pod on node A on a `volumeMode: Block` claim of 2 GiB.
- Floor: a 2 GiB thin LV in the same thin pool on node A, written by the same fio image run with podman on node A itself, outside Kubernetes.

## Memory (MiB)

Idle 10 min after start, no Volume:

| Pod | r-drbd VmRSS | RssAnon | Helpers RSS | Container memory.current |
| --- | ---: | ---: | ---: | ---: |
| Controller | 5.3 | 1.3 | 0 | 5.4 |
| Agent, node A | 7.7 | 1.7 | 0.3 | 6.2 |
| Agent, node B | 8.9 | 1.6 | 0.3 | 2.2 |
| Agent, node C | 8.9 | 1.5 | 0.3 | 1.9 |

20 Volumes with their pods, 5 min after the burst:

| Pod | r-drbd VmRSS | RssAnon | Helpers RSS | Container memory.current |
| --- | ---: | ---: | ---: | ---: |
| Controller | 7.6 | 2.3 | 0 | 6.8 |
| Agent, node A | 11.3 | 4.8 | 0.4 | 13.9 |
| Agent, node B | 10.3 | 3.1 | 0.4 | 15.5 |
| Agent, node C | 10.1 | 2.6 | 0.4 | 5.5 |

Peak over the 9 samples of the burst (each column its own maximum):

| Pod | r-drbd VmRSS | RssAnon | Helpers RSS | Container memory.current |
| --- | ---: | ---: | ---: | ---: |
| Controller | 7.9 | 2.6 | 0 | 7.4 |
| Agent, node A | 11.4 | 5.0 | 11.5 | 17.3 |
| Agent, node B | 10.5 | 3.3 | 11.5 | 23.0 |
| Agent, node C | 10.2 | 2.7 | 0.4 | 6.1 |

5 min after deleting every Volume:

| Pod | r-drbd VmRSS | RssAnon | Helpers RSS | Container memory.current |
| --- | ---: | ---: | ---: | ---: |
| Controller | 7.5 | 2.2 | 0 | 6.2 |
| Agent, node A | 10.7 | 4.2 | 0.3 | 12.7 |
| Agent, node B | 9.9 | 2.6 | 0.3 | 14.5 |
| Agent, node C | 9.5 | 2.0 | 0.3 | 4.7 |

- Even the whole agent container stays under 30 MiB at its peak (23.0 MiB).
- The agent pod with its registrar reached 30.9 MiB on node A with 20 Volumes; the target is on the process, which stays met.
- The controller pod with its four CSI sidecars: 121.5 MiB idle, 162.2 MiB with 20 Volumes.
- `memory.current` can be below VmRSS: file pages of the binary first read by another cgroup count in RSS but are not charged to the container.

CPU time of each container over its first 10 min, idle:

| Container | CPU seconds |
| --- | ---: |
| Controller | 0.10 |
| Agent, node A | 0.57 |
| Agent, node B | 0.52 |
| Agent, node C | 0.10 |

For comparison, process RSS of a Java-based DRBD control plane, idle: 756 MiB for its controller plus 35 MiB for its wrapper, 345 MiB for its node agent.
Those figures come from another bed (one node, 2 pools, 2 resources) and another method; compare the order of magnitude only.

## Provisioning latency (s)

10 sequential claims with a pod each:

| Class | Bound p50 | Bound p95 | Pod Running p50 | Pod Running p95 |
| --- | ---: | ---: | ---: | ---: |
| Local, 1 replica | 0.73 | 1.04 | 7.29 | 8.41 |
| 2 replicas | 1.96 | 2.24 | 7.25 | 10.90 |

- Burst: 20 claims and 20 pods created at once (10 local, 10 replicated, spread over nodes A and B), all pods Running after 17.67 s.

Other operations, replicated class, median of 3:

| Operation | Median | Runs |
| --- | ---: | --- |
| VolumeSnapshot created to readyToUse | 1.57 | 1.57, 1.57, 1.29 |
| Restore claim and pod created to pod Running | 29.43 | 29.26, 29.43, 29.72 |
| Resize 1Gi to 2Gi to the file system grown in the pod | 47.34 | 47.34, 53.07, 38.77 |
| Claim deleted to Volume gone | 7.55 | 15.80, 3.85, 7.55 |

- The restore claims ask for 2Gi: when this was measured a snapshot of a 1Gi Volume reported the DRBD device size, a little above 1Gi, and a 1Gi restore claim was refused; snapshots now report the requested size and a same-size restore binds.
- No provisioning latency of the Java-based stack was measured, so there is no direct comparison.
- Every measured time is far below the acceptance bounds under Targets.

## Data path

Second run of each:

| Device | 4k randwrite p50 (us) | p99 (us) | IOPS | 1 MiB seqwrite (MiB/s) |
| --- | ---: | ---: | ---: | ---: |
| Floor: thin LV, host fio | 47.4 | 74.2 | 16819 | 1493.7 |
| Local Volume (plain LV) | 47.4 | 72.2 | 16760 | 1456.2 |
| 2 replicas, Primary on node A | 2474 | 4554.8 | 434 | 535.8 |

- A local Volume is the floor: it is a thin LV with no layer between the pod and LVM.
- A replicated write waits for the peer: protocol C, disk flushes left at DRBD's default.
- The L2 round trip (0.196 ms) explains little of the 2474 us p50; the peer's disk with a flush after every write runs at 1211 IOPS, 826 us per write (4k randwrite with `--fdatasync=1` on a thin LV of node B).
- The rest of the gap was not broken down further.
- Sequential throughput varies between runs on this bed: the floor did 2084.6 MiB/s in its first run and 1493.7 MiB/s in its second.

## Caveats

- Virtual machines with a stacked thin pool (guest thin pool on a host thin LV): the disks are virtio, the network a virtual bridge; real disks and NICs give other numbers.
- Shared host: the physical host runs other workloads, and the cluster ran an unrelated set of pods on every node; neither was stopped.
- Thin LVs with zeroing everywhere, the floor included; a first write to a chunk costs more than the steady state shown.
- One measurement session, on one day; no averaging across sessions or days.
- Memory is RSS, not a limit: set requests and limits from your own peaks with headroom.
