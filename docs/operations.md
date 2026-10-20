# Operations

## Reading status

    kubectl get pools,volumes,snapshots
    kubectl get volume pvc-<uuid> -o yaml
    kubectl get events --field-selector involvedObject.kind=Volume

Find the Volume of a PVC:

    kubectl get pvc demo -o jsonpath='{.spec.volumeName}'

Useful Volume status fields:

| Field | Meaning |
|---|---|
| `phase` | Pending, Provisioning, Ready, Degraded, Failed, Deleting |
| `primary` | Node where the device is open |
| `nodes` | Diskful nodes |
| `usableBytes` | Smallest size the diskful replicas on reachable nodes provide |
| `replicas[]` | One entry per node, written by its agent |
| `replicas[].diskState` | DRBD disk state: UpToDate, Inconsistent, Outdated, Diskless, ... |
| `replicas[].syncPercent` | Progress while resyncing |
| `replicas[].peers` | Connection state to each peer |
| `replicas[].quorum` | Whether this replica has quorum |
| `replicas[].lastError` | Last failed command on that node |
| `replicas[].unreachablePeers` | Peers this node's DRBD has not reached for a while, with a TCP probe result |
| `attachmentErrors[]` | Attached nodes the controller could not give a diskless replica, and why |

Conditions:

| Type | True means |
|---|---|
| `Ready` | Every diskful replica and the tiebreaker present; diskful replicas UpToDate and connected |
| `Degraded` | Usable, but not every replica is healthy; the reason and message name the node |
| `QuorumLost` | A replica in use has no quorum; IO there is suspended until quorum returns |
| `SplitBrain` | Two replicas diverged; the message names them |
| `PeerUnreachable` | A DRBD connection between two running nodes does not come up; the message names both nodes, the address and the likely cause |
| `AttachmentFailed` | A node in `spec.attachments` has no diskless replica (no free DRBD port on it, or all 32 node ids of the volume taken); `attachmentErrors` names it |

Degraded reasons: `ReplicaMissing`, `ReplicaNotUpToDate`, `ReplicaDisconnected`, `NodeUnreachable`.
A resize of a Ready volume keeps `Ready` True with reason `Resizing`.
A volume never Ready yet stays Provisioning below its size, with `Ready` False and reason `Resizing`: a restore larger than its snapshot grows before its first Ready.
A publish to a node listed in `attachmentErrors` fails at once with ResourceExhausted and that entry's message; the pod event shows it, other nodes are not affected.

Logs:

    kubectl -n r-drbd-system logs deploy/r-drbd-controller -c controller
    kubectl -n r-drbd-system logs ds/r-drbd-agent -c agent

Set `RUST_LOG=debug` on a container for more detail.

## A node goes down

r-drbd never moves data on its own.

- Its agent stops renewing its Lease; after 40 s its Volumes turn Degraded with reason `NodeUnreachable`.
- Replicated volumes keep serving from the surviving replicas, as long as they hold quorum.
- Local volumes on that node are unavailable until it returns.
- When the node returns, DRBD resyncs only the changed blocks and the Volumes turn Ready.

To restart a pod from a dead node on another node, Kubernetes must first release the attachment.
Use the standard non-graceful shutdown: once the node is really off, taint it

    kubectl taint node node2 node.kubernetes.io/out-of-service=nodeshutdown:NoExecute

and remove the taint when it is back.

## Node shutdown

With graceful node shutdown on (see install), a reboot or power-off releases every volume first:

- kubelet stops the pods, their volumes are unmounted and DRBD turns Secondary on the last close.
- The agent is a node-critical pod, stopped last.
  On SIGTERM it refuses new NodeStage and NodePublish calls; unstage and unpublish keep working.
- It then checks whether the node is going down: logind preparing for shutdown, systemd stopping, or, on hosts without systemd, kubelet's node shutdown condition on the agent's pod.
- If so, it waits briefly for running CSI calls, flushes each mounted file system it owns (`sync -f`; skipped on a device with suspended I/O while the DRBD event stream is up, tried for up to 4 s when it is not), then forces every Primary it owns to Secondary (`drbdsetup secondary --force`).
  A pod stuck on suspended I/O then gets I/O errors instead of holding the shutdown; the peers stay UpToDate.
- A volume still in use when the node goes down is flushed when it can be and then cut like a crash; the clean result needs graceful node shutdown.
- Without graceful node shutdown, systemd stops the agent together with everything else, and drbd-utils' `drbd-graceful-disconnect.service`, when loaded, disconnects an in-use Primary within a fraction of a second, before the agent can act: its peers turn Outdated until the node returns.
- An agent update or a pod delete is not a shutdown and demotes nothing.
  On hosts without systemd, that check waits up to 4 s for the pod.

The one gap: no guard runs when the agent pod is not running at that moment.
The cost is a volume that waits for the node to return, or a slow reboot; never data loss.

## Planned maintenance

1. `kubectl drain node2 --ignore-daemonsets`.
2. Reboot or service the node.
3. `kubectl uncordon node2`; replicas resync.

With 3 nodes, service one node at a time and wait for every Volume to be Ready before the next.

## Replacing a node: `spec.evict`

To rebuild the replicas of a dead or retired node elsewhere, list the node in each Volume's `spec.evict`:

    kubectl patch volume pvc-<uuid> --type merge -p '{"spec":{"evict":["node2"]}}'

For every Volume with a replica there:

    kubectl get volumes -o json \
      | jq -r '.items[] | select(any(.spec.assignment.replicas[]?; .node == "node2")) | .metadata.name'

The controller then:

1. Picks a new node with a Pool of the same tier (or the pinned Pool).
2. Creates an empty replica there; DRBD copies the full volume from an UpToDate peer.
3. Re-adds a tiebreaker if needed.
4. Removes the node from `spec.evict`.

It refuses any move while:

- A split brain is pending or being resolved.
- A snapshot is suspending, taking or resuming.
- The evicted node is attached, or its replica is in use as last reported, unless its Node object is deleted: a stale agent alone does not prove the workload stopped.

A diskful replica is also refused while:

- `status.initializedAt` is not set and the evicted node is the one that makes the first data: a volume never Ready holds nothing, so delete and recreate it.
  Any other replica of a volume never Ready moves as usual.
- No other diskful replica is UpToDate with quorum, or a previous rebuild is still running.
- The Volume is pinned to that node (`spec.node`, or `spec.pool` on that node).

A tiebreaker holds no data: it moves to another free node, or is dropped when there is none.

While blocked, it emits an `EvictionBlocked` Event and keeps trying.
On success, it emits `Evicted`.

Only one node per Volume moves per pass; a blocked node does not hold back the others listed.
Local volumes (`replicas: 1`) cannot be evicted; remove the node from `spec.evict`.

If the evicted node is alive, its agent takes DRBD down and keeps the LV as an orphan.
Remove it by hand once you no longer need it.

A dead node that will never return: delete its Node object.
That tells r-drbd the node is fenced, which lifts the quorum requirement on the remaining replica.
Never delete the Node of a machine that may still be writing.

## Split brain

DRBD quorum makes split brain rare; manual DRBD commands or unusual failure sequences can still cause one.

`SplitBrain` turns True and its message names the replicas:

    split brain between node1 and node2; set spec.resolve.survivor

Pick the node whose data wins, then:

    kubectl patch volume pvc-<uuid> --type merge -p '{"spec":{"resolve":{"survivor":"node1"}}}'

The controller discards the data on every other diskful replica, resyncs them from the survivor, clears `spec.resolve` and emits `SplitBrainResolved`.
Writes on the losing replicas since the split are lost.

Errors:

- `InvalidSurvivor`: the survivor holds no diskful replica.
- `ResolveWithoutSplitBrain`: there is no split brain; nothing is discarded; remove `spec.resolve`.

## Replicas never connect: `PeerUnreachable`

A replicated Volume stuck in Provisioning, or Degraded with `ReplicaDisconnected`, while every node is up, usually means DRBD traffic is blocked between nodes.

    kubectl get volume pvc-<uuid> -o jsonpath='{.status.conditions[?(@.type=="PeerUnreachable")]}'

How it is detected:

- A connection that stays out of Connected for 20 s, or keeps dropping, while both agents run and both replicas are up, counts as stuck.
- The stuck side's agent then opens a TCP connection from its DRBD address to the peer's DRBD address and port, and records the result in `replicas[].unreachablePeers`.
  It probes each connection at most every 5 minutes, again when a new drop follows a result older than 60 s, and at most 256 connections per pass.
- The controller turns `PeerUnreachable` True within about a minute and emits a Warning Event `PeerUnreachable`.
- A result counts only while the peer agent run the prober saw (`peerRun`, the Lease acquireTime) still holds the Lease and the controller has seen it renewed since the record appeared.
  Node clocks are never compared, so a peer booting with a wrong clock changes nothing.
  A peer that reboots or crashes is `NodeUnreachable`'s news, not a firewall.
  While two agent pods of one node overlap in a rollout, the run alternates and no new result is recorded; reports resume about 20 s after the old pod is gone.
- It turns False once the connection has held Connected for 10 s, at the agent's next pass (up to 30 s later).
  The Normal Event `PeersReachable` follows after 5 minutes of False, so a flapping link does not announce every drop.
  A controller restart inside those 5 minutes skips that Normal Event; the condition itself is correct.
- It stays False for a node whose agent is down, for a node in `spec.evict`, and for a StandAlone or split pair (that is `SplitBrain`).

Example message:

    rejected by a firewall between node1, node2, node3 (TCP 11000); open TCP 11000-11999 between nodes

The message has one clause per probe result; `replicas[].unreachablePeers` holds each direction.

Reasons and what to check:

| Reason | Probe saw | Check |
|---|---|---|
| `Firewall` | ICMP prohibited or unreachable, or a local filter | Open TCP 11000 to 11999 between nodes, both directions |
| `Timeout` | No answer in 3 s | A firewall that drops, or a routing problem between the addresses |
| `NotListening` | Connection refused | Check the firewall too: one that answers with a TCP reset looks the same. Then check that the Node InternalIP is the address DRBD should use on that host |
| `Unknown` | The port answers, or the probe failed | The kernel log (`dmesg`) on both nodes; the agent logs |

The reason is the most actionable one in the message.
A probe of a port where DRBD listens adds one line to that node's kernel log ("Error receiving initial packet") and changes nothing in DRBD.

Not covered: an authentication or protocol mismatch ends the connection StandAlone, so `PeerUnreachable` stays False.
It shows as Degraded `ReplicaDisconnected` (or Ready False while Provisioning), `replicas[].peers` reading StandAlone on one side, an agent `lastError` saying the adjust does not converge, and the reason in the kernel log of both nodes.

## A lost backing LV

If an LV that a node already reported disappears (deleted by hand, disk replaced):

- Replicated volumes: the agent creates an empty LV and DRBD syncs it in full from a healthy peer.
- Local volumes: the agent does not recreate it.
  The replica stays `present: false` with a `lastError`, so the loss stays visible.
  Restore from a backup into a new PVC, then delete the old one.

## Upgrading r-drbd

`kubectl apply -k deploy/base` of the new release: it carries the CRDs of that build.
The agent DaemonSet rolls one node at a time; DRBD devices stay up while an agent restarts.
