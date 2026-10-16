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
| `usableBytes` | Size every replica provides |
| `replicas[]` | One entry per node, written by its agent |
| `replicas[].diskState` | DRBD disk state: UpToDate, Inconsistent, Outdated, Diskless, ... |
| `replicas[].syncPercent` | Progress while resyncing |
| `replicas[].peers` | Connection state to each peer |
| `replicas[].quorum` | Whether this replica has quorum |
| `replicas[].lastError` | Last failed command on that node |

Conditions:

| Type | True means |
|---|---|
| `Ready` | Every replica present, UpToDate and connected |
| `Degraded` | Usable, but not every replica is healthy; the reason and message name the node |
| `QuorumLost` | A replica in use has no quorum; IO there is suspended until quorum returns |
| `SplitBrain` | Two replicas diverged; the message names them |

Degraded reasons: `ReplicaMissing`, `ReplicaNotUpToDate`, `ReplicaDisconnected`, `NodeUnreachable`, `Resizing`.

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

## Planned maintenance

1. `kubectl drain node2 --ignore-daemonsets`.
2. Reboot or service the node.
   With `drbd-graceful-down.service` enabled, DRBD goes down cleanly at shutdown.
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

It refuses to move a replica while it would risk data:

- The volume was never Ready.
- A split brain is pending or being resolved.
- A snapshot is in progress.
- No other diskful replica is UpToDate with quorum, or a previous rebuild is still running.
- The evicted node's agent is alive and its replica is in use or attached.

While blocked, it emits an `EvictionBlocked` Event and keeps trying.
On success, it emits `Evicted`.

Only one node per Volume moves at a time.
Local volumes (`replicas: 1`) cannot be evicted.

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

## A lost backing LV

If an LV that a node already reported disappears (deleted by hand, disk replaced):

- Replicated volumes: the agent creates an empty LV and DRBD syncs it in full from a healthy peer.
- Local volumes: the agent does not recreate it.
  The replica stays `present: false` with a `lastError`, so the loss stays visible.
  Restore from a backup into a new PVC, then delete the old one.

## Upgrading r-drbd

Apply the new CRDs, then the new manifests.
The agent DaemonSet rolls one node at a time; DRBD devices stay up while an agent restarts.
