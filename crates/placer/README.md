# placer

Pure crate: where diskful replicas and the tiebreaker go.
Scope: distinct nodes, tier or pool or node pin, thin capacity with a fixed ratio, most free space first, tiebreaker on a node without a replica when the diskful count is even.

Tests under tests/:
- fits: thin ratio arithmetic at the boundaries, reserved space, zero capacity.
- filter: every Reason in order; node pin and pool pin; a node that already holds a replica is skipped.
- order: free bytes descending, then node name, then pool name; deterministic across runs.
- place: 1 replica, 2 replicas on 3 nodes, 2 replicas with one existing, insufficient nodes gives no partial result.
- tiebreaker: prefers the node with no pool of the tier, then fewest volumes, then name; none when all nodes hold a replica.

Dependencies allowed: none beyond std.
