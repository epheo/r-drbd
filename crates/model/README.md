# model

CRD types of r-drbd.io v1alpha1: Pool, Volume, Snapshot, with the shared names in `keys` and condition helpers.
Every per-node list is a list-map keyed by `node` so agents can server-side apply their own entry.
CEL rules enforce immutability (pool spec, volume replicas and tier, DRBD parameters of an assignment) and no shrink.
`model::crds()` returns the CRDs in install order; `r-drbd crds` prints them.
