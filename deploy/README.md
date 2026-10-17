# deploy

Static manifests; copy them and set your own StorageClass names and tiers.
Order: the CRDs from `r-drbd crds`, then `kubectl apply -k deploy/base`.
Sidecar versions: csi-provisioner v6.3.0, attacher v4.13.0, resizer v2.2.1, snapshotter v8.6.0, node-driver-registrar v2.18.0.
The image holds static binaries on scratch: r-drbd and drbd-utils. With R_DRBD_HOST_NS every other agent command re-execs the binary into the host IPC and mount namespaces and runs the host's lvm, xfsprogs and mount (hostPID and privileged are required for that). MicroShift and OpenShift also need the privileged SCC binding in 00-namespace-rbac.yaml.
