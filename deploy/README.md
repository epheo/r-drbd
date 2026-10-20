# deploy

Static manifests; copy them and set your own StorageClass names and tiers.
`kubectl apply -k deploy/base` (or `deploy/dev`) installs everything, the CRDs included.
`base/crds.yaml` is the output of `r-drbd crds`; a workspace test fails when it differs from the code.
`kubectl delete -k` deletes the CRDs too, and with them every Pool, Volume and Snapshot object: use it only to uninstall.
Sidecar versions: csi-provisioner v6.3.0, attacher v4.13.0, resizer v2.2.1, snapshotter v8.6.0, node-driver-registrar v2.18.0.
The image holds static binaries on scratch: r-drbd and drbd-utils. With R_DRBD_HOST_NS every other agent command re-execs the binary into the host IPC and mount namespaces and runs the host's lvm, xfsprogs and mount (hostPID and privileged are required for that). MicroShift and OpenShift also need the privileged SCC binding in 00-namespace-rbac.yaml.
`openshift/` adds what OpenShift needs beyond that: the DRBD module through Kernel Module Management, graceful node shutdown and an LVM devices file on every node.

## Image version

`base` pins one build: `ghcr.io/epheo/r-drbd:sha-<short commit>` with `imagePullPolicy: IfNotPresent` on the controller and the agent.
The tag is written in the two pod manifests and in the `images:` entry of `base/kustomization.yaml`, which overlays override.
Every agent then runs the same build, and a restarted pod starts from the node's copy even while the registry is unreachable.

`dev` follows `:latest` with `imagePullPolicy: Always`: each restart takes the newest main build.
Use it for development only: agents can end up on different builds, and an agent restarted during a registry outage stays down with its node's replicas.

    kubectl apply -k deploy/base    # pinned
    kubectl apply -k deploy/dev     # latest

Each build on main is tagged `sha-<short commit>`, each release `v<version>`.
A release bumps every place that names the tag together: `base/20-controller.yaml`, `base/30-agent.yaml` and `newTag` in `base/kustomization.yaml`.
A workspace test fails when those places disagree.
Changing the tag changes the pod spec, so `kubectl apply -k` rolls both.
Keep `Always` with a tag that can move; `IfNotPresent` only with a `sha-` tag or a digest.
