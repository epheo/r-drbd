# deploy

Static manifests; copy them and set your own StorageClass names and tiers.
Order: the CRDs from `r-drbd crds` of the same build, then `kubectl apply -k deploy/base` (or `deploy/dev`).
Sidecar versions: csi-provisioner v6.3.0, attacher v4.13.0, resizer v2.2.1, snapshotter v8.6.0, node-driver-registrar v2.18.0.
The image holds static binaries on scratch: r-drbd and drbd-utils. With R_DRBD_HOST_NS every other agent command re-execs the binary into the host IPC and mount namespaces and runs the host's lvm, xfsprogs and mount (hostPID and privileged are required for that). MicroShift and OpenShift also need the privileged SCC binding in 00-namespace-rbac.yaml.
`openshift/` adds what OpenShift needs beyond that: the DRBD module through Kernel Module Management, graceful node shutdown and an LVM devices file on every node.

## Image version

`base` pins one build: `ghcr.io/epheo/r-drbd:sha-<short commit>` with `imagePullPolicy: IfNotPresent` on the controller and the agent.
The tag is written in the two pod manifests, so a plain `kubectl apply -f` deploys the same build as `kubectl apply -k`; the `images:` entry in `base/kustomization.yaml` carries it for overlays.
Every agent then runs the same build, and a restarted pod starts from the node's copy even while the registry is unreachable.

`dev` follows `:latest` with `imagePullPolicy: Always`: each restart takes the newest main build.
Use it for development only: agents can end up on different builds, and an agent restarted during a registry outage stays down with its node's replicas.

    kubectl apply -k deploy/base    # pinned
    kubectl apply -k deploy/dev     # latest

Each build on main is tagged `sha-<short commit>`, each release `v<version>`.
A release bumps every place that names the tag together: `base/20-controller.yaml`, `base/30-agent.yaml`, `newTag` in `base/kustomization.yaml`, and the CRD commands in README.md, docs/install.md and docs/openshift.md.
A workspace test fails when those places disagree.
Changing the tag changes the pod spec, so `kubectl apply -k` rolls both.
Keep `Always` with a tag that can move; `IfNotPresent` only with a `sha-` tag or a digest.
