# deploy

Static manifests; copy them and set your own StorageClass names and tiers.
Order: the CRDs from `r-drbd crds`, then `kubectl apply -k deploy/base`.
Sidecar versions: csi-provisioner v6.3.0, attacher v4.13.0, resizer v2.2.1, snapshotter v8.6.0, node-driver-registrar v2.18.0.
The image holds static binaries on scratch: r-drbd and drbd-utils. With R_DRBD_HOST_NS every other agent command re-execs the binary into the host IPC and mount namespaces and runs the host's lvm, xfsprogs and mount (hostPID and privileged are required for that). MicroShift and OpenShift also need the privileged SCC binding in 00-namespace-rbac.yaml.
`openshift/` adds what OpenShift needs beyond that: the DRBD module through Kernel Module Management, graceful node shutdown and an LVM devices file on every node.

## Image version

The base pulls `ghcr.io/epheo/r-drbd:latest` with `imagePullPolicy: Always`, so every restart takes the newest build.
For a fixed version, pin one tag in an overlay so the controller and every agent run the same build:

    apiVersion: kustomize.config.k8s.io/v1beta1
    kind: Kustomization
    resources:
      - ../base
    images:
      - name: ghcr.io/epheo/r-drbd
        newTag: sha-1a2b3c4
    patches:
      - target: {kind: Deployment, name: r-drbd-controller}
        patch: |
          - {op: replace, path: /spec/template/spec/containers/0/imagePullPolicy, value: IfNotPresent}
      - target: {kind: DaemonSet, name: r-drbd-agent}
        patch: |
          - {op: replace, path: /spec/template/spec/containers/0/imagePullPolicy, value: IfNotPresent}

Each build on main is tagged `sha-<short commit>`, each release `v<version>`.
Keep `Always` with a `v` tag or any other tag that can move; switch to `IfNotPresent` only for a `sha-` tag or a `digest:`.
Changing the tag changes the pod spec, so `kubectl apply -k` rolls both.
