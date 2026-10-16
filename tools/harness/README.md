# harness tools

- measure-rss.sh SSH_HOST: VmRSS of the r-drbd processes on a node.
- csi-sanity.sh: controller plugin against envtest with a fake agent.
- latency.sh CLASS [N]: provisioning latency of a StorageClass, as markdown tables.
- vm-boot-test.sh: a KubeVirt VM boots from a disk provisioned by r-drbd.
- ../dev/Containerfile.dev: scratch image around a locally built static binary.
