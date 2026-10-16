# drbd-config

Pure crate: renders DRBD resource files with the product's fixed option set, and sizes the backing device for DRBD internal metadata.

## Resource file

- First line: `# r-drbd <version> sha256:<hex>`, the hash of every line after it.
  `tail -n +2 <file> | sha256sum` gives the same hex.
  The agent compares hashes to decide whether a file changed, so a new agent version alone rewrites nothing.
- Sections: options, net, disk, one `on` per host by node id, one `connection` per peer by node id.
- Only this host and the peers it connects to appear.
- Peer volumes: a diskful peer names `/dev/r-drbd/peer-disk-never-opened` (drbdadm turns the bitmap off for a peer without a disk; the path is never opened), a diskless one `disk none`.
- Optional settings the installed tools lack (tiebreaker, rs-discard-granularity, on-suspended-primary-outdated) are left out and reported.

## Sizing

- `gross_bytes(net, slots)`: backing size whose usable size is at least `net`, closed form, at most one 4 KiB block above the smallest.
- `metadata_bytes` and `usable_bytes`: superblock 4 KiB, activity log 32 KiB, bitmap of one bit per 4 KiB per peer slot, rounded up to 4 KiB.

## Dependencies

- `ring`: sha256 for the header; already the workspace's crypto backend.
- `thiserror`: error types.

## Tests

- `tests/render.rs`: a full file byte for byte, hash behaviour, unsupported settings, values drbdadm would misread.
- `tests/size.rs`: sizes measured with create-md, closed form against a search.
