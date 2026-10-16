# drbd-config

Pure crate: renders DRBD resource files with the product's fixed option set, computes internal metadata sizes.
Out of scope: property precedence, option whitelists from xml-help, external metadata, connection paths, outside addresses, handlers.

Tests under tests/:
- render_sample: a 2 diskful + 1 tiebreaker resource renders byte for byte (modulo header) like fixtures/sample.res, from a ResourceFile built by hand with FixedOptions::new(2, None, false).
- render_local_free: a single replica renders no quorum line and no connections.
- render_order: sections and options in the documented order; options sorted by name; blank lines as specified.
- quoting: string options quoted, numeric and boolean bare.
- tiebreaker_versions: yes, no, and the commented line below kernel 9.3.1 or utils 9.34.0.
- body_differs: header changes are ignored; missing marker counts as different.
- sizing: a worked example (1 GiB) and the minimum 68 KiB rule; bitmap block size validation.

Dependencies allowed: thiserror only.
