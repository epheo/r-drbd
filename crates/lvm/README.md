# lvm

Pure crate: argv for every LVM command the agent runs, JSON report parsers, LV names.
Thin pools only.

Modules:
- `cmd`: argv builders; one static `--config` on every lvm command (no LV scanning, DRBD devices rejected). The commands of a Pool built from devices (vgcreate, vgextend, thin pool create and growth, removal) never pass `--yes` or `--force`.
- `report`: serde parsers for the `lvs`, `vgs` and `pvs` JSON reports in bytes; errors name the report, the field and the raw value.
- `names`: volume and snapshot LV names (`<volume>+<snapshot>`), the LVM charset, and the checks lvcreate would fail on (reserved names, device-mapper name length).

Tests under tests/:
- cmd: exact argv of every builder, options before positional names.
- report: rows from real JSON output, field errors, percent forms, thin pool free bytes, errors from the JSON log.
- names: snapshot LV length bound, LVM charset, reserved names, device-mapper length as measured on a host.

Dependencies:
- serde, serde_json: the JSON reports.
- thiserror: report errors.
