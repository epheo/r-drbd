//! LV names of Volumes and Snapshots, and the checks LVM makes on them.

/// Joins volume and snapshot in a snapshot LV name. Kubernetes names never
/// hold it, so the split is unambiguous, and unlike `_` it starts none of
/// the suffixes LVM reserves (`_tdata`, `_rimage`, ...).
pub const SNAPSHOT_SEP: char = '+';

/// Cap on a Volume or Snapshot name: LVM keeps an LV name to 127 bytes and
/// a snapshot LV joins two names and the separator, so (127 - 1) / 2.
/// Not sufficient: the binding limit is the device-mapper name, which adds
/// the VG and doubles every `-` ([`check_new_lv`]); callers check it
/// against the real VG.
pub const NAME_MAX: usize = 63;

/// device-mapper names are 127 bytes; LVM builds them as `vg-lv` with
/// every `-` inside vg and lv doubled, and refuses an LV whose name overflows.
const DM_NAME_MAX: usize = 127;

const RESERVED_PREFIXES: [&str; 2] = ["snapshot", "pvmove"];
/// From lvm(8), "VALID NAMES".
const RESERVED_PARTS: [&str; 14] = [
    "_cdata", "_cmeta", "_corig", "_iorig", "_mimage", "_mlog", "_pmspare", "_rimage", "_rmeta",
    "_tdata", "_tmeta", "_vdata", "_vorigin", "_wcorig",
];

pub fn volume_lv(volume: &str) -> String {
    volume.to_owned()
}

pub fn snapshot_lv(volume: &str, snapshot: &str) -> String {
    format!("{volume}{SNAPSHOT_SEP}{snapshot}")
}

/// The LVM charset (lvm(8)): letters, digits, `+ _ . -`, no leading `-`,
/// at most 127 bytes, never `.` or `..`.
pub fn valid_lvm_name(name: &str) -> bool {
    let b = name.as_bytes();
    let ok = |c: &u8| c.is_ascii_alphanumeric() || matches!(c, b'+' | b'_' | b'.' | b'-');
    !b.is_empty()
        && b.len() <= 127
        && b[0] != b'-'
        && b.iter().all(ok)
        && name != "."
        && name != ".."
}

/// Bytes `name` takes in a device-mapper name: LVM doubles every `-`.
pub fn dm_len(name: &str) -> usize {
    name.len() + name.matches('-').count()
}

/// The LVM-reserved start of `lv`, if it has one.
pub fn reserved_prefix(lv: &str) -> Option<&'static str> {
    RESERVED_PREFIXES
        .iter()
        .copied()
        .find(|p| lv.starts_with(p))
}

/// Substrings LVM reserves; each starts with `_`.
pub fn reserved_parts() -> &'static [&'static str] {
    &RESERVED_PARTS
}

/// What lvcreate would refuse about a new LV `lv` in `vg`, in our words,
/// before any command runs.
pub fn check_new_lv(vg: &str, lv: &str) -> Result<(), String> {
    if !valid_lvm_name(vg) || !valid_lvm_name(lv) {
        return Err(format!("{vg}/{lv} is not a valid LVM name"));
    }
    if let Some(p) = reserved_prefix(lv) {
        return Err(format!("LVM reserves LV names starting with {p}: {lv}"));
    }
    if let Some(p) = RESERVED_PARTS.iter().find(|p| lv.contains(*p)) {
        return Err(format!("LVM reserves LV names containing {p}: {lv}"));
    }
    let dm = dm_len(vg) + 1 + dm_len(lv);
    if dm > DM_NAME_MAX {
        return Err(format!(
            "LV name {vg}/{lv} too long: its device-mapper name takes {dm} bytes, the limit is {DM_NAME_MAX}"
        ));
    }
    Ok(())
}
