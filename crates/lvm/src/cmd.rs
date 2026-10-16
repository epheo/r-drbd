//! Argv of every command the agent runs on LVM, options before positional
//! names as in the man page synopses. Names are checked by the caller.

/// Device rules every lvm command runs with:
/// - `scan_lvs = 0`: an LV is never read for PV signatures, so a guest's LVM
///   inside a volume, or behind DRBD on its backing LV, stays invisible.
/// - the filter rejects DRBD devices, whose data is a backing LV's data.
///
/// For our commands this `filter` replaces any host lvm.conf `filter`; the
/// host's `global_filter` and devices file still apply on top.
/// With a devices file LVM ignores `filter` and says so on stderr; DRBD
/// devices are not in that file, so they stay hidden either way.
/// The host's own scans (udev autoactivation) are not covered: see
/// [`MIN_VERSION`] and the agent's host check.
#[macro_export]
macro_rules! device_rules {
    () => {
        r#"scan_lvs = 0 filter = [ "r|^/dev/drbd|" ]"#
    };
}

/// Oldest LVM taken: `scan_lvs` must be a known setting. 2.03.03 is the
/// first release whose notes mention it (its default moved to 0 there).
pub const MIN_VERSION: (u32, u32, u32) = (2, 3, 3);

/// `lvm version`.
pub fn version() -> Vec<String> {
    vec!["lvm".to_owned(), "version".to_owned()]
}

/// The LVM version from [`version`] output, e.g. `LVM version: 2.03.41(2)`.
pub fn parse_version(stdout: &str) -> Option<(u32, u32, u32)> {
    let v = stdout
        .lines()
        .find_map(|l| l.trim().strip_prefix("LVM version:"))?
        .trim();
    let v = v
        .split(|c: char| !(c.is_ascii_digit() || c == '.'))
        .next()?;
    let mut parts = v.split('.').map(str::parse::<u32>);
    match (parts.next(), parts.next(), parts.next()) {
        (Some(Ok(a)), Some(Ok(b)), Some(Ok(c))) => Some((a, b, c)),
        _ => None,
    }
}

/// The host's effective value of one setting, defaults included, without
/// our `--config`: what udev-run LVM commands on the host use.
pub fn host_setting(key: &str) -> Vec<String> {
    ["lvmconfig", "--typeconfig", "full", key]
        .map(str::to_owned)
        .to_vec()
}

/// `name=value` from [`host_setting`] output, value unquoted for strings.
pub fn parse_host_setting(stdout: &str) -> Option<String> {
    let (_, v) = stdout.trim().split_once('=')?;
    Some(v.trim().to_owned())
}

/// The `--config` value of every lvm command.
pub const CONFIG: &str = concat!("devices { ", device_rules!(), " }");

/// The line LVM prints on hosts with a devices file because of our filter.
pub const FILTER_IGNORED_NOTICE: &str =
    "Please remove the lvm.conf filter, it is ignored with the devices file.";

/// Fields of the LV report: what the reconciler and the pool probe read.
pub const LV_FIELDS: &str = "lv_name,lv_path,lv_size,segtype,pool_lv,lv_active_locally,lv_device_open,zero,data_percent,chunk_size";
pub const TAG_FIELDS: &str = "lv_name,lv_tags";
pub const VG_FIELDS: &str = "vg_name,vg_extent_size";

fn lvm<S: AsRef<str>>(program: &str, options: &[S], target: String) -> Vec<String> {
    let mut v = vec![program.to_owned(), "--config".to_owned(), CONFIG.to_owned()];
    v.extend(options.iter().map(|o| o.as_ref().to_owned()));
    v.push(target);
    v
}

/// JSON, exact bytes without a unit suffix, so serde parses it whole.
/// `--binary` gives 0 or 1 for two-valued fields instead of words.
fn report(program: &str, fields: &str, vg: &str) -> Vec<String> {
    lvm(
        program,
        &[
            "--reportformat",
            "json",
            "--units",
            "b",
            "--nosuffix",
            "--binary",
            "-o",
            fields,
        ],
        vg.to_owned(),
    )
}

pub fn lvs(vg: &str) -> Vec<String> {
    report("lvs", LV_FIELDS, vg)
}

/// Tags are read only when a decision needs them, apart from [`lvs`].
pub fn lv_tags(vg: &str) -> Vec<String> {
    report("lvs", TAG_FIELDS, vg)
}

pub fn vgs(vg: &str) -> Vec<String> {
    report("vgs", VG_FIELDS, vg)
}

fn size(bytes: u64) -> String {
    format!("{bytes}b")
}

fn with_tags(mut options: Vec<String>, tags: &[&str]) -> Vec<String> {
    for t in tags {
        options.push("--addtag".to_owned());
        options.push((*t).to_owned());
    }
    options
}

/// A thin LV of `bytes` in `pool`, active once lvcreate returns.
/// `--thinpool` with `--virtualsize` implies `--type thin`.
pub fn create_thin(vg: &str, pool: &str, lv: &str, bytes: u64, tags: &[&str]) -> Vec<String> {
    let opts = with_tags(
        vec![
            "--thinpool".to_owned(),
            pool.to_owned(),
            "--virtualsize".to_owned(),
            size(bytes),
            "--name".to_owned(),
            lv.to_owned(),
        ],
        tags,
    );
    lvm("lvcreate", &opts, vg.to_owned())
}

/// A thin snapshot: no size, so it shares the origin's pool.
/// - activation skip: never activated, so no device node exists for udev,
///   blkid or a guest's LVM to open; set here, not left to the host default.
/// - read-only: the data a restore copies never changes.
pub fn snapshot(vg: &str, origin: &str, snap: &str) -> Vec<String> {
    lvm(
        "lvcreate",
        &[
            "--snapshot",
            "--setactivationskip",
            "y",
            "--permission",
            "r",
            "--name",
            snap,
        ],
        format!("{vg}/{origin}"),
    )
}

/// A writable thin snapshot of a snapshot LV, active once lvcreate returns.
/// Activation skip off: the copy is a volume and activates like one.
/// A new LV is writable by default, also from a read-only source.
pub fn restore(vg: &str, source: &str, lv: &str, tags: &[&str]) -> Vec<String> {
    let opts = with_tags(
        vec![
            "--snapshot".to_owned(),
            "--setactivationskip".to_owned(),
            "n".to_owned(),
            "--name".to_owned(),
            lv.to_owned(),
        ],
        tags,
    );
    lvm("lvcreate", &opts, format!("{vg}/{source}"))
}

/// lvextend, not lvresize: it refuses a smaller size, so a stale size can
/// never cut data off. Extending leaves any file system alone.
pub fn extend(vg: &str, lv: &str, bytes: u64) -> Vec<String> {
    lvm("lvextend", &["--size", &size(bytes)], format!("{vg}/{lv}"))
}

/// `--yes`: an active LV asks for confirmation, and with stdin on
/// /dev/null the answer would be no.
pub fn remove(vg: &str, lv: &str) -> Vec<String> {
    lvm("lvremove", &["--yes"], format!("{vg}/{lv}"))
}

pub fn activate(vg: &str, lv: &str) -> Vec<String> {
    lvm("lvchange", &["--activate", "y"], format!("{vg}/{lv}"))
}

pub fn deactivate(vg: &str, lv: &str) -> Vec<String> {
    lvm("lvchange", &["--activate", "n"], format!("{vg}/{lv}"))
}

/// One lvchange for both lists, so a swap never leaves neither tag.
pub fn change_tags(vg: &str, lv: &str, add: &[&str], del: &[&str]) -> Vec<String> {
    let mut opts = Vec::new();
    for (flag, tags) in [("--addtag", add), ("--deltag", del)] {
        for t in tags {
            opts.push(flag);
            opts.push(*t);
        }
    }
    lvm("lvchange", &opts, format!("{vg}/{lv}"))
}

/// Without pool zeroing a new thin LV can read blocks of a removed one.
pub fn enable_zeroing(vg: &str, pool: &str) -> Vec<String> {
    lvm("lvchange", &["--zero", "y"], format!("{vg}/{pool}"))
}

pub fn device_size(dev: &str) -> Vec<String> {
    vec![
        "blockdev".to_owned(),
        "--getsize64".to_owned(),
        dev.to_owned(),
    ]
}

/// The stdout of [`device_size`].
pub fn parse_device_size(stdout: &str) -> Option<u64> {
    stdout.trim().parse().ok()
}
