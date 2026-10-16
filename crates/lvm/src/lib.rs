//! Thin LVM command construction and report parsing for the agent.
//!
//! Pure crate: builds argument vectors and parses stdout; it never runs a
//! process. Thin only.

#![forbid(unsafe_code)]

/// The `--config` filter argument built from a PV list.
/// `None` means no `--config` is passed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PvFilter(pub Option<String>);

impl PvFilter {
    /// Empty PV list gives `PvFilter(None)`; otherwise the single-argument
    /// `devices { filter=['a|<pv>|',...,'r|.*|'] }` string.
    pub fn from_pvs(pvs: &[String]) -> Self {
        if pvs.is_empty() {
            return Self(None);
        }
        let mut s = String::from("devices { filter=[");
        for pv in pvs {
            s.push_str("'a|");
            s.push_str(pv);
            s.push_str("|',");
        }
        s.push_str("'r|.*|'] }");
        Self(Some(s))
    }
}

/// Size in KiB as LVM reports and accepts it.
pub type Kib = u64;

/// Argument vectors, program name first, in a fixed order the tests pin.
pub mod argv {
    use super::{Kib, PvFilter};

    fn head(program: &str, filter: &PvFilter) -> Vec<String> {
        let mut v = vec![program.to_owned()];
        if let Some(f) = &filter.0 {
            v.push("--config".to_owned());
            v.push(f.clone());
        }
        v
    }

    fn push(v: &mut Vec<String>, args: &[&str]) {
        v.extend(args.iter().map(|&a| a.to_owned()));
    }

    pub fn pvdisplay_for_vg(vg: &str) -> Vec<String> {
        let mut v = Vec::new();
        push(
            &mut v,
            &[
                "pvdisplay",
                "--config",
                r#"devices { ignore_suspended_devices=1 filter=["r|^/dev/drbd.*|"] }"#,
                "--columns",
                "-o",
                "pv_name",
                "-S",
            ],
        );
        v.push(format!("vg_name={vg}"));
        push(&mut v, &["--noheadings", "--nosuffix"]);
        v
    }
    pub fn lvs(filter: &PvFilter, vgs: &[String]) -> Vec<String> {
        let mut v = head("lvs", filter);
        push(
            &mut v,
            &[
                "-o",
                "lv_name,lv_path,lv_size,vg_name,pool_lv,data_percent,lv_attr,metadata_percent,chunk_size,stripes,origin",
                "--separator",
                ";",
                "--noheadings",
                "--units",
                "k",
                "--nosuffix",
            ],
        );
        v.extend(vgs.iter().cloned());
        v
    }
    /// The thin variant (7 fields).
    pub fn vgs(filter: &PvFilter, vgs: &[String]) -> Vec<String> {
        let mut v = head("vgs", filter);
        push(
            &mut v,
            &[
                "-o",
                "vg_name,vg_extent_size,vg_size,vg_free,lv_name,lv_size,data_percent",
                "--separator",
                ";",
                "--units",
                "k",
                "--noheadings",
                "--nosuffix",
            ],
        );
        v.extend(vgs.iter().cloned());
        v
    }
    pub fn lvcreate_thin(
        filter: &PvFilter,
        vg: &str,
        thinpool: &str,
        lv: &str,
        size: Kib,
    ) -> Vec<String> {
        lvcreate_thin_tagged(filter, vg, thinpool, lv, size, None)
    }
    /// The tag is set by lvcreate itself, so the LV never exists without it.
    pub fn lvcreate_thin_tagged(
        filter: &PvFilter,
        vg: &str,
        thinpool: &str,
        lv: &str,
        size: Kib,
        tag: Option<&str>,
    ) -> Vec<String> {
        let mut v = head("lvcreate", filter);
        push_tag(&mut v, tag);
        v.push("--virtualsize".to_owned());
        v.push(format!("{size}k"));
        push(&mut v, &[vg, "--thinpool", thinpool, "--name", lv]);
        v
    }
    fn push_tag(v: &mut Vec<String>, tag: Option<&str>) {
        if let Some(t) = tag {
            push(v, &["--addtag", t]);
        }
    }
    pub fn lvresize(filter: &PvFilter, vg: &str, lv: &str, size: Kib) -> Vec<String> {
        let mut v = head("lvresize", filter);
        v.push("--size".to_owned());
        v.push(format!("{size}k"));
        v.push(format!("{vg}/{lv}"));
        v.push("-f".to_owned());
        v
    }
    pub fn lvremove(filter: &PvFilter, vg: &str, lv: &str) -> Vec<String> {
        let mut v = head("lvremove", filter);
        v.push("-f".to_owned());
        v.push(format!("{vg}/{lv}"));
        v
    }
    pub fn activate(filter: &PvFilter, vg: &str, lv: &str) -> Vec<String> {
        let mut v = head("lvchange", filter);
        push(&mut v, &["-ay", "-K", "-y"]);
        v.push(format!("{vg}/{lv}"));
        v
    }
    pub fn deactivate(filter: &PvFilter, vg: &str, lv: &str) -> Vec<String> {
        let mut v = head("lvchange", filter);
        v.push("-an".to_owned());
        v.push(format!("{vg}/{lv}"));
        v
    }
    pub fn thinpool_zero(filter: &PvFilter, vg: &str, thinpool: &str) -> Vec<String> {
        let mut v = head("lvchange", filter);
        push(&mut v, &["--zero", "y"]);
        v.push(format!("{vg}/{thinpool}"));
        v
    }
    /// Read-only thin snapshot, VG-qualified name.
    pub fn snapshot_thin(filter: &PvFilter, vg: &str, origin: &str, snap: &str) -> Vec<String> {
        let mut v = head("lvcreate", filter);
        push(
            &mut v,
            &[
                "--snapshot",
                "--setactivationskip",
                "y",
                "--ignoreactivationskip",
                "--activate",
                "y",
                "--name",
            ],
        );
        v.push(format!("{vg}/{snap}"));
        v.push(format!("{vg}/{origin}"));
        v.push("-pr".to_owned());
        v
    }
    /// Writable thin snapshot used for restore, followed by activate.
    pub fn restore_thin(filter: &PvFilter, vg: &str, source: &str, target: &str) -> Vec<String> {
        restore_thin_tagged(filter, vg, source, target, None)
    }
    /// Tags of the source are copied too; `tag` is added on top of them.
    pub fn restore_thin_tagged(
        filter: &PvFilter,
        vg: &str,
        source: &str,
        target: &str,
        tag: Option<&str>,
    ) -> Vec<String> {
        let mut v = head("lvcreate", filter);
        push_tag(&mut v, tag);
        push(&mut v, &["--snapshot", "--name", target]);
        v.push(format!("{vg}/{source}"));
        v
    }
    pub fn blockdev_getsize64(dev: &str) -> Vec<String> {
        let mut v = Vec::new();
        push(&mut v, &["blockdev", "--getsize64", dev]);
        v
    }
    pub fn blockdev_setrw(dev: &str) -> Vec<String> {
        let mut v = Vec::new();
        push(&mut v, &["blockdev", "--setrw", dev]);
        v
    }
    pub fn wipefs(dev: &str) -> Vec<String> {
        let mut v = Vec::new();
        push(&mut v, &["wipefs", "-a", "-f", dev]);
        v
    }
}

/// Parsed report rows.
pub mod report {
    use super::{Kib, ParseError};

    #[derive(Debug, Clone, PartialEq)]
    pub struct LvRow {
        pub lv_name: String,
        pub lv_path: String,
        pub lv_size: Kib,
        pub vg_name: String,
        pub pool_lv: Option<String>,
        pub data_percent: f32,
        pub lv_attr: String,
        pub metadata_percent: String,
        pub chunk_size: Kib,
        pub stripes: u32,
        pub origin: Option<String>,
    }

    impl LvRow {
        /// `lv_attr` contains `a`.
        pub fn is_active(&self) -> bool {
            self.lv_attr.contains('a')
        }
        /// `lv_attr` contains `z` (thin pool zeroing).
        pub fn is_zeroing(&self) -> bool {
            self.lv_attr.contains('z')
        }
    }

    #[derive(Debug, Clone, PartialEq)]
    pub struct VgRow {
        pub vg_name: String,
        pub vg_extent_size: Kib,
        pub vg_size: Kib,
        pub vg_free: Kib,
        pub lv_name: String,
        pub lv_size: Kib,
        pub data_percent: f32,
    }

    fn err(what: impl Into<String>, value: &str) -> ParseError {
        ParseError::Field {
            what: what.into(),
            value: value.to_owned(),
        }
    }

    /// "Integer part": LVM prints `1048576.00`, or `1048576,00` under a comma locale.
    fn int_part<T: std::str::FromStr>(raw: &str) -> Option<T> {
        let t = raw.trim();
        let int = t.split(['.', ',']).next().unwrap_or("");
        int.parse().ok()
    }

    /// Empty reads as 100: unknown fill counts as full.
    fn percent(raw: &str) -> Option<f32> {
        let t = raw.trim();
        if t.is_empty() {
            return Some(100.0);
        }
        t.replace(',', ".").parse().ok()
    }

    fn opt(raw: &str) -> Option<String> {
        let t = raw.trim();
        (!t.is_empty()).then(|| t.to_owned())
    }

    /// Lines with the wrong field count are ignored silently.
    pub fn lvs(stdout: &str) -> Result<Vec<LvRow>, ParseError> {
        let mut rows = Vec::new();
        for line in stdout.split('\n') {
            let f: Vec<&str> = line.trim().split(';').collect();
            if f.len() != 11 {
                continue;
            }
            rows.push(LvRow {
                lv_name: f[0].to_owned(),
                lv_path: f[1].to_owned(),
                lv_size: int_part(f[2])
                    .ok_or_else(|| err("Unable to parse logical volume size", f[2]))?,
                vg_name: f[3].to_owned(),
                pool_lv: opt(f[4]),
                data_percent: percent(f[5])
                    .ok_or_else(|| err("Unable to parse data_percent of thin lv", f[5]))?,
                lv_attr: f[6].trim().to_owned(),
                metadata_percent: f[7].to_owned(),
                chunk_size: int_part(f[8])
                    .ok_or_else(|| err("Unable to parse logical chunk size", f[8]))?,
                stripes: int_part(f[9]).ok_or_else(|| err("Unable to parse stripes", f[9]))?,
                origin: opt(f[10]),
            });
        }
        Ok(rows)
    }
    pub fn vgs(stdout: &str) -> Result<Vec<VgRow>, ParseError> {
        let mut rows = Vec::new();
        for line in stdout.split('\n') {
            let f: Vec<&str> = line.trim().split(';').collect();
            // An LV-less VG prints empty LV fields; skip it so other VGs still parse.
            if f.len() != 7 || f[4].trim().is_empty() {
                continue;
            }
            let vg = f[0];
            let what =
                |column: &str| format!("Unable to parse '{column}' of vgs for volume group '{vg}'");
            let kib = |i: usize, column: &str| {
                int_part::<Kib>(f[i]).ok_or_else(|| err(what(column), f[i]))
            };
            rows.push(VgRow {
                vg_name: vg.to_owned(),
                vg_extent_size: kib(1, "vg_extent_size")?,
                vg_size: kib(2, "vg_size")?,
                vg_free: kib(3, "vg_free")?,
                lv_name: f[4].to_owned(),
                lv_size: kib(5, "lv_size")?,
                data_percent: percent(f[6]).ok_or_else(|| err(what("data_percent"), f[6]))?,
            });
        }
        Ok(rows)
    }
    /// `blockdev --getsize64` bytes to KiB, truncating.
    pub fn blockdev_size(stdout: &str) -> Result<Kib, ParseError> {
        let t = stdout.trim();
        t.parse::<u64>()
            .map(|bytes| bytes / 1024)
            .map_err(|_| err("Unable to parse blockdev size", t))
    }
    /// Thin free space: `lv_size * (1 - data_percent / 100)`, truncated.
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "pool sizes stay far below 2^52 KiB and the clamped factor is in 0..=1"
    )]
    pub fn thin_free(pool: &VgRow) -> Kib {
        // NaN counts as full: never report free space we cannot vouch for.
        let pct = if pool.data_percent.is_nan() {
            100.0
        } else {
            f64::from(pool.data_percent).clamp(0.0, 100.0)
        };
        let free = pool.lv_size as f64 * (1.0 - pct / 100.0);
        free as Kib
    }
}

/// LV naming: one volume per resource, so no volume number suffix.
pub mod names {
    /// The volume's LV is the volume name itself.
    pub fn volume_lv(volume: &str) -> String {
        volume.to_owned()
    }
    /// `<volume>_<snapshot>`.
    pub fn snapshot_lv(volume: &str, snapshot: &str) -> String {
        format!("{volume}_{snapshot}")
    }
    /// Volume or snapshot name rule: 2 to 48 bytes, at least one
    /// ASCII letter, first byte letter or `_`, others letter, digit, `_` or `-`.
    /// Applies to each name component, not to the composed snapshot LV.
    pub fn valid(name: &str) -> bool {
        let b = name.as_bytes();
        let Some(&first) = b.first() else {
            return false;
        };
        (2..=48).contains(&b.len())
            && (first.is_ascii_alphabetic() || first == b'_')
            && b.iter().any(u8::is_ascii_alphabetic)
            && b.iter()
                .all(|&c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-')
    }

    /// VG and thin pool names (Pool.spec): ASCII letters, digits, `+ _ . -`,
    /// 1 to 127 bytes, never a leading `-` (lvm would read an option), not
    /// `.` or `..` (path components under /dev).
    pub fn valid_lvm_name(name: &str) -> bool {
        let b = name.as_bytes();
        (1..=127).contains(&b.len())
            && b[0] != b'-'
            && name != "."
            && name != ".."
            && b.iter()
                .all(|&c| c.is_ascii_alphanumeric() || matches!(c, b'+' | b'_' | b'.' | b'-'))
    }
}

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum ParseError {
    #[error("{what}: '{value}'")]
    Field { what: String, value: String },
}
