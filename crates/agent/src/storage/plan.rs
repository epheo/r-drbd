//! What a Pool built from devices needs next, from one look at the host.
//! Pure: the caller observes, asks for one step, runs it and observes
//! again, so a crash between any two steps resumes from what is there.

use lvm::report::{PoolLv, VgFull};
use model::{DeviceState, DeviceStatus};

use crate::reconcile::{
    REASON_BUILDING, REASON_DEVICE_BUSY, REASON_DEVICE_MISSING, REASON_DEVICE_NOT_EMPTY,
    REASON_NOT_A_DISK, REASON_UNAVAILABLE, REASON_VG_NOT_OWNED,
};

/// Bytes of thin pool metadata per data chunk: twice the measured need of
/// a fully mapped pool, for B-tree slack and snapshots diverging.
pub const META_PER_CHUNK: u64 = 64;
/// Per-device metadata for about 10000 thin LVs.
pub const META_BASE: u64 = 64 << 20;
/// LVM's largest pool metadata (15.81 GiB, rounded down to MiB). Newer LVM
/// takes a little more; this bound holds for every supported release.
pub const META_MAX: u64 = 16_189 << 20;

/// `data / chunk * 64 B + 64 MiB`, rounded up to the extent.
pub fn metadata_bytes(data: u64, chunk: u64, extent: u64) -> u64 {
    let raw = (data / chunk.max(1))
        .saturating_mul(META_PER_CHUNK)
        .saturating_add(META_BASE);
    round_up(raw, extent)
}

fn round_up(bytes: u64, extent: u64) -> u64 {
    if extent == 0 {
        bytes
    } else {
        bytes.div_ceil(extent).saturating_mul(extent)
    }
}

/// Largest metadata size LVM takes, in whole extents.
fn meta_max(extent: u64) -> u64 {
    META_MAX
        .checked_div(extent)
        .map_or(META_MAX, |n| n * extent)
}

/// Chunk and metadata of a new pool: the chunk doubles until the rule fits
/// under LVM's metadata maximum, up to LVM's largest chunk.
pub fn chunk_and_metadata(data: u64, chunk: u64, extent: u64) -> (u64, u64) {
    let mut c = chunk;
    while metadata_bytes(data, c, extent) > meta_max(extent) && c < model::pool::MAX_CHUNK_BYTES {
        c *= 2;
    }
    (c, metadata_bytes(data, c, extent).min(meta_max(extent)))
}

/// A Kubernetes quantity in bytes, whole numbers only.
pub fn quantity_bytes(q: &str) -> Option<u64> {
    let q = q.trim();
    let split = q
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(q.len());
    let (num, suffix) = q.split_at(split);
    let mult: u128 = match suffix {
        "" => 1,
        "k" => 1_000,
        "M" => 1_000_000,
        "G" => 1_000_000_000,
        "T" => 1_000_000_000_000,
        "Ki" => 1 << 10,
        "Mi" => 1 << 20,
        "Gi" => 1 << 30,
        "Ti" => 1 << 40,
        _ => return None,
    };
    let (int, frac) = num.split_once('.').unwrap_or((num, ""));
    if int.is_empty() && frac.is_empty() {
        return None;
    }
    let digits: u128 = format!("{int}{frac}").parse().ok()?;
    let scale = 10u128.checked_pow(u32::try_from(frac.len()).ok()?)?;
    let value = digits.checked_mul(mult)?;
    (value % scale == 0)
        .then(|| u64::try_from(value / scale).ok())
        .flatten()
}

/// The chunk to build with: `chunkSize`, or the default when absent.
pub fn chunk_bytes(spec: Option<&str>) -> Result<u64, String> {
    let Some(q) = spec else {
        return Ok(model::pool::DEFAULT_CHUNK_BYTES);
    };
    quantity_bytes(q)
        .filter(|b| {
            b.is_power_of_two()
                && (model::pool::MIN_CHUNK_BYTES..=model::pool::MAX_CHUNK_BYTES).contains(b)
        })
        .ok_or_else(|| format!("chunkSize {q} is not a power of two from 64Ki to 1Gi"))
}

/// What the Pool asks for.
#[derive(Debug, Clone, Copy)]
pub struct Target<'a> {
    pub tag: &'a str,
    pub vg: &'a str,
    pub thin: &'a str,
    pub chunk_bytes: u64,
}

/// One look at the host.
#[derive(Debug, Clone, Copy)]
pub struct Seen<'a> {
    pub vg: Option<&'a VgFull>,
    /// The LV named like the thin pool, whatever its type.
    pub thin: Option<&'a PoolLv>,
    pub devices: &'a [DeviceStatus],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    Vgcreate(Vec<String>),
    CreateThinPool {
        chunk_bytes: u64,
        meta_bytes: u64,
    },
    Vgextend(Vec<String>),
    GrowMetadata(u64),
    /// `None`: all free space; `Some`: up to this data size.
    GrowData(Option<u64>),
    Ready,
}

impl Step {
    /// The step as the Building message names it.
    pub fn name(&self) -> String {
        match self {
            Self::Vgcreate(d) => format!("vgcreate on {}", d.join(", ")),
            Self::CreateThinPool {
                chunk_bytes,
                meta_bytes,
            } => format!("thin pool create, chunk {chunk_bytes} B, metadata {meta_bytes} B"),
            Self::Vgextend(d) => format!("vgextend with {}", d.join(", ")),
            Self::GrowMetadata(b) => format!("metadata grow to {b} B"),
            Self::GrowData(None) => "data grow".into(),
            Self::GrowData(Some(b)) => format!("data grow to {b} B"),
            Self::Ready => "ready".into(),
        }
    }
}

/// Available False with this reason and message; nothing runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Blocked {
    pub reason: &'static str,
    pub message: String,
}

fn blocked(reason: &'static str, message: impl Into<String>) -> Blocked {
    Blocked {
        reason,
        message: message.into(),
    }
}

fn reason_of(state: DeviceState) -> &'static str {
    match state {
        DeviceState::Missing => REASON_DEVICE_MISSING,
        DeviceState::NotADisk => REASON_NOT_A_DISK,
        DeviceState::Busy => REASON_DEVICE_BUSY,
        DeviceState::NotEmpty => REASON_DEVICE_NOT_EMPTY,
        DeviceState::Member | DeviceState::Blank => REASON_BUILDING,
    }
}

/// `path is State: message` for each row.
pub fn describe(rows: &[&DeviceStatus]) -> String {
    rows.iter()
        .map(|r| match &r.message {
            Some(m) => format!("{} is {:?}: {m}", r.path, r.state),
            None => format!("{} is {:?}", r.path, r.state),
        })
        .collect::<Vec<_>>()
        .join("; ")
}

/// The next step, or why there is none.
pub fn plan(t: &Target<'_>, s: &Seen<'_>) -> Result<Step, Blocked> {
    let Some(vg) = s.vg else {
        // One bad path stops the whole Pool: no half-built VG from a typo.
        let mut bad: Vec<&DeviceStatus> = s
            .devices
            .iter()
            .filter(|d| d.state != DeviceState::Blank)
            .collect();
        if bad.is_empty() {
            return Ok(Step::Vgcreate(
                s.devices.iter().map(|d| d.path.clone()).collect(),
            ));
        }
        bad.sort_by_key(|d| d.state);
        return Err(blocked(reason_of(bad[0].state), describe(&bad)));
    };
    if !vg.tags.iter().any(|g| g == t.tag) {
        return Err(blocked(
            REASON_VG_NOT_OWNED,
            format!(
                "volume group {} exists without tag {}: never adopted, never touched; a VG made by hand belongs in a Pool without devices",
                t.vg, t.tag
            ),
        ));
    }
    if vg.missing_pvs > 0 {
        let gone: Vec<&DeviceStatus> = s
            .devices
            .iter()
            .filter(|d| d.state == DeviceState::Missing)
            .collect();
        return Err(blocked(
            REASON_DEVICE_MISSING,
            format!(
                "volume group {} misses {} PV(s); {}",
                t.vg,
                vg.missing_pvs,
                if gone.is_empty() {
                    "no listed path is missing".into()
                } else {
                    describe(&gone)
                }
            ),
        ));
    }
    let Some(pool) = s.thin else {
        let (chunk_bytes, meta_bytes) =
            chunk_and_metadata(vg.free_bytes, t.chunk_bytes, vg.extent_bytes);
        return Ok(Step::CreateThinPool {
            chunk_bytes,
            meta_bytes,
        });
    };
    if !pool.thin_pool {
        return Err(blocked(
            REASON_UNAVAILABLE,
            format!("LV {} in {} is not a thin pool", t.thin, t.vg),
        ));
    }
    let blank: Vec<String> = s
        .devices
        .iter()
        .filter(|d| d.state == DeviceState::Blank)
        .map(|d| d.path.clone())
        .collect();
    if !blank.is_empty() {
        return Ok(Step::Vgextend(blank));
    }
    if pool.chunk_bytes == 0 {
        return Err(blocked(
            REASON_UNAVAILABLE,
            format!("thin pool {} reports chunk size 0: growth skipped", t.thin),
        ));
    }
    let ext = vg.extent_bytes.max(1);
    if vg.free_bytes < ext {
        return Ok(Step::Ready);
    }
    // Metadata first: a pool whose data outgrows its metadata fails whole.
    let target = pool.size_bytes + vg.free_bytes;
    let rule = metadata_bytes(target, pool.chunk_bytes, vg.extent_bytes);
    let want = rule.min(meta_max(vg.extent_bytes));
    if want > pool.metadata_bytes {
        return Ok(Step::GrowMetadata(want));
    }
    if rule <= pool.metadata_bytes {
        return Ok(Step::GrowData(None));
    }
    // The chunk is fixed: past LVM's metadata maximum, data grows only as
    // far as the metadata covers by the rule.
    let covered = covered_data(pool.metadata_bytes, pool.chunk_bytes, ext).min(target);
    if covered >= pool.size_bytes + ext {
        return Ok(Step::GrowData(Some(covered)));
    }
    Ok(Step::Ready)
}

/// Data bytes `meta` covers by the rule at `chunk`, in whole extents.
pub fn covered_data(meta: u64, chunk: u64, extent: u64) -> u64 {
    let data = meta.saturating_sub(META_BASE) / META_PER_CHUNK * chunk;
    data / extent.max(1) * extent.max(1)
}

/// Why VG space stays out of a pool that has nothing left to do.
pub fn unused(vg: &VgFull, pool: &PoolLv) -> Option<String> {
    (vg.free_bytes >= vg.extent_bytes.max(1)).then(|| {
        format!(
            "{} bytes of the volume group stay out of the thin pool: its metadata is at LVM's maximum, which covers {} bytes of data at chunk {} B",
            vg.free_bytes,
            covered_data(pool.metadata_bytes, pool.chunk_bytes, vg.extent_bytes),
            pool.chunk_bytes
        )
    })
}

/// Rows that keep the Pool as it is but cannot join it.
pub fn stuck(devices: &[DeviceStatus]) -> Vec<&DeviceStatus> {
    devices
        .iter()
        .filter(|d| !matches!(d.state, DeviceState::Member | DeviceState::Blank))
        .collect()
}

/// The next removal step of a Pool deleted with reclaimPolicy Delete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Release {
    Deactivate,
    RemovePool,
    RemoveVg,
    RemovePvs(Vec<String>),
    Done,
}

/// `lvs`: names of the visible LVs of the VG, the thin pool's included,
/// with whether the thin pool is active. `orphans`: listed paths that are
/// PVs without a VG and that this Pool's status last showed as Member.
pub fn release(
    t: &Target<'_>,
    vg: Option<&VgFull>,
    lvs: &[(String, bool)],
    orphans: &[String],
) -> Result<Release, String> {
    let Some(vg) = vg else {
        if orphans.is_empty() {
            return Ok(Release::Done);
        }
        return Ok(Release::RemovePvs(orphans.to_vec()));
    };
    if !vg.tags.iter().any(|g| g == t.tag) {
        return Err(format!(
            "volume group {} has no tag {}: nothing removed",
            t.vg, t.tag
        ));
    }
    if vg.missing_pvs > 0 {
        return Err(format!(
            "volume group {} misses {} PV(s): nothing removed",
            t.vg, vg.missing_pvs
        ));
    }
    let others: Vec<&str> = lvs
        .iter()
        .map(|(n, _)| n.as_str())
        .filter(|n| *n != t.thin)
        .collect();
    if !others.is_empty() {
        return Err(format!(
            "volume group {} holds LV {}: nothing removed",
            t.vg,
            others.join(", ")
        ));
    }
    Ok(match lvs.iter().find(|(n, _)| n == t.thin) {
        Some((_, true)) => Release::Deactivate,
        Some((_, false)) => Release::RemovePool,
        None => Release::RemoveVg,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: u64 = 1 << 20;
    const GIB: u64 = 1 << 30;
    const TIB: u64 = 1 << 40;
    const EXT: u64 = 4 * MIB;
    const K64: u64 = 64 << 10;
    const T: Target<'static> = Target {
        tag: "r-drbd.pool.p1",
        vg: "vg1",
        thin: "thin",
        chunk_bytes: K64,
    };

    #[test]
    fn metadata_rule() {
        // An 18 GiB pool at 64 KiB chunks: 18 MiB plus the 64 MiB base,
        // then whole 4 MiB extents.
        assert_eq!(metadata_bytes(18 * GIB, K64, 0), 82 * MIB);
        assert_eq!(metadata_bytes(18 * GIB, K64, EXT), 84 * MIB);
        // Rounded up to the extent.
        assert_eq!(metadata_bytes(GIB, K64, EXT), 68 * MIB);
        assert_eq!(metadata_bytes(GIB + 1, K64, EXT), 68 * MIB);
        assert_eq!(metadata_bytes(GIB, K64, 0), 65 * MIB);
        assert_eq!(metadata_bytes(100 * GIB, K64, EXT), 164 * MIB);
        // Under the cap the chunk stays.
        assert_eq!(chunk_and_metadata(18 * GIB, K64, EXT), (K64, 84 * MIB));
        assert_eq!(chunk_and_metadata(15 * TIB, K64, EXT), (K64, 15_424 * MIB));
        // Above about 15.7 TiB at 64 KiB the cap doubles the chunk.
        assert_eq!(
            chunk_and_metadata(16 * TIB, K64, EXT),
            (128 << 10, 8_256 * MIB)
        );
        assert_eq!(
            chunk_and_metadata(64 * TIB, K64, EXT),
            (512 << 10, 8_256 * MIB)
        );
        // 1 GiB is LVM's largest chunk: past it the cap holds.
        let (c, m) = chunk_and_metadata(u64::MAX / 2, K64, EXT);
        assert_eq!((c, m), (GIB, meta_max(EXT)));
        assert!(m <= META_MAX);
    }

    #[test]
    fn quantities() {
        for (q, b) in [
            ("64Ki", 65_536),
            ("1Gi", GIB),
            ("131072", 131_072),
            ("0.5Mi", 524_288),
            ("1M", 1_000_000),
        ] {
            assert_eq!(quantity_bytes(q), Some(b), "{q}");
        }
        for q in ["", "x", "1.5", "1Ei", "1e6", "."] {
            assert_eq!(quantity_bytes(q), None, "{q}");
        }
        assert_eq!(chunk_bytes(None), Ok(K64));
        assert_eq!(chunk_bytes(Some("256Ki")), Ok(256 << 10));
        assert!(chunk_bytes(Some("1M")).is_err());
        assert!(chunk_bytes(Some("32Ki")).is_err());
        assert!(chunk_bytes(Some("2Gi")).is_err());
    }

    fn row(path: &str, state: DeviceState) -> DeviceStatus {
        DeviceStatus {
            path: path.into(),
            state,
            size_bytes: None,
            model: None,
            serial: None,
            message: None,
        }
    }

    fn vg(size: u64, free: u64, tags: &[&str]) -> VgFull {
        VgFull {
            name: "vg1".into(),
            extent_bytes: EXT,
            size_bytes: size,
            free_bytes: free,
            tags: tags.iter().map(|&t| t.to_owned()).collect(),
            missing_pvs: 0,
        }
    }

    fn pool(data: u64, meta: u64) -> PoolLv {
        PoolLv {
            name: "thin".into(),
            thin_pool: true,
            size_bytes: data,
            metadata_bytes: meta,
            chunk_bytes: K64,
        }
    }

    const A: &str = "/dev/disk/by-id/a";
    const B: &str = "/dev/disk/by-id/b";
    const C: &str = "/dev/disk/by-id/c";
    const TAG: &[&str] = &["r-drbd.pool.p1"];

    fn plan_of(
        v: Option<&VgFull>,
        p: Option<&PoolLv>,
        d: &[DeviceStatus],
    ) -> Result<Step, Blocked> {
        plan(
            &T,
            &Seen {
                vg: v,
                thin: p,
                devices: d,
            },
        )
    }

    #[test]
    fn build_rows() {
        use DeviceState::*;
        // VG absent, all blank: one vgcreate with every device.
        let d = [row(A, Blank), row(B, Blank)];
        assert_eq!(
            plan_of(None, None, &d),
            Ok(Step::Vgcreate(vec![A.into(), B.into()]))
        );
        // One bad path blocks it all; the worst state names the reason.
        for (bad, reason) in [
            (Missing, REASON_DEVICE_MISSING),
            (NotADisk, REASON_NOT_A_DISK),
            (Busy, REASON_DEVICE_BUSY),
            (NotEmpty, REASON_DEVICE_NOT_EMPTY),
        ] {
            let mut r = row(B, bad);
            r.message = Some("why".into());
            let e = plan_of(None, None, &[row(A, Blank), r]).unwrap_err();
            assert_eq!(e.reason, reason);
            assert_eq!(e.message, format!("{B} is {bad:?}: why"));
        }
        let e = plan_of(
            None,
            None,
            &[row(A, NotEmpty), row(B, Missing), row(C, Busy)],
        )
        .unwrap_err();
        assert_eq!(e.reason, REASON_DEVICE_MISSING);
        assert_eq!(
            e.message,
            format!("{B} is Missing; {C} is Busy; {A} is NotEmpty")
        );

        // A VG without our tag: never touched, whatever the devices.
        let foreign = vg(8 * GIB, 8 * GIB, &[]);
        let e = plan_of(Some(&foreign), None, &[row(A, Blank)]).unwrap_err();
        assert_eq!(e.reason, REASON_VG_NOT_OWNED);
        let other = vg(8 * GIB, 8 * GIB, &["r-drbd.pool.p2"]);
        assert_eq!(
            plan_of(Some(&other), None, &[]).unwrap_err().reason,
            REASON_VG_NOT_OWNED
        );

        // Restart after vgcreate: the tagged VG gets its thin pool.
        let fresh = vg(4_290_772_992, 4_290_772_992, TAG);
        assert_eq!(
            plan_of(Some(&fresh), None, &[row(A, Member)]),
            Ok(Step::CreateThinPool {
                chunk_bytes: K64,
                meta_bytes: 68 * MIB
            })
        );
        // A chunkSize from the spec is kept.
        let big = Target {
            chunk_bytes: 1 << 20,
            ..T
        };
        assert_eq!(
            plan(
                &big,
                &Seen {
                    vg: Some(&fresh),
                    thin: None,
                    devices: &[]
                }
            ),
            Ok(Step::CreateThinPool {
                chunk_bytes: 1 << 20,
                meta_bytes: 68 * MIB
            })
        );
        // An LV of that name that is not a thin pool.
        let mut linear = pool(GIB, 0);
        linear.thin_pool = false;
        assert_eq!(
            plan_of(Some(&fresh), Some(&linear), &[])
                .unwrap_err()
                .reason,
            REASON_UNAVAILABLE
        );
        // Built: nothing free, every device a member.
        let full = vg(4_290_772_992, 0, TAG);
        let built = pool(4_148_166_656, 71_303_168);
        assert_eq!(
            plan_of(Some(&full), Some(&built), &[row(A, Member)]),
            Ok(Step::Ready)
        );
    }

    #[test]
    fn growth_rows() {
        use DeviceState::*;
        let full = vg(4 * GIB, 0, TAG);
        let built = pool(4_148_166_656, 68 * MIB);
        // A new blank path: vgextend first, with every blank path.
        assert_eq!(
            plan_of(
                Some(&full),
                Some(&built),
                &[row(A, Member), row(B, Blank), row(C, Blank)]
            ),
            Ok(Step::Vgextend(vec![B.into(), C.into()]))
        );
        // Restart after vgextend: metadata to the rule for the new size.
        let grown = vg(8 * GIB, 4 * GIB, TAG);
        let d = [row(A, Member), row(B, Member)];
        let want = metadata_bytes(4_148_166_656 + 4 * GIB, K64, EXT);
        assert_eq!(want, 72 * MIB);
        assert_eq!(
            plan_of(Some(&grown), Some(&built), &d),
            Ok(Step::GrowMetadata(want))
        );
        // Restart after the metadata grew: data takes the rest.
        let after_meta = vg(8 * GIB, 4 * GIB - 8 * MIB, TAG);
        let meta_done = pool(4_148_166_656, 72 * MIB);
        assert_eq!(
            plan_of(Some(&after_meta), Some(&meta_done), &d),
            Ok(Step::GrowData(None))
        );
        // Less than an extent free is nothing to grow.
        let crumbs = vg(8 * GIB, EXT - 1, TAG);
        assert_eq!(
            plan_of(Some(&crumbs), Some(&meta_done), &d),
            Ok(Step::Ready)
        );
        // A path that cannot join keeps the Pool as it is.
        let d = [row(A, Member), row(B, NotEmpty), row(C, Missing)];
        assert_eq!(plan_of(Some(&full), Some(&built), &d), Ok(Step::Ready));
        assert_eq!(stuck(&d).len(), 2);
        // Metadata never grows past LVM's maximum, and data only as far as
        // that metadata covers: 16124 MiB / 64 B * 64 KiB is about 15.7 TiB.
        let huge = vg(40 * TIB, 20 * TIB, TAG);
        let below = pool(10 * TIB, 10_304 * MIB);
        assert_eq!(
            plan_of(Some(&huge), Some(&below), &[]),
            Ok(Step::GrowMetadata(meta_max(EXT)))
        );
        let capped = pool(10 * TIB, meta_max(EXT));
        let covered = covered_data(meta_max(EXT), K64, EXT);
        assert_eq!(covered, 16_124 * TIB / 1024);
        assert_eq!(
            plan_of(Some(&huge), Some(&capped), &[]),
            Ok(Step::GrowData(Some(covered)))
        );
        // Grown that far: the rest stays out, said once the pool is ready.
        let rest = vg(40 * TIB, 40 * TIB - covered - 2 * meta_max(EXT), TAG);
        let full = pool(covered, meta_max(EXT));
        assert_eq!(plan_of(Some(&rest), Some(&full), &[]), Ok(Step::Ready));
        let note = unused(&rest, &full).unwrap();
        assert!(note.contains("metadata is at LVM's maximum"), "{note}");
        assert_eq!(unused(&vg(8 * GIB, 0, TAG), &full), None);
        // A pool report with chunk 0 blocks growth instead of dividing by it.
        let mut odd = pool(GIB, 68 * MIB);
        odd.chunk_bytes = 0;
        let e = plan_of(Some(&grown), Some(&odd), &[]).unwrap_err();
        assert_eq!(
            e.message,
            "thin pool thin reports chunk size 0: growth skipped"
        );
    }

    #[test]
    fn missing_member_blocks() {
        use DeviceState::*;
        let mut partial = vg(8 * GIB, 0, TAG);
        partial.missing_pvs = 1;
        let mut gone = row(B, Missing);
        gone.message = Some("No such file".into());
        let e = plan_of(
            Some(&partial),
            Some(&pool(8 * GIB, 72 * MIB)),
            &[row(A, Member), gone, row(C, Blank)],
        )
        .unwrap_err();
        assert_eq!(e.reason, REASON_DEVICE_MISSING);
        assert_eq!(
            e.message,
            format!("volume group vg1 misses 1 PV(s); {B} is Missing: No such file")
        );
    }

    #[test]
    fn release_rows() {
        let ours = vg(8 * GIB, 0, TAG);
        let lv = |n: &str, active: bool| (n.to_owned(), active);
        assert_eq!(
            release(&T, Some(&ours), &[lv("thin", true)], &[]),
            Ok(Release::Deactivate)
        );
        assert_eq!(
            release(&T, Some(&ours), &[lv("thin", false)], &[]),
            Ok(Release::RemovePool)
        );
        assert_eq!(release(&T, Some(&ours), &[], &[]), Ok(Release::RemoveVg));
        // Restart after vgremove: only the recorded members are cleared.
        assert_eq!(
            release(&T, None, &[], &[A.into()]),
            Ok(Release::RemovePvs(vec![A.into()]))
        );
        assert_eq!(release(&T, None, &[], &[]), Ok(Release::Done));
        // Anything else in the VG stops it, named.
        assert_eq!(
            release(
                &T,
                Some(&ours),
                &[lv("thin", true), lv("pvc-1", true), lv("mine", false)],
                &[]
            ),
            Err("volume group vg1 holds LV pvc-1, mine: nothing removed".into())
        );
        let foreign = vg(8 * GIB, 0, &[]);
        assert_eq!(
            release(&T, Some(&foreign), &[], &[]),
            Err("volume group vg1 has no tag r-drbd.pool.p1: nothing removed".into())
        );
        let mut partial = vg(8 * GIB, 0, TAG);
        partial.missing_pvs = 2;
        assert!(release(&T, Some(&partial), &[], &[]).is_err());
    }
}
