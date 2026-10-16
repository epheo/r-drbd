//! Thin LVM on this node: pool probe and capacity, volume and snapshot LVs.
//! Every command goes through [`Exec`] with argv from the lvm crate.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use lvm::PvFilter;
use lvm::report::{LvRow, VgRow};

use crate::Exec;
use crate::exec::{CmdError, DEFAULT_TIMEOUT, failure, run_any};

/// lvremove busy retry: 3 extra attempts 100 ms apart.
const REMOVE_BUSY_RETRIES: usize = 3;
const REMOVE_BUSY_DELAY: Duration = Duration::from_millis(100);

/// Capacity of one thin pool, in bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolCapacity {
    pub total_bytes: u64,
    pub free_bytes: u64,
    pub chunk_bytes: u64,
    pub extent_kib: u64,
}

/// LVM driver: caches the PV filter per VG and retries a failed
/// command once with a fresh filter.
pub struct Lvm {
    exec: Arc<dyn Exec>,
    filters: Mutex<HashMap<String, PvFilter>>,
}

impl std::fmt::Debug for Lvm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Lvm").finish_non_exhaustive()
    }
}

impl Lvm {
    pub fn new(exec: Arc<dyn Exec>) -> Self {
        Self {
            exec,
            filters: Mutex::new(HashMap::new()),
        }
    }

    pub fn exec(&self) -> &dyn Exec {
        self.exec.as_ref()
    }

    fn cached_filter(&self, vg: &str) -> Option<PvFilter> {
        self.filters.lock().ok().and_then(|m| m.get(vg).cloned())
    }

    async fn filter(&self, vg: &str, fresh: bool) -> PvFilter {
        if !fresh && let Some(f) = self.cached_filter(vg) {
            return f;
        }
        let argv = lvm::argv::pvdisplay_for_vg(vg);
        let pvs: Vec<String> = match run_any(self.exec(), &argv, DEFAULT_TIMEOUT).await {
            Ok(out) if out.exit_code == Some(0) => out
                .stdout
                .lines()
                .map(str::trim)
                .filter(|l| !l.is_empty())
                .map(str::to_owned)
                .collect(),
            Ok(out) => {
                tracing::debug!(vg, error = %failure(&argv, &out), "pvdisplay failed, no PV filter");
                Vec::new()
            }
            Err(e) => {
                tracing::debug!(vg, error = %e, "pvdisplay failed, no PV filter");
                Vec::new()
            }
        };
        let f = PvFilter::from_pvs(&pvs);
        if let Ok(mut m) = self.filters.lock() {
            m.insert(vg.to_owned(), f.clone());
        }
        f
    }

    /// Run with the cached filter; on failure recompute it and run once more.
    async fn run_lvm<F, A>(&self, vg: &str, build: F, accept: A) -> Result<crate::Output, CmdError>
    where
        F: Fn(&PvFilter) -> Vec<String>,
        A: Fn(&crate::Output) -> bool,
    {
        let mut last = None;
        for fresh in [false, true] {
            let argv = build(&self.filter(vg, fresh).await);
            match run_any(self.exec(), &argv, DEFAULT_TIMEOUT).await {
                Ok(out) if accept(&out) => return Ok(out),
                Ok(out) => last = Some(failure(&argv, &out)),
                Err(e) => last = Some(e),
            }
        }
        Err(last.unwrap_or_else(|| CmdError::new("lvm", "no attempt made")))
    }

    async fn run_ok<F>(&self, vg: &str, build: F) -> Result<(), CmdError>
    where
        F: Fn(&PvFilter) -> Vec<String>,
    {
        self.run_lvm(vg, build, |o| o.exit_code == Some(0))
            .await
            .map(|_| ())
    }

    /// Rows of `vg`. lvs exit codes are ignored while it prints rows of the
    /// VG; no rows and a non-zero exit is an error with stderr.
    pub async fn lvs(&self, vg: &str) -> Result<Vec<LvRow>, CmdError> {
        let vgs = [vg.to_owned()];
        let parse = |o: &crate::Output| -> Vec<LvRow> {
            lvm::report::lvs(&o.stdout)
                .unwrap_or_default()
                .into_iter()
                .filter(|r| r.vg_name == vg)
                .collect()
        };
        let out = self
            .run_lvm(
                vg,
                |f| lvm::argv::lvs(f, &vgs),
                |o| o.exit_code == Some(0) || !parse(o).is_empty(),
            )
            .await?;
        lvm::report::lvs(&out.stdout)
            .map(|rows| rows.into_iter().filter(|r| r.vg_name == vg).collect())
            .map_err(|e| CmdError::new("lvs", e.to_string()))
    }

    /// Thin `vgs` rows of `vg`, same exit rule as [`Self::lvs`].
    pub async fn vgs(&self, vg: &str) -> Result<Vec<VgRow>, CmdError> {
        let vgs = [vg.to_owned()];
        let parse = |o: &crate::Output| -> Vec<VgRow> {
            lvm::report::vgs(&o.stdout)
                .unwrap_or_default()
                .into_iter()
                .filter(|r| r.vg_name == vg)
                .collect()
        };
        let out = self
            .run_lvm(
                vg,
                |f| lvm::argv::vgs(f, &vgs),
                |o| o.exit_code == Some(0) || !parse(o).is_empty(),
            )
            .await?;
        lvm::report::vgs(&out.stdout)
            .map(|rows| rows.into_iter().filter(|r| r.vg_name == vg).collect())
            .map_err(|e| CmdError::new("vgs", e.to_string()))
    }

    /// Thin pool probe and capacity.
    pub async fn probe_pool(&self, vg: &str, thin: &str) -> Result<PoolCapacity, String> {
        let lvs = self.lvs(vg).await.map_err(|e| e.to_string())?;
        let vgs = self.vgs(vg).await.map_err(|e| e.to_string())?;
        pool_capacity(&lvs, &vgs, thin)
    }

    /// Thin LV, then activate.
    pub async fn create(&self, vg: &str, thin: &str, lv: &str, kib: u64) -> Result<(), CmdError> {
        self.create_tagged(vg, thin, lv, kib, None).await
    }

    pub async fn create_tagged(
        &self,
        vg: &str,
        thin: &str,
        lv: &str,
        kib: u64,
        tag: Option<&str>,
    ) -> Result<(), CmdError> {
        self.run_ok(vg, |f| {
            lvm::argv::lvcreate_thin_tagged(f, vg, thin, lv, kib, tag)
        })
        .await?;
        self.activate(vg, lv).await
    }

    /// Writable thin snapshot of a snapshot LV, then activate.
    /// No wipe: the data is the point.
    pub async fn restore(&self, vg: &str, source: &str, lv: &str) -> Result<(), CmdError> {
        self.restore_tagged(vg, source, lv, None).await
    }

    pub async fn restore_tagged(
        &self,
        vg: &str,
        source: &str,
        lv: &str,
        tag: Option<&str>,
    ) -> Result<(), CmdError> {
        self.run_ok(vg, |f| {
            lvm::argv::restore_thin_tagged(f, vg, source, lv, tag)
        })
        .await?;
        self.activate(vg, lv).await
    }

    pub async fn resize(&self, vg: &str, lv: &str, kib: u64) -> Result<(), CmdError> {
        self.run_ok(vg, |f| lvm::argv::lvresize(f, vg, lv, kib))
            .await
    }

    pub async fn thinpool_zero(&self, vg: &str, thin: &str) -> Result<(), CmdError> {
        self.run_ok(vg, |f| lvm::argv::thinpool_zero(f, vg, thin))
            .await
    }

    pub async fn activate(&self, vg: &str, lv: &str) -> Result<(), CmdError> {
        self.run_ok(vg, |f| lvm::argv::activate(f, vg, lv)).await
    }

    pub async fn deactivate(&self, vg: &str, lv: &str) -> Result<(), CmdError> {
        self.run_ok(vg, |f| lvm::argv::deactivate(f, vg, lv)).await
    }

    /// Read-only thin snapshot.
    pub async fn snapshot(&self, vg: &str, origin: &str, snap: &str) -> Result<(), CmdError> {
        self.run_ok(vg, |f| lvm::argv::snapshot_thin(f, vg, origin, snap))
            .await
    }

    /// `lvremove -f` with the busy retry inside the filter retry.
    pub async fn remove(&self, vg: &str, lv: &str) -> Result<(), CmdError> {
        let mut attempt = 0;
        loop {
            match self.run_ok(vg, |f| lvm::argv::lvremove(f, vg, lv)).await {
                Err(e) if attempt < REMOVE_BUSY_RETRIES && e.message.contains("busy") => {
                    attempt += 1;
                    tokio::time::sleep(REMOVE_BUSY_DELAY).await;
                }
                r => return r,
            }
        }
    }
}

/// Legacy metadata tag (before per-node tags): proves nothing by itself.
pub const MD_TAG: &str = "r-drbd.md";
/// Set right after every lvcreate of a DRBD backing LV: made by us, no
/// metadata proven yet, never attached.
pub const NEW_TAG: &str = "r-drbd.new";

/// Proves the DRBD metadata on an LV was made by this product for this
/// node id; day-0 ids age out of drbdmeta's history, the tag does not.
/// Per node id, so an LV kept by a departed node proves nothing.
pub fn md_tag(node_id: u8) -> String {
    format!("{MD_TAG}.{node_id}")
}

/// The legacy tag and every per-node metadata tag.
pub fn is_md_tag(tag: &str) -> bool {
    tag == MD_TAG || tag.strip_prefix(MD_TAG).is_some_and(|r| r.starts_with('.'))
}

impl Lvm {
    /// Tags of every LV of `vg`.
    pub async fn tags(&self, vg: &str) -> Result<HashMap<String, Vec<String>>, CmdError> {
        let out = self
            .run_lvm(vg, |f| tags_argv(f, vg), |o| o.exit_code == Some(0))
            .await?;
        Ok(parse_tags(&out.stdout))
    }

    /// One lvchange, so a swap never leaves the LV with neither tag.
    pub async fn change_tags(
        &self,
        vg: &str,
        lv: &str,
        add: &[&str],
        del: &[&str],
    ) -> Result<(), CmdError> {
        self.run_ok(vg, |f| lvchange_tags(f, add, del, vg, lv))
            .await
    }
}

/// The lvm crate keeps its argv head private; same shape here.
pub fn lvm_head(program: &str, filter: &PvFilter) -> Vec<String> {
    let mut v = vec![program.to_owned()];
    if let Some(f) = &filter.0 {
        v.push("--config".to_owned());
        v.push(f.clone());
    }
    v
}

pub fn lvchange_tags(
    filter: &PvFilter,
    add: &[&str],
    del: &[&str],
    vg: &str,
    lv: &str,
) -> Vec<String> {
    let mut v = lvm_head("lvchange", filter);
    for (op, tags) in [("--addtag", add), ("--deltag", del)] {
        for t in tags {
            v.push(op.to_owned());
            v.push((*t).to_owned());
        }
    }
    v.push(format!("{vg}/{lv}"));
    v
}

pub fn tags_argv(filter: &PvFilter, vg: &str) -> Vec<String> {
    let mut v = lvm_head("lvs", filter);
    v.extend(
        [
            "--noheadings",
            "-o",
            "lv_name,lv_tags",
            "--separator",
            ";",
            vg,
        ]
        .map(str::to_owned),
    );
    v
}

/// `name;tag1,tag2` lines.
pub fn parse_tags(stdout: &str) -> HashMap<String, Vec<String>> {
    stdout
        .lines()
        .filter_map(|l| l.trim().split_once(';'))
        .map(|(name, tags)| {
            let tags = tags
                .split(',')
                .map(str::trim)
                .filter(|t| !t.is_empty())
                .map(str::to_owned)
                .collect();
            (name.trim().to_owned(), tags)
        })
        .collect()
}

/// Pool capacity from one lvs and one thin vgs read of the VG.
pub fn pool_capacity(lvs: &[LvRow], vgs: &[VgRow], thin: &str) -> Result<PoolCapacity, String> {
    let pool = lvs
        .iter()
        .find(|r| r.lv_name == thin)
        .ok_or_else(|| "thin pool not found".to_owned())?;
    let vg_row = vgs
        .iter()
        .find(|r| r.lv_name == thin)
        .ok_or_else(|| "thin pool not found".to_owned())?;
    Ok(PoolCapacity {
        total_bytes: pool.lv_size * 1024,
        free_bytes: lvm::report::thin_free(vg_row) * 1024,
        chunk_bytes: pool.chunk_size * 1024,
        extent_kib: vg_row.vg_extent_size,
    })
}

/// sizeBytes in KiB, rounded up to the VG extent size.
pub fn desired_kib(size_bytes: u64, extent_kib: u64) -> u64 {
    let kib = size_bytes.div_ceil(1024);
    if extent_kib == 0 {
        return kib;
    }
    kib.div_ceil(extent_kib) * extent_kib
}

/// The sixth lv_attr character is `o` while the device is open.
pub fn is_open(row: &LvRow) -> bool {
    row.lv_attr.chars().nth(5) == Some('o')
}

/// Thin LVs carved from `thin` (the pool itself excluded).
pub fn thin_lvs<'a>(rows: &'a [LvRow], thin: &'a str) -> impl Iterator<Item = &'a LvRow> {
    rows.iter()
        .filter(move |r| r.pool_lv.as_deref() == Some(thin) && r.lv_name != thin)
}

#[cfg(test)]
pub(crate) mod fixtures {
    //! Scripted lvm output for the r-drbd-test VG.

    use crate::exec::scripted::{ScriptedExec, code, ok};

    pub(crate) const VG: &str = "vg1";
    pub(crate) const THIN: &str = "thin";
    pub(crate) const PV: &str = "/dev/loop0";

    pub(crate) fn filter() -> lvm::PvFilter {
        lvm::PvFilter::from_pvs(&[PV.to_owned()])
    }

    pub(crate) fn lv_row(name: &str, kib: u64, attr: &str, origin: &str) -> String {
        format!("  {name};/dev/{VG}/{name};{kib}.00;{VG};{THIN};0.00;{attr};;64.00;1;{origin}\n")
    }

    pub(crate) fn pool_row() -> String {
        format!("  {THIN};;52428800.00;{VG};;10.00;twi-a-tz--;10.44;64.00;1;\n")
    }

    pub(crate) fn vgs_out() -> String {
        format!("  {VG};4096.00;62910464.00;10366976.00;{THIN};52428800.00;10.00\n")
    }

    pub(crate) fn expect_pvdisplay(x: &ScriptedExec) {
        x.expect(lvm::argv::pvdisplay_for_vg(VG), ok(&format!("  {PV}\n")));
    }

    pub(crate) fn expect_lvs(x: &ScriptedExec, rows: &[String]) {
        let mut out = pool_row();
        for r in rows {
            out.push_str(r);
        }
        x.expect(lvm::argv::lvs(&filter(), &[VG.to_owned()]), ok(&out));
    }

    pub(crate) fn expect_vgs(x: &ScriptedExec) {
        x.expect(lvm::argv::vgs(&filter(), &[VG.to_owned()]), ok(&vgs_out()));
    }

    pub(crate) fn fail(stderr: &str) -> Result<crate::Output, String> {
        code(5, "", stderr)
    }
}

#[cfg(test)]
mod tests {
    use super::fixtures::*;
    use super::*;
    use crate::exec::scripted::{ScriptedExec, code, ok};

    #[test]
    fn rounding() {
        assert_eq!(desired_kib(1 << 30, 4096), 1_048_576);
        assert_eq!(desired_kib((1 << 30) + 1, 4096), 1_048_576 + 4096);
        assert_eq!(desired_kib(1000, 4096), 4096);
        assert_eq!(desired_kib(1000, 0), 1);
    }

    #[tokio::test]
    async fn capacity_from_reports() {
        let x = Arc::new(ScriptedExec::new());
        expect_pvdisplay(&x);
        expect_lvs(&x, &[]);
        expect_vgs(&x);
        let lvm = Lvm::new(x.clone());
        let cap = lvm.probe_pool(VG, THIN).await.unwrap();
        assert_eq!(
            cap,
            PoolCapacity {
                total_bytes: 52_428_800 * 1024,
                free_bytes: 47_185_920 * 1024,
                chunk_bytes: 64 * 1024,
                extent_kib: 4096,
            }
        );
        x.assert_done();
    }

    #[tokio::test]
    async fn missing_vg_retries_with_fresh_filter_then_fails() {
        let x = Arc::new(ScriptedExec::new());
        let none = PvFilter(None);
        let lvs = lvm::argv::lvs(&none, &[VG.to_owned()]);
        x.expect(lvm::argv::pvdisplay_for_vg(VG), code(5, "", ""))
            .expect(lvs.clone(), fail("  Volume group \"vg1\" not found"))
            .expect(lvm::argv::pvdisplay_for_vg(VG), code(5, "", ""))
            .expect(lvs, fail("  Volume group \"vg1\" not found"));
        let lvm = Lvm::new(x.clone());
        let e = lvm.probe_pool(VG, THIN).await.unwrap_err();
        assert_eq!(e, "lvs: exit 5: Volume group \"vg1\" not found");
        x.assert_done();
    }

    #[tokio::test]
    async fn missing_thin_pool() {
        let x = Arc::new(ScriptedExec::new());
        expect_pvdisplay(&x);
        x.expect(
            lvm::argv::lvs(&filter(), &[VG.to_owned()]),
            ok(&lv_row("other", 4096, "-wi-a-----", "")),
        );
        x.expect(
            lvm::argv::vgs(&filter(), &[VG.to_owned()]),
            ok(&format!("  {VG};4096.00;100.00;100.00;other;4096.00;\n")),
        );
        let e = Lvm::new(x.clone()).probe_pool(VG, THIN).await.unwrap_err();
        assert_eq!(e, "thin pool not found");
        x.assert_done();
    }

    #[tokio::test]
    async fn remove_retries_busy() {
        let x = Arc::new(ScriptedExec::new());
        expect_pvdisplay(&x);
        let rm = lvm::argv::lvremove(&filter(), VG, "lv");
        x.expect(rm.clone(), fail("Logical volume vg1/lv in use: busy"));
        expect_pvdisplay(&x);
        x.expect(rm.clone(), fail("Logical volume vg1/lv in use: busy"));
        x.expect(rm, ok(""));
        Lvm::new(x.clone()).remove(VG, "lv").await.unwrap();
        x.assert_done();
    }

    #[test]
    fn tags_parse() {
        let out = "  a;r-drbd.md\n  b;\n  c;x,r-drbd.md.2\n  d;r-drbd.mdx\n";
        let t = parse_tags(out);
        assert_eq!(t["a"], ["r-drbd.md"]);
        assert_eq!(t["b"].len(), 0);
        assert_eq!(t["c"], ["x", "r-drbd.md.2"]);
        assert_eq!(md_tag(2), "r-drbd.md.2");
        assert!(is_md_tag("r-drbd.md") && is_md_tag("r-drbd.md.2"));
        assert!(!is_md_tag("r-drbd.mdx") && !is_md_tag(NEW_TAG));
        assert_eq!(
            lvchange_tags(&PvFilter(None), &["a"], &["b", "c"], "vg", "lv"),
            [
                "lvchange", "--addtag", "a", "--deltag", "b", "--deltag", "c", "vg/lv"
            ]
        );
    }

    #[test]
    fn open_flag() {
        let rows = lvm::report::lvs(&lv_row("a", 4096, "Vwi-aotz--", "")).unwrap();
        assert!(is_open(&rows[0]));
        let rows = lvm::report::lvs(&lv_row("a", 4096, "Vwi-a-tz--", "")).unwrap();
        assert!(!is_open(&rows[0]));
    }
}
