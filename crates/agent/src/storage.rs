//! Thin LVM on this node: pool probe and capacity, volume and snapshot LVs.
//! Every command goes through [`Exec`] with argv from the lvm crate.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use lvm::cmd;
use lvm::report::{self, Lv, PoolLv, Pv, Vg, VgFull};

pub mod plan;

use crate::exec::{CmdError, LVM_CHANGE_TIMEOUT, LVM_REPORT_TIMEOUT, run_any};
use crate::{Exec, Output};

/// Pauses before each look at an LV's open flag on remove. A udev probe or
/// a last close in flight holds an LV for milliseconds; anything still
/// open after 3 s is a real user, and the next pass tries again.
const REMOVE_OPEN_WAITS: [Duration; 4] = [
    Duration::ZERO,
    Duration::from_millis(250),
    Duration::from_millis(750),
    Duration::from_secs(2),
];

/// Capacity of one thin pool, in bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolCapacity {
    pub total_bytes: u64,
    pub free_bytes: u64,
    pub chunk_bytes: u64,
}

pub struct Lvm {
    exec: Arc<dyn Exec>,
}

impl std::fmt::Debug for Lvm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Lvm").finish_non_exhaustive()
    }
}

/// stderr of a failed lvm command, without the notice our own filter causes.
fn lvm_failure(argv: &[String], out: &Output, extra: Vec<String>) -> CmdError {
    let mut lines: Vec<String> = out
        .stderr
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && *l != cmd::FILTER_IGNORED_NOTICE)
        .map(str::to_owned)
        .collect();
    lines.extend(extra);
    let code = out
        .exit_code
        .map_or_else(|| "signal".to_owned(), |c| c.to_string());
    let program = argv.first().cloned().unwrap_or_default();
    CmdError::new(program, format!("exit {code}: {}", lines.join("; ")))
}

impl Lvm {
    pub fn new(exec: Arc<dyn Exec>) -> Self {
        Self { exec }
    }

    pub fn exec(&self) -> &dyn Exec {
        self.exec.as_ref()
    }

    /// stdout of a report that exited 0.
    async fn report(&self, argv: &[String]) -> Result<String, CmdError> {
        let out = run_any(self.exec(), argv, LVM_REPORT_TIMEOUT).await?;
        if out.exit_code == Some(0) {
            return Ok(out.stdout);
        }
        Err(lvm_failure(argv, &out, report::log_errors(&out.stdout)))
    }

    async fn change(&self, argv: &[String]) -> Result<(), CmdError> {
        let out = run_any(self.exec(), argv, LVM_CHANGE_TIMEOUT).await?;
        if out.exit_code == Some(0) {
            return Ok(());
        }
        Err(lvm_failure(argv, &out, Vec::new()))
    }

    /// LVs of `vg`.
    pub async fn lvs(&self, vg: &str) -> Result<Vec<Lv>, CmdError> {
        let out = self.report(&cmd::lvs(vg)).await?;
        report::parse_lvs(&out).map_err(|e| CmdError::new("lvs", e.to_string()))
    }

    /// Tags of every LV of `vg`.
    pub async fn tags(&self, vg: &str) -> Result<HashMap<String, Vec<String>>, CmdError> {
        let out = self.report(&cmd::lv_tags(vg)).await?;
        report::parse_tags(&out).map_err(|e| CmdError::new("lvs", e.to_string()))
    }

    pub async fn vg(&self, vg: &str) -> Result<Vg, CmdError> {
        let out = self.report(&cmd::vgs(vg)).await?;
        report::parse_vgs(&out)
            .map_err(|e| CmdError::new("vgs", e.to_string()))?
            .into_iter()
            .find(|r| r.name == vg)
            .ok_or_else(|| CmdError::new("vgs", format!("volume group {vg} not in the report")))
    }

    /// Every VG of the host.
    pub async fn vgs_all(&self) -> Result<Vec<VgFull>, CmdError> {
        let out = self.report(&cmd::vgs_all()).await?;
        report::parse_vgs_all(&out).map_err(|e| CmdError::new("vgs", e.to_string()))
    }

    /// Every PV the host's LVM sees.
    pub async fn pvs_all(&self) -> Result<Vec<Pv>, CmdError> {
        let out = self.report(&cmd::pvs_all()).await?;
        report::parse_pvs(&out).map_err(|e| CmdError::new("pvs", e.to_string()))
    }

    /// LVs of `vg` with thin pool sizes.
    pub async fn thin_pools(&self, vg: &str) -> Result<Vec<PoolLv>, CmdError> {
        let out = self.report(&cmd::thin_pools(vg)).await?;
        report::parse_thin_pools(&out).map_err(|e| CmdError::new("lvs", e.to_string()))
    }

    pub async fn vgcreate(&self, vg: &str, tag: &str, devices: &[String]) -> Result<(), CmdError> {
        self.change(&cmd::vgcreate(vg, tag, devices)).await
    }

    pub async fn vgextend(&self, vg: &str, devices: &[String]) -> Result<(), CmdError> {
        self.change(&cmd::vgextend(vg, devices)).await
    }

    pub async fn create_thin_pool(
        &self,
        vg: &str,
        thin: &str,
        chunk_bytes: u64,
        meta_bytes: u64,
    ) -> Result<(), CmdError> {
        self.change(&cmd::create_thin_pool(vg, thin, chunk_bytes, meta_bytes))
            .await
    }

    pub async fn extend_pool_metadata(
        &self,
        vg: &str,
        thin: &str,
        meta_bytes: u64,
    ) -> Result<(), CmdError> {
        self.change(&cmd::extend_pool_metadata(vg, thin, meta_bytes))
            .await
    }

    pub async fn extend_pool_data(&self, vg: &str, thin: &str) -> Result<(), CmdError> {
        self.change(&cmd::extend_pool_data(vg, thin)).await
    }

    pub async fn extend_pool_data_to(
        &self,
        vg: &str,
        thin: &str,
        bytes: u64,
    ) -> Result<(), CmdError> {
        self.change(&cmd::extend_pool_data_to(vg, thin, bytes))
            .await
    }

    pub async fn remove_inactive(&self, vg: &str, lv: &str) -> Result<(), CmdError> {
        self.change(&cmd::remove_inactive(vg, lv)).await
    }

    pub async fn vgremove(&self, vg: &str) -> Result<(), CmdError> {
        self.change(&cmd::vgremove(vg)).await
    }

    pub async fn pvremove(&self, devices: &[String]) -> Result<(), CmdError> {
        self.change(&cmd::pvremove(devices)).await
    }

    /// Thin pool probe and capacity.
    pub async fn probe_pool(&self, vg: &str, thin: &str) -> Result<PoolCapacity, String> {
        let lvs = self.lvs(vg).await.map_err(|e| e.to_string())?;
        pool_capacity(&lvs, thin)
    }

    /// A new LV's name, checked before lvcreate sees it.
    fn check(program: &str, vg: &str, lv: &str) -> Result<(), CmdError> {
        lvm::names::check_new_lv(vg, lv).map_err(|e| CmdError::new(program, e))
    }

    /// Thin LV, active on return.
    pub async fn create(
        &self,
        vg: &str,
        thin: &str,
        lv: &str,
        bytes: u64,
        tags: &[&str],
    ) -> Result<(), CmdError> {
        Self::check("lvcreate", vg, lv)?;
        self.change(&cmd::create_thin(vg, thin, lv, bytes, tags))
            .await
    }

    /// Writable copy of a snapshot LV, active on return.
    /// No wipe: the data is the point.
    pub async fn restore(
        &self,
        vg: &str,
        source: &str,
        lv: &str,
        tags: &[&str],
    ) -> Result<(), CmdError> {
        Self::check("lvcreate", vg, lv)?;
        self.change(&cmd::restore(vg, source, lv, tags)).await
    }

    /// Read-only thin snapshot, never activated.
    pub async fn snapshot(&self, vg: &str, origin: &str, snap: &str) -> Result<(), CmdError> {
        Self::check("lvcreate", vg, snap)?;
        self.change(&cmd::snapshot(vg, origin, snap)).await
    }

    /// Grow only; lvextend refuses anything else.
    pub async fn extend(&self, vg: &str, lv: &str, bytes: u64) -> Result<(), CmdError> {
        self.change(&cmd::extend(vg, lv, bytes)).await
    }

    pub async fn enable_zeroing(&self, vg: &str, thin: &str) -> Result<(), CmdError> {
        self.change(&cmd::enable_zeroing(vg, thin)).await
    }

    pub async fn activate(&self, vg: &str, lv: &str) -> Result<(), CmdError> {
        self.change(&cmd::activate(vg, lv)).await
    }

    pub async fn deactivate(&self, vg: &str, lv: &str) -> Result<(), CmdError> {
        self.change(&cmd::deactivate(vg, lv)).await
    }

    pub async fn change_tags(
        &self,
        vg: &str,
        lv: &str,
        add: &[&str],
        del: &[&str],
    ) -> Result<(), CmdError> {
        self.change(&cmd::change_tags(vg, lv, add, del)).await
    }

    /// Removes the LV, active or not; a missing LV is done.
    /// lvremove refuses an open LV, so the report is read first and an open
    /// LV is waited for on [`REMOVE_OPEN_WAITS`].
    pub async fn remove(&self, vg: &str, lv: &str) -> Result<(), CmdError> {
        let mut failed: Option<CmdError> = None;
        let mut retried_in_use = false;
        for wait in REMOVE_OPEN_WAITS {
            if !wait.is_zero() {
                tokio::time::sleep(wait).await;
            }
            let Some(row) = self.lvs(vg).await?.into_iter().find(|r| r.name == lv) else {
                return Ok(());
            };
            if row.open {
                // An lvremove that failed meanwhile lost the race to this opener.
                failed = None;
                continue;
            }
            if let Some(e) = failed.take() {
                // An opener came and went between two reads: try once more.
                let in_use = e.message.contains("in use");
                if !in_use || retried_in_use {
                    return Err(e);
                }
                retried_in_use = true;
            }
            match self.change(&cmd::remove(vg, lv)).await {
                Ok(()) => return Ok(()),
                Err(e) => failed = Some(e),
            }
        }
        let waited: Duration = REMOVE_OPEN_WAITS.iter().sum();
        Err(CmdError::new(
            "lvremove",
            format!("{vg}/{lv} is open: still in use after {waited:?}"),
        ))
    }
}

/// What the startup check found about the host's LVM.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HostLvm {
    /// `None` when `lvm version` failed or was unreadable: check again later.
    pub version: Option<(u32, u32, u32)>,
    /// Makes every Pool unavailable: a version read and too old.
    pub unsupported: Option<String>,
    /// Logged only: the host's own LVM may scan DRBD devices or LVs.
    pub warning: Option<String>,
}

/// One host setting; `None` when this LVM does not know it.
async fn host_setting(exec: &dyn Exec, key: &str) -> Option<String> {
    let out = run_any(exec, &cmd::host_setting(key), LVM_REPORT_TIMEOUT)
        .await
        .ok()?;
    (out.exit_code == Some(0))
        .then(|| cmd::parse_host_setting(&out.stdout))
        .flatten()
}

/// LVM version and the host's own scanning rules.
/// Too old for `scan_lvs` blocks the Pools: our commands could then read
/// guest PVs inside volumes. Host scanning is a warning only: it is the
/// host's udev autoactivation, not our commands, and the host may prevent
/// it in ways this check cannot see (event_activation, udev rules).
pub async fn probe_host(exec: &dyn Exec) -> HostLvm {
    let version = run_any(exec, &cmd::version(), LVM_REPORT_TIMEOUT)
        .await
        .ok()
        .and_then(|o| cmd::parse_version(&o.stdout));
    let (a, b, c) = cmd::MIN_VERSION;
    let unsupported = match version {
        Some(v) if v < cmd::MIN_VERSION => Some(format!(
            "LVM {}.{:02}.{:02} is older than {a}.{b:02}.{c:02}: no scan_lvs, so LVM could read guest PVs inside volumes",
            v.0, v.1, v.2
        )),
        _ => None,
    };
    let devices_file = host_setting(exec, "devices/use_devicesfile").await;
    // Missing, LVM scans as if the setting were off.
    let file_present = match devices_file.as_deref() {
        Some("1") => devices_file_absent(exec)
            .await
            .is_ok_and(|missing| missing.is_none()),
        _ => false,
    };
    let scan_lvs = host_setting(exec, "devices/scan_lvs").await;
    let global_filter = host_setting(exec, "devices/global_filter").await;
    HostLvm {
        version,
        unsupported,
        warning: host_scan_warning(
            devices_file.as_deref(),
            file_present,
            scan_lvs.as_deref(),
            global_filter.as_deref(),
        ),
    }
}

/// Where LVM keeps its devices files (lvmdevices(8)).
const DEVICES_DIR: &str = "/etc/lvm/devices";

/// The devices file LVM would use but that does not exist, if so. With
/// `use_devicesfile = 1` and no file, vgcreate and vgextend create one
/// that lists only their new PVs, and every other VG of the host drops
/// out of LVM's view, the root VG included.
pub async fn devices_file_missing(exec: &dyn Exec) -> Result<Option<String>, CmdError> {
    let setting = |key: &'static str| async move {
        let out = run_any(exec, &cmd::host_setting(key), LVM_REPORT_TIMEOUT).await?;
        Ok::<_, CmdError>(
            (out.exit_code == Some(0))
                .then(|| cmd::parse_host_setting(&out.stdout))
                .flatten(),
        )
    };
    match setting("devices/use_devicesfile").await?.as_deref() {
        Some("1") => {}
        Some(_) => return Ok(None),
        None => {
            return Err(CmdError::new(
                "lvmconfig",
                "devices/use_devicesfile unreadable",
            ));
        }
    }
    devices_file_absent(exec).await
}

/// With `use_devicesfile = 1` known: the devices file's path when it does
/// not exist.
pub async fn devices_file_absent(exec: &dyn Exec) -> Result<Option<String>, CmdError> {
    let setting = |key: &'static str| async move {
        let out = run_any(exec, &cmd::host_setting(key), LVM_REPORT_TIMEOUT).await?;
        Ok::<_, CmdError>(
            (out.exit_code == Some(0))
                .then(|| cmd::parse_host_setting(&out.stdout))
                .flatten(),
        )
    };
    let name = setting("devices/devicesfile")
        .await?
        .map(|n| n.trim_matches('"').to_owned())
        .filter(|n| !n.is_empty())
        .unwrap_or_else(|| "system.devices".to_owned());
    if name.contains('/') {
        return Err(CmdError::new(
            "lvmconfig",
            format!("devices/devicesfile {name:?} is not a file name"),
        ));
    }
    let path = format!("{DEVICES_DIR}/{name}");
    let out = run_any(exec, &devices_file_argv(&path), LVM_REPORT_TIMEOUT).await?;
    match out.exit_code {
        Some(0) => Ok(None),
        Some(1) => Ok(Some(path)),
        _ => Err(crate::exec::failure(&devices_file_argv(&path), &out)),
    }
}

/// Runs on the host, where the devices file lives.
pub fn devices_file_argv(path: &str) -> Vec<String> {
    vec!["test".to_owned(), "-f".to_owned(), path.to_owned()]
}

/// With a devices file the host scans only listed devices. Without one,
/// the setting on but the file absent included, it needs scan_lvs = 0 and
/// a global_filter that rejects DRBD devices.
pub fn host_scan_warning(
    devices_file: Option<&str>,
    file_present: bool,
    scan_lvs: Option<&str>,
    global_filter: Option<&str>,
) -> Option<String> {
    const FIX: &str = "use a devices file (an empty one does), or global_filter rejecting /dev/drbd with scan_lvs = 0";
    if devices_file == Some("1") && file_present {
        return None;
    }
    let no_file = if devices_file == Some("1") {
        "use_devicesfile is 1 but the devices file does not exist, so "
    } else {
        ""
    };
    if scan_lvs != Some("0") {
        return Some(format!(
            "{no_file}host LVM scans LVs: udev autoactivation could activate a guest VG inside a volume; {FIX}"
        ));
    }
    match global_filter.and_then(lvm::filter::rejects_drbd) {
        Some(true) => None,
        Some(false) => Some(format!(
            "{no_file}host LVM accepts DRBD devices: udev autoactivation could activate a guest VG through /dev/drbdN; {FIX}"
        )),
        None => Some(format!(
            "{no_file}host global_filter not verified (pattern syntax beyond this check): make sure it rejects /dev/drbd, or {FIX}"
        )),
    }
}

/// Set by lvcreate on every DRBD backing LV: made by r-drbd and never held
/// data, so its DRBD metadata may be (re)created. The ownership tag that
/// replaces it lives in `drbd::md_tag`.
pub const NEW_TAG: &str = "r-drbd.new";

/// Pool capacity from one lvs read of the VG.
pub fn pool_capacity(lvs: &[Lv], thin: &str) -> Result<PoolCapacity, String> {
    let pool = lvs
        .iter()
        .find(|r| r.name == thin)
        .ok_or_else(|| "thin pool not found".to_owned())?;
    if !pool.thin_pool {
        return Err(format!("LV {thin} is not a thin pool"));
    }
    let free_bytes = report::thin_free_bytes(pool)
        .ok_or_else(|| format!("thin pool {thin} is inactive: no usage figure"))?;
    Ok(PoolCapacity {
        total_bytes: pool.size_bytes,
        free_bytes,
        chunk_bytes: pool.chunk_bytes,
    })
}

/// sizeBytes rounded up to the VG extent size.
pub fn desired_bytes(size_bytes: u64, extent_bytes: u64) -> u64 {
    if extent_bytes == 0 {
        return size_bytes;
    }
    size_bytes.div_ceil(extent_bytes) * extent_bytes
}

/// Thin LVs carved from `thin`.
pub fn thin_lvs<'a>(rows: &'a [Lv], thin: &'a str) -> impl Iterator<Item = &'a Lv> {
    rows.iter().filter(move |r| r.pool.as_deref() == Some(thin))
}

#[cfg(test)]
pub(crate) mod fixtures {
    //! Scripted lvm output for a VG `vg1` with thin pool `thin`.

    use crate::exec::scripted::{ScriptedExec, code, ok};

    pub(crate) const VG: &str = "vg1";
    pub(crate) const THIN: &str = "thin";
    /// 4 MiB extents.
    pub(crate) const EXTENT: u64 = 4 << 20;
    /// 50 GiB pool, 10 percent used.
    pub(crate) const POOL_BYTES: u64 = 50 << 30;
    pub(crate) const POOL_FREE: u64 = 45 << 30;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) enum St {
        Inactive,
        Active,
        Open,
    }

    /// An LV of thin pool `pool`; an empty pool makes a linear LV.
    pub(crate) fn lv_in(name: &str, kib: u64, pool: &str, st: St) -> String {
        let (active, open) = match st {
            St::Inactive => ("0", "-1"),
            St::Active => ("1", "0"),
            St::Open => ("1", "1"),
        };
        let segtype = if pool.is_empty() { "linear" } else { "thin" };
        let data = if pool.is_empty() || st == St::Inactive {
            ""
        } else {
            "0.00"
        };
        format!(
            r#"{{"lv_name":"{name}", "lv_path":"/dev/{VG}/{name}", "lv_size":"{}", "segtype":"{segtype}", "pool_lv":"{pool}", "lv_active_locally":"{active}", "lv_device_open":"{open}", "zero":"1", "data_percent":"{data}", "chunk_size":"0"}}"#,
            kib * 1024
        )
    }

    pub(crate) fn lv_row(name: &str, kib: u64, st: St) -> String {
        lv_in(name, kib, THIN, st)
    }

    pub(crate) fn pool_row_zero(zero: bool) -> String {
        format!(
            r#"{{"lv_name":"{THIN}", "lv_path":"", "lv_size":"{POOL_BYTES}", "segtype":"thin-pool", "pool_lv":"", "lv_active_locally":"1", "lv_device_open":"1", "zero":"{}", "data_percent":"10.00", "chunk_size":"65536"}}"#,
            u8::from(zero)
        )
    }

    pub(crate) fn pool_row() -> String {
        pool_row_zero(true)
    }

    /// The JSON document LVM prints around report rows.
    pub(crate) fn json(kind: &str, rows: &[String]) -> String {
        format!(
            "  {{\n      \"report\": [\n          {{\n              \"{kind}\": [\n                  {}\n              ]\n          }}\n      ]\n  }}\n",
            rows.join(",\n                  ")
        )
    }

    /// The lvs report: the pool, then `rows`.
    pub(crate) fn lvs_out(rows: &[String]) -> String {
        let mut all = vec![pool_row()];
        all.extend_from_slice(rows);
        json("lv", &all)
    }

    pub(crate) fn expect_lvs(x: &ScriptedExec, rows: &[String]) {
        x.expect(lvm::cmd::lvs(VG), ok(&lvs_out(rows)));
    }

    pub(crate) fn vgs_out() -> String {
        json(
            "vg",
            &[format!(
                r#"{{"vg_name":"{VG}", "vg_extent_size":"{EXTENT}"}}"#
            )],
        )
    }

    pub(crate) fn expect_vgs(x: &ScriptedExec) {
        x.expect(lvm::cmd::vgs(VG), ok(&vgs_out()));
    }

    /// The tags report: each LV with its tags.
    pub(crate) fn tags_out(lvs: &[(&str, &[&str])]) -> String {
        let rows: Vec<String> = lvs
            .iter()
            .map(|(n, t)| format!(r#"{{"lv_name":"{n}", "lv_tags":"{}"}}"#, t.join(",")))
            .collect();
        json("lv", &rows)
    }

    /// A failed report: LVM logs the error into the JSON, not to stderr.
    pub(crate) fn report_fail(message: &str) -> Result<crate::Output, String> {
        let out = format!(
            r#"{{"report": [{{"lv": []}}], "log": [{{"log_seq_num":"1", "log_type":"error", "log_context":"processing", "log_object_type":"vg", "log_object_name":"{VG}", "log_object_id":"", "log_object_group":"", "log_object_group_id":"", "log_message":{}, "log_errno":"-1", "log_ret_code":"0"}}]}}"#,
            serde_json::to_string(message).unwrap()
        );
        code(5, &out, "")
    }

    pub(crate) fn fail(stderr: &str) -> Result<crate::Output, String> {
        code(5, "", stderr)
    }
}

#[cfg(test)]
mod tests {
    use super::fixtures::*;
    use super::*;
    use crate::exec::scripted::{ScriptedExec, ok};

    #[test]
    fn rounding() {
        assert_eq!(desired_bytes(1 << 30, EXTENT), 1 << 30);
        assert_eq!(desired_bytes((1 << 30) + 1, EXTENT), (1 << 30) + EXTENT);
        assert_eq!(desired_bytes(1000, EXTENT), EXTENT);
        assert_eq!(desired_bytes(1000, 0), 1000);
    }

    #[tokio::test]
    async fn capacity_from_report() {
        let x = Arc::new(ScriptedExec::new());
        expect_lvs(&x, &[]);
        let cap = Lvm::new(x.clone()).probe_pool(VG, THIN).await.unwrap();
        assert_eq!(
            cap,
            PoolCapacity {
                total_bytes: POOL_BYTES,
                free_bytes: POOL_FREE,
                chunk_bytes: 65_536,
            }
        );
        x.assert_done();
    }

    #[tokio::test]
    async fn missing_vg_names_lvm_error() {
        let x = Arc::new(ScriptedExec::new());
        x.expect(
            lvm::cmd::lvs(VG),
            report_fail("Volume group \"vg1\" not found"),
        );
        let e = Lvm::new(x.clone()).probe_pool(VG, THIN).await.unwrap_err();
        assert_eq!(e, "lvs: exit 5: Volume group \"vg1\" not found");
        x.assert_done();
    }

    #[tokio::test]
    async fn pool_must_be_a_thin_pool() {
        let x = Arc::new(ScriptedExec::new());
        x.expect(
            lvm::cmd::lvs(VG),
            ok(&json("lv", &[lv_in(THIN, 4096, "", St::Active)])),
        );
        let e = Lvm::new(x.clone()).probe_pool(VG, THIN).await.unwrap_err();
        assert_eq!(e, "LV thin is not a thin pool");
        let x = Arc::new(ScriptedExec::new());
        x.expect(
            lvm::cmd::lvs(VG),
            ok(&json("lv", &[lv_row("other", 4096, St::Active)])),
        );
        let e = Lvm::new(x.clone()).probe_pool(VG, THIN).await.unwrap_err();
        assert_eq!(e, "thin pool not found");
    }

    #[tokio::test]
    async fn filter_notice_is_dropped_from_errors() {
        let x = Arc::new(ScriptedExec::new());
        let notice = format!("  {}\n", lvm::cmd::FILTER_IGNORED_NOTICE);
        x.expect(
            lvm::cmd::extend(VG, "lv", 1 << 30),
            fail(&format!(
                "{notice}  New size given (256 extents) not larger than existing size (512 extents).\n"
            )),
        );
        let e = Lvm::new(x.clone())
            .extend(VG, "lv", 1 << 30)
            .await
            .unwrap_err();
        assert_eq!(
            e.to_string(),
            "lvextend: exit 5: New size given (256 extents) not larger than existing size (512 extents)."
        );
    }

    #[tokio::test]
    async fn create_checks_the_name_first() {
        let x = Arc::new(ScriptedExec::new());
        let e = Lvm::new(x.clone())
            .create(VG, THIN, "snapshot-1", 1 << 30, &[])
            .await
            .unwrap_err();
        assert_eq!(
            e.to_string(),
            "lvcreate: LVM reserves LV names starting with snapshot: snapshot-1"
        );
        assert!(x.seen.lock().unwrap().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn remove_waits_for_a_brief_open() {
        let x = Arc::new(ScriptedExec::new());
        expect_lvs(&x, &[lv_row("lv", 4096, St::Open)]);
        expect_lvs(&x, &[lv_row("lv", 4096, St::Active)]);
        x.expect(lvm::cmd::remove(VG, "lv"), ok(""));
        Lvm::new(x.clone()).remove(VG, "lv").await.unwrap();
        x.assert_done();
    }

    #[tokio::test(start_paused = true)]
    async fn remove_of_a_held_lv_says_it_is_open() {
        let x = Arc::new(ScriptedExec::new());
        for _ in REMOVE_OPEN_WAITS {
            expect_lvs(&x, &[lv_row("lv", 4096, St::Open)]);
        }
        let e = Lvm::new(x.clone()).remove(VG, "lv").await.unwrap_err();
        assert_eq!(
            e.to_string(),
            "lvremove: vg1/lv is open: still in use after 3s"
        );
        x.assert_done();
    }

    #[tokio::test(start_paused = true)]
    async fn remove_race_with_an_opener_then_other_failure() {
        // lvremove lost the race against an opener: the next read decides.
        let x = Arc::new(ScriptedExec::new());
        expect_lvs(&x, &[lv_row("lv", 4096, St::Active)]);
        x.expect(
            lvm::cmd::remove(VG, "lv"),
            fail("  Logical volume vg1/lv in use.\n"),
        );
        expect_lvs(&x, &[lv_row("lv", 4096, St::Open)]);
        expect_lvs(&x, &[lv_row("lv", 4096, St::Active)]);
        x.expect(lvm::cmd::remove(VG, "lv"), ok(""));
        Lvm::new(x.clone()).remove(VG, "lv").await.unwrap();
        x.assert_done();

        // In use, then closed on the next read: one more lvremove.
        let x = Arc::new(ScriptedExec::new());
        let busy = || fail("  Logical volume vg1/lv in use.\n");
        expect_lvs(&x, &[lv_row("lv", 4096, St::Active)]);
        x.expect(lvm::cmd::remove(VG, "lv"), busy());
        expect_lvs(&x, &[lv_row("lv", 4096, St::Active)]);
        x.expect(lvm::cmd::remove(VG, "lv"), ok(""));
        Lvm::new(x.clone()).remove(VG, "lv").await.unwrap();
        x.assert_done();
        // Only once.
        let x = Arc::new(ScriptedExec::new());
        for _ in 0..2 {
            expect_lvs(&x, &[lv_row("lv", 4096, St::Active)]);
            x.expect(lvm::cmd::remove(VG, "lv"), busy());
        }
        expect_lvs(&x, &[lv_row("lv", 4096, St::Active)]);
        let e = Lvm::new(x.clone()).remove(VG, "lv").await.unwrap_err();
        assert_eq!(
            e.to_string(),
            "lvremove: exit 5: Logical volume vg1/lv in use."
        );
        x.assert_done();

        // A failure on a closed LV is not retried.
        let x = Arc::new(ScriptedExec::new());
        expect_lvs(&x, &[lv_row("lv", 4096, St::Active)]);
        x.expect(lvm::cmd::remove(VG, "lv"), fail("  denied\n"));
        expect_lvs(&x, &[lv_row("lv", 4096, St::Active)]);
        let e = Lvm::new(x.clone()).remove(VG, "lv").await.unwrap_err();
        assert_eq!(e.to_string(), "lvremove: exit 5: denied");
        x.assert_done();
    }

    #[tokio::test]
    async fn remove_of_a_missing_lv_is_done() {
        let x = Arc::new(ScriptedExec::new());
        expect_lvs(&x, &[]);
        Lvm::new(x.clone()).remove(VG, "lv").await.unwrap();
        x.assert_done();
    }

    #[test]
    fn host_scan_rules() {
        assert_eq!(host_scan_warning(Some("1"), true, Some("1"), None), None);
        // The setting on but no file: LVM scans as without it.
        let w = host_scan_warning(Some("1"), false, Some("0"), None).unwrap();
        assert!(w.starts_with("use_devicesfile is 1 but the devices file does not exist, so host global_filter not verified"), "{w}");
        assert_eq!(
            host_scan_warning(Some("1"), false, Some("0"), Some(r#"["r|^/dev/drbd|"]"#)),
            None
        );
        assert_eq!(
            host_scan_warning(
                Some("0"),
                false,
                Some("0"),
                Some(r#"["r|^/dev/drbd|", "a|.*|"]"#)
            ),
            None
        );
        assert!(host_scan_warning(Some("0"), false, Some("0"), Some(r#"["a|.*|"]"#)).is_some());
        assert!(host_scan_warning(None, false, Some("1"), Some(r#"["r|/dev/drbd|"]"#)).is_some());
        assert!(host_scan_warning(None, false, None, None).is_some());
        let w =
            host_scan_warning(None, false, Some("0"), Some(r#"["r|/dev/drbd[0-9]+|"]"#)).unwrap();
        assert!(w.starts_with("host global_filter not verified"), "{w}");
        assert!(
            host_scan_warning(
                None,
                false,
                Some("0"),
                Some(r#"["a|.*|", "r|^/dev/drbd|"]"#)
            )
            .is_some()
        );
    }

    #[tokio::test]
    async fn host_probe_flags_old_lvm() {
        let x = Arc::new(ScriptedExec::new());
        x.expect(
            lvm::cmd::version(),
            ok("  LVM version:     2.02.187(2) (2020-03-24)\n"),
        )
        .expect(
            lvm::cmd::host_setting("devices/use_devicesfile"),
            fail("  Configuration node devices/use_devicesfile not found\n"),
        )
        .expect(
            lvm::cmd::host_setting("devices/scan_lvs"),
            ok("scan_lvs=0\n"),
        )
        .expect(
            lvm::cmd::host_setting("devices/global_filter"),
            ok("global_filter=[\"r|^/dev/drbd|\"]\n"),
        );
        let h = probe_host(x.as_ref()).await;
        assert_eq!(
            h.unsupported.as_deref(),
            Some(
                "LVM 2.02.187 is older than 2.03.03: no scan_lvs, so LVM could read guest PVs inside volumes"
            )
        );
        assert_eq!(h.warning, None);
        x.assert_done();

        // An unreadable version blocks nothing; the caller checks again.
        let x = Arc::new(ScriptedExec::new());
        x.expect(lvm::cmd::version(), fail("  lvm: command failed\n"))
            .expect(
                lvm::cmd::host_setting("devices/use_devicesfile"),
                ok("use_devicesfile=1\n"),
            )
            .expect(
                lvm::cmd::host_setting("devices/devicesfile"),
                ok("devicesfile=\"system.devices\"\n"),
            )
            .expect(devices_file_argv("/etc/lvm/devices/system.devices"), ok(""))
            .expect(
                lvm::cmd::host_setting("devices/scan_lvs"),
                ok("scan_lvs=0\n"),
            )
            .expect(
                lvm::cmd::host_setting("devices/global_filter"),
                ok("global_filter=[\"a|.*|\"]\n"),
            );
        let h = probe_host(x.as_ref()).await;
        assert_eq!((h.version, h.unsupported), (None, None));

        let x = Arc::new(ScriptedExec::new());
        x.expect(lvm::cmd::version(), ok("  LVM version:     2.03.41(2)\n"))
            .expect(
                lvm::cmd::host_setting("devices/use_devicesfile"),
                ok("use_devicesfile=1\n"),
            )
            .expect(
                lvm::cmd::host_setting("devices/devicesfile"),
                ok("devicesfile=\"system.devices\"\n"),
            )
            .expect(devices_file_argv("/etc/lvm/devices/system.devices"), ok(""))
            .expect(
                lvm::cmd::host_setting("devices/scan_lvs"),
                ok("scan_lvs=0\n"),
            )
            .expect(
                lvm::cmd::host_setting("devices/global_filter"),
                ok("global_filter=[\"a|.*|\"]\n"),
            );
        let h = probe_host(x.as_ref()).await;
        assert_eq!(
            (h.version, h.unsupported, h.warning),
            (Some((2, 3, 41)), None, None)
        );

        // The setting on but no file (a stock RHCOS node): LVM scans
        // everything, and the host accepts DRBD devices.
        let x = Arc::new(ScriptedExec::new());
        x.expect(lvm::cmd::version(), ok("  LVM version:     2.03.41(2)\n"))
            .expect(
                lvm::cmd::host_setting("devices/use_devicesfile"),
                ok("use_devicesfile=1\n"),
            )
            .expect(
                lvm::cmd::host_setting("devices/devicesfile"),
                ok("devicesfile=\"system.devices\"\n"),
            )
            .expect(
                devices_file_argv("/etc/lvm/devices/system.devices"),
                crate::exec::scripted::code(1, "", ""),
            )
            .expect(
                lvm::cmd::host_setting("devices/scan_lvs"),
                ok("scan_lvs=0\n"),
            )
            .expect(
                lvm::cmd::host_setting("devices/global_filter"),
                ok("global_filter=[\"a|.*|\"]\n"),
            );
        let h = probe_host(x.as_ref()).await;
        let w = h.warning.unwrap();
        assert!(
            w.starts_with("use_devicesfile is 1 but the devices file does not exist, so host LVM accepts DRBD devices"),
            "{w}"
        );
        x.assert_done();
    }

    #[tokio::test]
    async fn devices_file_must_exist_when_used() {
        let expect = |x: &ScriptedExec, use_df: &str, exists: Option<i32>| {
            x.expect(
                lvm::cmd::host_setting("devices/use_devicesfile"),
                ok(&format!("use_devicesfile={use_df}\n")),
            );
            if let Some(code) = exists {
                x.expect(
                    lvm::cmd::host_setting("devices/devicesfile"),
                    ok("devicesfile=\"system.devices\"\n"),
                )
                .expect(
                    devices_file_argv("/etc/lvm/devices/system.devices"),
                    crate::exec::scripted::code(code, "", ""),
                );
            }
        };
        let x = ScriptedExec::new();
        expect(&x, "1", Some(1));
        assert_eq!(
            devices_file_missing(&x).await.unwrap().as_deref(),
            Some("/etc/lvm/devices/system.devices")
        );
        let x = ScriptedExec::new();
        expect(&x, "1", Some(0));
        assert_eq!(devices_file_missing(&x).await.unwrap(), None);
        let x = ScriptedExec::new();
        expect(&x, "0", None);
        assert_eq!(devices_file_missing(&x).await.unwrap(), None);
        x.assert_done();
    }

    #[tokio::test]
    async fn tags_read() {
        let x = Arc::new(ScriptedExec::new());
        x.expect(
            lvm::cmd::lv_tags(VG),
            ok(&tags_out(&[
                ("a", &["r-drbd.md"]),
                ("b", &[]),
                ("c", &["x", "r-drbd.md.2"]),
            ])),
        );
        let t = Lvm::new(x.clone()).tags(VG).await.unwrap();
        assert_eq!(t["a"], ["r-drbd.md"]);
        assert_eq!(t["b"].len(), 0);
        assert_eq!(t["c"], ["x", "r-drbd.md.2"]);
    }
}
