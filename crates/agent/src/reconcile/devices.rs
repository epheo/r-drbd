//! The Pool path of a Pool built from devices: checks, build and growth
//! one step at a time, and removal under reclaimPolicy Delete. Once the
//! thin pool is ready the Pool goes on as one on an existing VG.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use kube::ResourceExt;
use lvm::report::{PoolLv, VgFull};
use model::condition;
use model::{DeviceState, DeviceStatus, Pool, ReclaimPolicy};

use super::{
    REASON_BUILDING, REASON_DEVICE_MISSING, REASON_REMOVE_BLOCKED, REASON_REMOVE_FAILED,
    REASON_REMOVED, REASON_UNAVAILABLE, REASON_VG_NOT_OWNED, Reads, Reconciler, StatusWrite,
};
use crate::devices::{self, Checked};
use crate::exec::CmdError;
use crate::storage;
use crate::storage::plan::{self, Blocked, Release, Seen, Step, Target};

/// More steps than a build and a growth take; a loop past it is a bug.
const MAX_STEPS: usize = 8;

/// What the devices path found and did in one pass.
#[derive(Debug)]
pub(super) struct Outcome {
    /// `None` when the host could not be read: the last rows stay.
    pub rows: Option<Vec<DeviceStatus>>,
    /// Err: Available False with this reason; Ok: the usual probe decides.
    pub gate: Result<(), (&'static str, String)>,
    /// No LV may be made or looked for: the VG is absent or not ours, or
    /// it has no thin pool. Only then can none of the Pool's LVs exist.
    pub unready: bool,
    /// The message of Available when the probe makes it True.
    pub note: Option<String>,
    /// (status, reason, message) of Released, under reclaimPolicy Delete.
    pub released: Option<(&'static str, &'static str, String)>,
    pub events: Vec<StatusWrite>,
}

impl Default for Outcome {
    fn default() -> Self {
        Self {
            rows: None,
            gate: Ok(()),
            unready: false,
            note: None,
            released: None,
            events: Vec::new(),
        }
    }
}

/// One look at the host for one Pool.
struct Look {
    vg: Option<VgFull>,
    thin: Option<PoolLv>,
    /// Resolved device node to VG name, empty for an orphan PV.
    pvs: BTreeMap<PathBuf, String>,
    checked: Vec<Checked>,
}

impl Look {
    fn rows(&self) -> Vec<DeviceStatus> {
        self.checked.iter().map(|c| c.row.clone()).collect()
    }

    /// The VG is ours and holds the thin pool: the Pool serves.
    fn built(&self, t: &Target<'_>) -> bool {
        self.vg
            .as_ref()
            .is_some_and(|v| v.tags.iter().any(|g| g == t.tag))
            && self.thin.as_ref().is_some_and(|l| l.thin_pool)
    }

    /// Listed paths that are PVs without a VG and were `members`.
    fn orphans(&self, members: &BTreeSet<&str>) -> Vec<String> {
        self.checked
            .iter()
            .filter(|c| members.contains(c.row.path.as_str()))
            .filter(|c| {
                c.node
                    .as_ref()
                    .and_then(|n| self.pvs.get(n))
                    .is_some_and(String::is_empty)
            })
            .map(|c| c.row.path.clone())
            .collect()
    }

    /// A VG of that name exists without the Pool's tag: not even the
    /// usual probe may run, it can switch on zeroing.
    fn foreign(&self, t: &Target<'_>) -> Option<(&'static str, String)> {
        let vg = self.vg.as_ref()?;
        (!vg.tags.iter().any(|g| g == t.tag)).then(|| {
            (
                REASON_VG_NOT_OWNED,
                format!(
                    "volume group {} exists without tag {}: never touched",
                    t.vg, t.tag
                ),
            )
        })
    }
}

/// Up to three PVs with their VG, for the devices-file refusal.
fn pv_list(pvs: &BTreeMap<PathBuf, String>) -> String {
    let mut s: Vec<String> = pvs
        .iter()
        .take(3)
        .map(|(p, vg)| {
            if vg.is_empty() {
                format!("{} without a VG", p.display())
            } else {
                format!("{} of {vg}", p.display())
            }
        })
        .collect();
    if pvs.len() > 3 {
        s.push("...".to_owned());
    }
    s.join(", ")
}

/// The host could not be read: for a Pool that showed a member, the usual
/// probe decides, as for a Pool on an existing VG, and the rows stay. One
/// that never did can hold no LV yet.
fn look_failed(pool: &Pool, error: String) -> Outcome {
    tracing::warn!(pool = pool.name_any(), %error, "pool devices unreadable");
    let built_once = pool
        .status
        .as_ref()
        .is_some_and(|s| s.devices.iter().any(|d| d.state == DeviceState::Member));
    if built_once {
        return Outcome::default();
    }
    // Not even the probe: the VG might not be ours.
    Outcome {
        gate: Err((REASON_UNAVAILABLE, error)),
        unready: true,
        ..Outcome::default()
    }
}

impl Reconciler {
    async fn look(&self, pool: &Pool, paths: &[String]) -> Result<Look, String> {
        let (vg_name, thin) = (&pool.spec.vg, &pool.spec.thin_pool);
        let vg = self
            .lvm
            .vgs_all()
            .await
            .map_err(|e| e.to_string())?
            .into_iter()
            .find(|v| &v.name == vg_name);
        let thin = match &vg {
            Some(_) => self
                .lvm
                .thin_pools(vg_name)
                .await
                .map_err(|e| e.to_string())?
                .into_iter()
                .find(|l| &l.name == thin),
            None => None,
        };
        let pvs = self
            .lvm
            .pvs_all()
            .await
            .map_err(|e| e.to_string())?
            .into_iter()
            .map(|p| (devices::resolved(&p.name), p.vg.unwrap_or_default()))
            .collect();
        let checked = devices::check(self.lvm.exec(), &self.disks, paths, &pvs, vg_name).await;
        Ok(Look {
            vg,
            thin,
            pvs,
            checked,
        })
    }

    pub(super) async fn devices_pool(&mut self, reads: &mut Reads, pool: &Pool) -> Outcome {
        let paths = pool.spec.devices.clone().unwrap_or_default();
        let tag = model::keys::pool_vg_tag(&pool.name_any());
        let chunk = match plan::chunk_bytes(pool.spec.chunk_size.as_ref().map(|q| q.0.as_str())) {
            Ok(c) => c,
            Err(e) => {
                return Outcome {
                    gate: Err((REASON_UNAVAILABLE, e)),
                    unready: true,
                    ..Outcome::default()
                };
            }
        };
        let t = Target {
            tag: &tag,
            vg: &pool.spec.vg,
            thin: &pool.spec.thin_pool,
            chunk_bytes: chunk,
        };
        let out = if pool.metadata.deletion_timestamp.is_none() {
            self.build(reads, pool, &paths, &t).await
        } else if pool.spec.reclaim() == ReclaimPolicy::Delete {
            self.release(reads, pool, &paths, &t).await
        } else {
            // Retained: look only, never build what is going away.
            let _held = self.lock_disks().await;
            match self.look(pool, &paths).await {
                Ok(l) => Outcome {
                    rows: Some(l.rows()),
                    unready: !l.built(&t),
                    gate: l.foreign(&t).map_or(Ok(()), Err),
                    ..Outcome::default()
                },
                Err(e) => look_failed(pool, e),
            }
        };
        // Against the rows seen last, not the status: a failing status
        // write must not wake the inventory every pass.
        if let Some(rows) = &out.rows
            && self.device_rows.get(&pool.name_any()) != Some(rows)
        {
            self.device_rows.insert(pool.name_any(), rows.clone());
            self.disks_changed = true;
        }
        self.device_events(pool, out)
    }

    /// A Pool's disks changed since the last call: NodeDisks is due.
    pub fn take_disks_changed(&mut self) -> bool {
        std::mem::take(&mut self.disks_changed)
    }

    async fn build(
        &mut self,
        reads: &mut Reads,
        pool: &Pool,
        paths: &[String],
        t: &Target<'_>,
    ) -> Outcome {
        let mut out = Outcome::default();
        for _ in 0..MAX_STEPS {
            // Each look and its step under the disk lock: the inventory's
            // exclusive open never meets our open or a vgcreate.
            let _held = self.lock_disks().await;
            // Checks right before every step, in the same pass.
            let look = match self.look(pool, paths).await {
                Ok(l) => l,
                Err(e) => return look_failed(pool, e),
            };
            let built = look.built(t);
            let mut rows = look.rows();
            let seen = Seen {
                vg: look.vg.as_ref(),
                thin: look.thin.as_ref(),
                devices: &rows,
            };
            let step = plan::plan(t, &seen);
            out.rows = Some(rows.clone());
            let step = match step {
                Ok(Step::Ready) => {
                    if let (Some(vg), Some(thin)) = (&look.vg, &look.thin) {
                        out.note = plan::unused(vg, thin);
                    }
                    return out;
                }
                Ok(s) => s,
                // A partial VG may still serve what it holds: Available
                // False, the Pool's LVs stay in reach.
                Err(b) if built && b.reason == REASON_DEVICE_MISSING => {
                    out.gate = Err((b.reason, b.message));
                    return out;
                }
                Err(Blocked { message, .. }) if built => {
                    out.note = Some(message);
                    return out;
                }
                Err(Blocked { reason, message }) => {
                    out.gate = Err((reason, message));
                    out.unready = true;
                    return out;
                }
            };
            tracing::info!(pool = pool.name_any(), step = step.name(), "building pool");
            let res = self.run_step(t, &look.pvs, &step).await;
            self.disks_changed |= res.is_ok();
            reads.invalidate(t.vg);
            let Err(e) = res else { continue };
            let message = format!("{}: {e}", step.name());
            tracing::warn!(pool = pool.name_any(), error = %message, "pool step failed");
            if !built {
                out.gate = Err((REASON_BUILDING, message));
                out.unready = true;
                return out;
            }
            // A failed growth leaves the built Pool serving as it was.
            if let Step::Vgextend(added) = &step {
                for r in rows.iter_mut().filter(|r| added.contains(&r.path)) {
                    r.message = Some(message.clone());
                }
                out.rows = Some(rows);
            }
            out.note = Some(message);
            return out;
        }
        out.note = Some(format!("still building after {MAX_STEPS} steps"));
        out
    }

    async fn run_step(
        &self,
        t: &Target<'_>,
        host_pvs: &BTreeMap<PathBuf, String>,
        step: &Step,
    ) -> Result<(), CmdError> {
        let (vg, thin) = (t.vg, t.thin);
        if matches!(step, Step::Vgcreate(_) | Step::Vgextend(_)) {
            let missing = storage::devices_file_missing(self.lvm.exec()).await?;
            if let Some(path) = missing {
                // Without the file LVM sees every PV; the command creates
                // the file holding only its own disks. What counts is the
                // host's PVs on disks and partitions: our view leaves out
                // DRBD devices and LVs on purpose, the host must not scan
                // them either, so a file without them is the wanted state.
                // With none to hide, the new file lists exactly the host's
                // PVs. The advice never says --all: on a host without a
                // filter that would import guest PVs found on DRBD devices.
                if !host_pvs.is_empty() {
                    return Err(CmdError::new(
                        "lvm",
                        format!(
                            "use_devicesfile is 1 but {path} does not exist: this command would create it with only the new disks and hide the host's {} PV(s) ({}); add them to a devices file first, `vgimportdevices <vg>` per VG or `lvmdevices --adddev <device>` for a PV without one, never `--all` on a host whose DRBD devices may hold guest LVM",
                            host_pvs.len(),
                            pv_list(host_pvs)
                        ),
                    ));
                }
                tracing::info!(
                    vg,
                    path,
                    "no devices file and no PV on the host: lvm creates the file with the new disks"
                );
            }
        }
        match step {
            Step::Vgcreate(d) => self.lvm.vgcreate(vg, t.tag, d).await,
            Step::CreateThinPool {
                chunk_bytes,
                meta_bytes,
            } => {
                self.lvm
                    .create_thin_pool(vg, thin, *chunk_bytes, *meta_bytes)
                    .await
            }
            Step::Vgextend(d) => self.lvm.vgextend(vg, d).await,
            Step::GrowMetadata(b) => self.lvm.extend_pool_metadata(vg, thin, *b).await,
            Step::GrowData(None) => self.lvm.extend_pool_data(vg, thin).await,
            Step::GrowData(Some(b)) => self.lvm.extend_pool_data_to(vg, thin, *b).await,
            Step::Ready => Ok(()),
        }
    }

    async fn run_release(&self, t: &Target<'_>, step: &Release) -> Result<(), CmdError> {
        match step {
            Release::Deactivate => self.lvm.deactivate(t.vg, t.thin).await,
            Release::RemovePool => self.lvm.remove_inactive(t.vg, t.thin).await,
            Release::RemoveVg => self.lvm.vgremove(t.vg).await,
            Release::RemovePvs(d) => self.lvm.pvremove(d).await,
            Release::Done => Ok(()),
        }
    }

    /// reclaimPolicy Delete on a deleted Pool. The controller waits on
    /// Released only once no Volume is left; status.volumes is checked
    /// again here so nothing is removed under a late one.
    async fn release(
        &mut self,
        reads: &mut Reads,
        pool: &Pool,
        paths: &[String],
        t: &Target<'_>,
    ) -> Outcome {
        let mut out = Outcome::default();
        let status = pool.status.clone().unwrap_or_default();
        // Recorded while the VG stood: a PV left orphan by a crash after
        // vgremove is cleared only if it was ours.
        let members: BTreeSet<&str> = status
            .devices
            .iter()
            .filter(|d| d.state == DeviceState::Member)
            .map(|d| d.path.as_str())
            .collect();
        for _ in 0..MAX_STEPS {
            let _held = self.lock_disks().await;
            let look = match self.look(pool, paths).await {
                Ok(l) => l,
                Err(e) => {
                    out.released = Some((condition::FALSE, REASON_REMOVE_FAILED, e));
                    return out;
                }
            };
            let orphans = look.orphans(&members);
            // An ex-member whose label is still there stays Member in the
            // written rows, or a failed pvremove would be forgotten.
            let mut rows = look.rows();
            for r in rows.iter_mut().filter(|r| orphans.contains(&r.path)) {
                r.state = DeviceState::Member;
                r.message = Some("PV label left after the VG was removed: pvremove pending".into());
            }
            out.rows = Some(rows.clone());
            out.unready = !look.built(t);
            if let Some(e) = look.foreign(t) {
                out.gate = Err(e);
            }
            if !status.volumes.is_empty() {
                out.released = Some((
                    condition::FALSE,
                    REASON_REMOVE_BLOCKED,
                    format!(
                        "volumes still placed here: {}: nothing removed",
                        status.volumes.join(", ")
                    ),
                ));
                return out;
            }
            let owned = look
                .vg
                .as_ref()
                .is_some_and(|v| v.tags.iter().any(|g| g == t.tag));
            let lvs: Vec<(String, bool)> = if owned {
                match reads.lvs(&self.lvm, t.vg).await {
                    Ok(rows) => rows.into_iter().map(|r| (r.name, r.active)).collect(),
                    Err(e) => {
                        out.released = Some((condition::FALSE, REASON_REMOVE_FAILED, e));
                        return out;
                    }
                }
            } else {
                Vec::new()
            };
            let step = match plan::release(t, look.vg.as_ref(), &lvs, &orphans, &rows, &members) {
                Ok(Release::Done) => {
                    out.released = Some((
                        condition::TRUE,
                        REASON_REMOVED,
                        format!("thin pool, volume group {} and PV labels removed", t.vg),
                    ));
                    out.gate = Err((
                        REASON_UNAVAILABLE,
                        "removed: reclaimPolicy Delete".to_owned(),
                    ));
                    out.unready = true;
                    return out;
                }
                Ok(s) => s,
                Err(m) => {
                    out.released = Some((condition::FALSE, REASON_REMOVE_BLOCKED, m));
                    return out;
                }
            };
            tracing::info!(pool = pool.name_any(), step = ?step, "releasing pool");
            let res = self.run_release(t, &step).await;
            self.disks_changed |= res.is_ok();
            reads.invalidate(t.vg);
            if let Err(e) = res {
                out.released = Some((
                    condition::FALSE,
                    REASON_REMOVE_FAILED,
                    format!("{step:?}: {e}"),
                ));
                return out;
            }
        }
        out.released = Some((
            condition::FALSE,
            REASON_REMOVE_FAILED,
            format!("still removing after {MAX_STEPS} steps"),
        ));
        out
    }

    /// The Events of [`Self::device_events`] for `pool` did not reach the
    /// API: forget them so the next pass sends them again.
    pub(crate) fn events_unsent(&mut self, pool: &str) {
        self.device_events.remove(pool);
        self.pool_notes.remove(pool);
    }

    /// One Warning Event per path that cannot join a built Pool, when its
    /// state changes, and one per new note; rows and Available say it on
    /// every pass.
    fn device_events(&mut self, pool: &Pool, mut out: Outcome) -> Outcome {
        let name = pool.name_any();
        if out.gate.is_err() || pool.metadata.deletion_timestamp.is_some() {
            self.device_events.remove(&name);
            self.pool_notes.remove(&name);
            return out;
        }
        if out.note != self.pool_notes.get(&name).cloned() {
            if let Some(n) = &out.note {
                out.events.push(StatusWrite::Event {
                    name: name.clone(),
                    uid: pool.metadata.uid.clone(),
                    reason: "PoolNotGrown".into(),
                    message: n.clone(),
                });
                self.pool_notes.insert(name.clone(), n.clone());
            } else {
                self.pool_notes.remove(&name);
            }
        }
        let Some(rows) = &out.rows else {
            return out;
        };
        let now: BTreeMap<String, DeviceState> = plan::stuck(rows)
            .into_iter()
            .map(|r| (r.path.clone(), r.state))
            .collect();
        let before = self.device_events.get(&name);
        let new: Vec<&DeviceStatus> = plan::stuck(rows)
            .into_iter()
            .filter(|r| before.and_then(|b| b.get(&r.path)) != Some(&r.state))
            .collect();
        if !new.is_empty() {
            out.events.push(StatusWrite::Event {
                name: name.clone(),
                uid: pool.metadata.uid.clone(),
                reason: "DeviceNotUsable".into(),
                message: format!("not added to the pool: {}", plan::describe(&new)),
            });
        }
        self.device_events.insert(name, now);
        out
    }
}

#[cfg(test)]
mod tests;
