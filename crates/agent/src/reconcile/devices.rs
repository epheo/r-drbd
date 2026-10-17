//! The Pool path of a Pool built from devices: checks, build and growth
//! one step at a time, and removal under reclaimPolicy Delete. Once the
//! thin pool is ready the Pool goes on as one on an existing VG.

use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;

use kube::ResourceExt;
use lvm::report::{PoolLv, VgFull};
use model::condition;
use model::{DeviceState, DeviceStatus, Pool, ReclaimPolicy};

use super::{
    REASON_BUILDING, REASON_REMOVE_BLOCKED, REASON_REMOVE_FAILED, REASON_REMOVED,
    REASON_UNAVAILABLE, Reads, Reconciler, StatusWrite,
};
use crate::devices::{self, Checked};
use crate::exec::CmdError;
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
    /// (status, reason, message) of Released, under reclaimPolicy Delete.
    pub released: Option<(&'static str, &'static str, String)>,
    pub events: Vec<StatusWrite>,
}

impl Default for Outcome {
    fn default() -> Self {
        Self {
            rows: None,
            gate: Ok(()),
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
}

/// A PV name as the agent's /dev resolves it, so it compares with a
/// resolved by-id link.
fn resolved(name: &str) -> PathBuf {
    std::fs::canonicalize(name).unwrap_or_else(|_| PathBuf::from(name))
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
            .map(|p| (resolved(&p.name), p.vg.unwrap_or_default()))
            .collect();
        let checked =
            devices::check(self.lvm.exec(), self.disks.as_ref(), paths, &pvs, vg_name).await;
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
            match self.look(pool, &paths).await {
                Ok(l) => Outcome {
                    rows: Some(l.rows()),
                    ..Outcome::default()
                },
                Err(e) => Outcome {
                    gate: Err((REASON_UNAVAILABLE, e)),
                    ..Outcome::default()
                },
            }
        };
        self.device_events(pool, out)
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
            // Checks right before every step, in the same pass.
            let look = match self.look(pool, paths).await {
                Ok(l) => l,
                Err(e) => {
                    out.gate = Err((REASON_UNAVAILABLE, e));
                    return out;
                }
            };
            let rows = look.rows();
            let seen = Seen {
                vg: look.vg.as_ref(),
                thin: look.thin.as_ref(),
                devices: &rows,
            };
            let step = plan::plan(t, &seen);
            out.rows = Some(rows.clone());
            let step = match step {
                Ok(Step::Ready) => return out,
                Ok(s) => s,
                Err(Blocked { reason, message }) => {
                    out.gate = Err((reason, message));
                    return out;
                }
            };
            tracing::info!(pool = pool.name_any(), step = step.name(), "building pool");
            let res = self.run_step(t, &step).await;
            reads.invalidate(t.vg);
            if let Err(e) = res {
                out.gate = Err((REASON_BUILDING, format!("{}: {e}", step.name())));
                return out;
            }
        }
        out.gate = Err((
            REASON_BUILDING,
            format!("still building after {MAX_STEPS} steps"),
        ));
        out
    }

    async fn run_step(&self, t: &Target<'_>, step: &Step) -> Result<(), CmdError> {
        let (vg, thin) = (t.vg, t.thin);
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
            Step::GrowData => self.lvm.extend_pool_data(vg, thin).await,
            Step::Ready => Ok(()),
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
        let members: HashSet<&str> = status
            .devices
            .iter()
            .filter(|d| d.state == DeviceState::Member)
            .map(|d| d.path.as_str())
            .collect();
        for _ in 0..MAX_STEPS {
            let look = match self.look(pool, paths).await {
                Ok(l) => l,
                Err(e) => {
                    out.released = Some((condition::FALSE, REASON_REMOVE_FAILED, e));
                    return out;
                }
            };
            out.rows = Some(look.rows());
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
            let orphans: Vec<String> = look
                .checked
                .iter()
                .filter(|c| members.contains(c.row.path.as_str()))
                .filter(|c| {
                    c.node
                        .as_ref()
                        .and_then(|n| look.pvs.get(n))
                        .is_some_and(String::is_empty)
                })
                .map(|c| c.row.path.clone())
                .collect();
            let step = match plan::release(t, look.vg.as_ref(), &lvs, &orphans) {
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
                    return out;
                }
                Ok(s) => s,
                Err(m) => {
                    out.released = Some((condition::FALSE, REASON_REMOVE_BLOCKED, m));
                    return out;
                }
            };
            tracing::info!(pool = pool.name_any(), step = ?step, "releasing pool");
            let res = match &step {
                Release::Deactivate => self.lvm.deactivate(t.vg, t.thin).await,
                Release::RemovePool => self.lvm.remove_inactive(t.vg, t.thin).await,
                Release::RemoveVg => self.lvm.vgremove(t.vg).await,
                Release::RemovePvs(d) => self.lvm.pvremove(d).await,
                Release::Done => Ok(()),
            };
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

    /// One Warning Event per path that cannot join a built Pool, when its
    /// state changes; the row says it on every pass.
    fn device_events(&mut self, pool: &Pool, mut out: Outcome) -> Outcome {
        let name = pool.name_any();
        if out.gate.is_err() || pool.metadata.deletion_timestamp.is_some() {
            self.device_events.remove(&name);
            return out;
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
