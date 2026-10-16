//! Parsers for the JSON reports of [`crate::cmd::lvs`], [`crate::cmd::lv_tags`]
//! and [`crate::cmd::vgs`].

use std::collections::HashMap;

use serde::Deserialize;

/// One LV of the report. The pool fields are only meaningful on a thin pool.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lv {
    pub name: String,
    /// Empty for a thin pool, which has no device of its own.
    pub path: String,
    pub size_bytes: u64,
    pub thin_pool: bool,
    /// The thin pool holding this LV.
    pub pool: Option<String>,
    pub active: bool,
    pub open: bool,
    pub zeroing: bool,
    /// Used share of a thin pool's data; `None` while the pool is inactive.
    pub data_used: Option<Percent>,
    pub chunk_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Vg {
    pub name: String,
    pub extent_bytes: u64,
}

/// A percentage as LVM prints it: `digits / 10^scale` percent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Percent {
    digits: u64,
    scale: u32,
}

impl Percent {
    /// More decimals than this would overflow the free space math.
    const MAX_SCALE: u32 = 9;

    fn parse(s: &str) -> Option<Self> {
        let (int, frac) = s.split_once(['.', ',']).unwrap_or((s, ""));
        let scale = u32::try_from(frac.len()).ok()?;
        let all_digits = |p: &str| p.bytes().all(|b| b.is_ascii_digit());
        if int.is_empty() || !all_digits(int) || !all_digits(frac) || scale > Self::MAX_SCALE {
            return None;
        }
        let digits: u64 = format!("{int}{frac}").parse().ok()?;
        let p = Self { digits, scale };
        (p.digits <= p.whole()).then_some(p)
    }

    fn whole(self) -> u64 {
        100 * 10u64.pow(self.scale)
    }
}

/// Free bytes of a thin pool: its size times the unused share of its data,
/// rounded down. `None` while the pool is inactive.
pub fn thin_free_bytes(pool: &Lv) -> Option<u64> {
    let used = pool.data_used?;
    let whole = u128::from(used.whole());
    let free = u128::from(pool.size_bytes) * (whole - u128::from(used.digits)) / whole;
    u64::try_from(free).ok()
}

/// A report that is not what the command was asked to print.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ReportError {
    #[error("{report} printed no JSON report we can read: {reason}")]
    Shape {
        report: &'static str,
        reason: String,
    },
    #[error("{report} gave {field} {value:?} for {object}: {reason}")]
    Field {
        report: &'static str,
        object: String,
        field: &'static str,
        value: String,
        reason: &'static str,
    },
}

#[derive(Deserialize)]
struct Doc<S> {
    report: Vec<S>,
}

#[derive(Deserialize)]
struct LogEntry {
    log_type: String,
    log_message: String,
}

#[derive(Deserialize)]
struct LvSection<R> {
    lv: Vec<R>,
}

#[derive(Deserialize)]
struct VgSection {
    vg: Vec<RawVg>,
}

#[derive(Deserialize)]
struct RawLv {
    lv_name: String,
    lv_path: String,
    lv_size: String,
    segtype: String,
    pool_lv: String,
    lv_active_locally: String,
    lv_device_open: String,
    zero: String,
    data_percent: String,
    chunk_size: String,
}

#[derive(Deserialize)]
struct RawTags {
    lv_name: String,
    lv_tags: String,
}

#[derive(Deserialize)]
struct RawVg {
    vg_name: String,
    vg_extent_size: String,
}

fn doc<S: for<'de> Deserialize<'de>>(
    report: &'static str,
    json: &str,
) -> Result<Doc<S>, ReportError> {
    serde_json::from_str(json).map_err(|e| ReportError::Shape {
        report,
        reason: e.to_string(),
    })
}

/// Field parsing for one object of one report.
struct Ctx<'a> {
    report: &'static str,
    object: &'a str,
}

impl Ctx<'_> {
    fn err(&self, field: &'static str, value: &str, reason: &'static str) -> ReportError {
        ReportError::Field {
            report: self.report,
            object: self.object.to_owned(),
            field,
            value: value.to_owned(),
            reason,
        }
    }

    fn bytes(&self, field: &'static str, v: &str) -> Result<u64, ReportError> {
        v.parse()
            .map_err(|_| self.err(field, v, "not a whole number of bytes"))
    }

    /// `--binary`: 1 is set; 0, and -1 or empty where LVM cannot tell
    /// (an inactive LV), are not.
    fn flag(&self, field: &'static str, v: &str) -> Result<bool, ReportError> {
        match v {
            "1" => Ok(true),
            "0" | "-1" | "" => Ok(false),
            _ => Err(self.err(field, v, "not 0 or 1")),
        }
    }
}

pub fn parse_lvs(json: &str) -> Result<Vec<Lv>, ReportError> {
    const R: &str = "lvs";
    let d: Doc<LvSection<RawLv>> = doc(R, json)?;
    d.report
        .into_iter()
        .flat_map(|s| s.lv)
        .map(|r| {
            let c = Ctx {
                report: R,
                object: &r.lv_name,
            };
            let data_used = match r.data_percent.as_str() {
                "" => None,
                v => Some(
                    Percent::parse(v)
                        .ok_or_else(|| c.err("data_percent", v, "not a percentage"))?,
                ),
            };
            Ok(Lv {
                size_bytes: c.bytes("lv_size", &r.lv_size)?,
                chunk_bytes: c.bytes("chunk_size", &r.chunk_size)?,
                active: c.flag("lv_active_locally", &r.lv_active_locally)?,
                open: c.flag("lv_device_open", &r.lv_device_open)?,
                zeroing: c.flag("zero", &r.zero)?,
                data_used,
                thin_pool: r.segtype == "thin-pool",
                pool: Some(r.pool_lv).filter(|p| !p.is_empty()),
                path: r.lv_path,
                name: r.lv_name,
            })
        })
        .collect()
}

/// Tags per LV name; an LV without tags maps to an empty list.
pub fn parse_tags(json: &str) -> Result<HashMap<String, Vec<String>>, ReportError> {
    let d: Doc<LvSection<RawTags>> = doc("lvs", json)?;
    Ok(d.report
        .into_iter()
        .flat_map(|s| s.lv)
        .map(|r| {
            let tags = r
                .lv_tags
                .split(',')
                .filter(|t| !t.is_empty())
                .map(str::to_owned)
                .collect();
            (r.lv_name, tags)
        })
        .collect())
}

pub fn parse_vgs(json: &str) -> Result<Vec<Vg>, ReportError> {
    const R: &str = "vgs";
    let d: Doc<VgSection> = doc(R, json)?;
    d.report
        .into_iter()
        .flat_map(|s| s.vg)
        .map(|r| {
            let c = Ctx {
                report: R,
                object: &r.vg_name,
            };
            Ok(Vg {
                extent_bytes: c.bytes("vg_extent_size", &r.vg_extent_size)?,
                name: r.vg_name,
            })
        })
        .collect()
}

/// The error messages of a failed report. In JSON mode LVM logs them into
/// the document, not to stderr.
pub fn log_errors(json: &str) -> Vec<String> {
    #[derive(Deserialize)]
    struct LogOnly {
        #[serde(default)]
        log: Vec<LogEntry>,
    }
    serde_json::from_str::<LogOnly>(json)
        .map(|d| {
            d.log
                .into_iter()
                .filter(|e| e.log_type == "error")
                .map(|e| e.log_message)
                .collect()
        })
        .unwrap_or_default()
}
