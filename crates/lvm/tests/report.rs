use lvm::report::{self, Lv, ReportError, Vg};

/// lvs output of a test host, a thin LV, an inactive snapshot and the pool.
const LVS: &str = r#"  {
      "report": [
          {
              "lv": [
                  {"lv_name":"pvc-1", "lv_path":"/dev/vg1/pvc-1", "lv_size":"1073741824", "segtype":"thin", "pool_lv":"thin", "lv_active_locally":"1", "lv_device_open":"1", "zero":"1", "data_percent":"0.00", "chunk_size":"0"},
                  {"lv_name":"pvc-1+s", "lv_path":"/dev/vg1/pvc-1+s", "lv_size":"1073741824", "segtype":"thin", "pool_lv":"thin", "lv_active_locally":"0", "lv_device_open":"-1", "zero":"1", "data_percent":"", "chunk_size":"0"},
                  {"lv_name":"thin", "lv_path":"", "lv_size":"268435456000", "segtype":"thin-pool", "pool_lv":"", "lv_active_locally":"1", "lv_device_open":"1", "zero":"0", "data_percent":"25.50", "chunk_size":"65536"}
              ]
          }
      ]
      ,
      "log": [
          {"log_seq_num":"1", "log_type":"warn", "log_context":"processing", "log_object_type":"lv", "log_object_name":"", "log_object_id":"", "log_object_group":"", "log_object_group_id":"", "log_message":"Please remove the lvm.conf filter, it is ignored with the devices file.", "log_errno":"0", "log_ret_code":"0"}
      ]
  }
"#;

const MISSING_VG: &str = r#"  {
      "report": [
          {
              "lv": [
              ]
          }
      ]
      ,
      "log": [
          {"log_seq_num":"1", "log_type":"warn", "log_context":"processing", "log_object_type":"lv", "log_object_name":"", "log_object_id":"", "log_object_group":"", "log_object_group_id":"", "log_message":"Please remove the lvm.conf filter, it is ignored with the devices file.", "log_errno":"0", "log_ret_code":"0"},
          {"log_seq_num":"2", "log_type":"error", "log_context":"processing", "log_object_type":"vg", "log_object_name":"nosuchvg", "log_object_id":"", "log_object_group":"", "log_object_group_id":"", "log_message":"Volume group \"nosuchvg\" not found", "log_errno":"-1", "log_ret_code":"0"},
          {"log_seq_num":"3", "log_type":"error", "log_context":"processing", "log_object_type":"vg", "log_object_name":"nosuchvg", "log_object_id":"", "log_object_group":"", "log_object_group_id":"", "log_message":"Cannot process volume group nosuchvg", "log_errno":"-1", "log_ret_code":"0"},
          {"log_seq_num":"4", "log_type":"status", "log_context":"processing", "log_object_type":"vg", "log_object_name":"nosuchvg", "log_object_id":"", "log_object_group":"", "log_object_group_id":"", "log_message":"failure", "log_errno":"-1", "log_ret_code":"5"}
      ]
  }
"#;

#[test]
fn lvs_rows() {
    let rows = report::parse_lvs(LVS).unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows[0],
        Lv {
            name: "pvc-1".into(),
            path: "/dev/vg1/pvc-1".into(),
            size_bytes: 1 << 30,
            thin_pool: false,
            pool: Some("thin".into()),
            active: true,
            open: true,
            zeroing: true,
            data_used: rows[0].data_used,
            chunk_bytes: 0,
        }
    );
    let snap = &rows[1];
    assert!(!snap.active && !snap.open && snap.data_used.is_none());
    let pool = &rows[2];
    assert!(pool.thin_pool && !pool.zeroing && pool.pool.is_none());
    assert_eq!(pool.chunk_bytes, 65_536);
    // 250 GiB at 25.50 percent used.
    assert_eq!(report::thin_free_bytes(pool), Some(199_984_414_720));
    assert_eq!(report::thin_free_bytes(snap), None);
}

fn with_field(field: &str, value: &str) -> String {
    let needle = format!("\"{field}\":\"");
    let line = LVS.lines().find(|l| l.contains("\"pvc-1\"")).unwrap();
    let start = line.find(&needle).unwrap() + needle.len();
    let end = start + line[start..].find('"').unwrap();
    let new = format!("{}{value}{}", &line[..start], &line[end..]);
    LVS.replace(line, &new)
}

#[test]
fn bad_fields_are_named() {
    let e = report::parse_lvs(&with_field("lv_size", "1.00g")).unwrap_err();
    assert_eq!(
        e.to_string(),
        r#"lvs gave lv_size "1.00g" for pvc-1: not a whole number of bytes"#
    );
    let e = report::parse_lvs(&with_field("lv_device_open", "open")).unwrap_err();
    assert!(matches!(
        e,
        ReportError::Field {
            field: "lv_device_open",
            ..
        }
    ));
    assert_eq!(
        e.to_string(),
        r#"lvs gave lv_device_open "open" for pvc-1: not 0 or 1"#
    );
    let e = report::parse_lvs(&with_field("data_percent", "101.00")).unwrap_err();
    assert_eq!(
        e.to_string(),
        r#"lvs gave data_percent "101.00" for pvc-1: not a percentage"#
    );
    let e = report::parse_lvs("  LV VG\n").unwrap_err();
    assert!(
        e.to_string()
            .starts_with("lvs printed no JSON report we can read: "),
        "{e}"
    );
    let missing = LVS.replace(r#""segtype":"thin", "#, "");
    let e = report::parse_lvs(&missing).unwrap_err();
    assert!(e.to_string().contains("segtype"), "{e}");
}

#[test]
fn percent_forms() {
    let free = |p: &str| {
        let rows = report::parse_lvs(&with_field("data_percent", p)).unwrap();
        report::thin_free_bytes(&Lv {
            size_bytes: 1_000_000,
            ..rows[0].clone()
        })
    };
    assert_eq!(free("0.00"), Some(1_000_000));
    assert_eq!(free("100.00"), Some(0));
    // Some locales print a decimal comma.
    assert_eq!(free("12,5"), Some(875_000));
    assert_eq!(free("33.333"), Some(666_670));
    assert_eq!(free("7"), Some(930_000));
    for bad in ["-1", ".5", "1.2.3", "1e2", "0.0000000001"] {
        assert!(
            report::parse_lvs(&with_field("data_percent", bad)).is_err(),
            "{bad}"
        );
    }
}

#[test]
fn tags() {
    let json = r#"{"report": [{"lv": [
        {"lv_name":"a", "lv_tags":"r-drbd.md.1,r-drbd.new"},
        {"lv_name":"b", "lv_tags":""}
    ]}]}"#;
    let t = report::parse_tags(json).unwrap();
    assert_eq!(t["a"], ["r-drbd.md.1", "r-drbd.new"]);
    assert_eq!(t["b"].len(), 0);
}

#[test]
fn vgs() {
    let json = r#"  {
      "report": [
          {
              "vg": [
                  {"vg_name":"vg1", "vg_extent_size":"4194304"}
              ]
          }
      ]
  }
"#;
    assert_eq!(
        report::parse_vgs(json).unwrap(),
        vec![Vg {
            name: "vg1".into(),
            extent_bytes: 4 << 20,
        }]
    );
    let e = report::parse_vgs(&json.replace("4194304", "4.00m")).unwrap_err();
    assert_eq!(
        e.to_string(),
        r#"vgs gave vg_extent_size "4.00m" for vg1: not a whole number of bytes"#
    );
}

#[test]
fn failed_report_errors_come_from_the_log() {
    assert_eq!(
        report::log_errors(MISSING_VG),
        [
            "Volume group \"nosuchvg\" not found",
            "Cannot process volume group nosuchvg"
        ]
    );
    assert_eq!(report::log_errors(LVS).len(), 0);
    assert_eq!(report::log_errors("not json").len(), 0);
    assert_eq!(report::parse_lvs(MISSING_VG).unwrap().len(), 0);
}

/// vgs_all on a host with a VG of a devices Pool before its thin pool
/// exists, and another VG.
const VGS_ALL: &str = r#"  {
      "report": [
          {
              "vg": [
                  {"vg_name":"vg1", "vg_extent_size":"4194304", "vg_size":"4290772992", "vg_free":"4290772992", "vg_tags":"r-drbd.pool.p1", "vg_missing_pv_count":"0"},
                  {"vg_name":"other", "vg_extent_size":"4194304", "vg_size":"21470642176", "vg_free":"2105540608", "vg_tags":"", "vg_missing_pv_count":"0"}
              ]
          }
      ]
      ,
      "log": [
          {"log_seq_num":"1", "log_type":"warn", "log_context":"processing", "log_object_type":"vg", "log_object_name":"", "log_object_id":"", "log_object_group":"", "log_object_group_id":"", "log_message":"Please remove the lvm.conf filter, it is ignored with the devices file.", "log_errno":"0", "log_ret_code":"0"}
      ]
  }
"#;

const PVS_ALL: &str = r#"  {
      "report": [
          {
              "pv": [
                  {"pv_name":"/dev/vdb", "vg_name":"other"},
                  {"pv_name":"/dev/vdd", "vg_name":""}
              ]
          }
      ]
  }
"#;

const THIN_POOLS: &str = r#"  {
      "report": [
          {
              "lv": [
                  {"lv_name":"thin", "segtype":"thin-pool", "lv_size":"4148166656", "lv_metadata_size":"71303168", "chunk_size":"65536"},
                  {"lv_name":"pvc-1", "segtype":"thin", "lv_size":"1073741824", "lv_metadata_size":"", "chunk_size":"0"}
              ]
          }
      ]
  }
"#;

#[test]
fn devices_mode_reports() {
    let vgs = report::parse_vgs_all(VGS_ALL).unwrap();
    assert_eq!(
        vgs[0],
        report::VgFull {
            name: "vg1".into(),
            extent_bytes: 4 << 20,
            size_bytes: 4_290_772_992,
            free_bytes: 4_290_772_992,
            tags: vec!["r-drbd.pool.p1".into()],
            missing_pvs: 0,
        }
    );
    assert_eq!(vgs[1].tags.len(), 0);

    let pvs = report::parse_pvs(PVS_ALL).unwrap();
    assert_eq!(pvs[0].vg.as_deref(), Some("other"));
    assert_eq!(
        (pvs[1].name.as_str(), pvs[1].vg.as_deref()),
        ("/dev/vdd", None)
    );

    let lvs = report::parse_thin_pools(THIN_POOLS).unwrap();
    assert!(lvs[0].thin_pool);
    assert_eq!(
        (lvs[0].size_bytes, lvs[0].metadata_bytes, lvs[0].chunk_bytes),
        (4_148_166_656, 71_303_168, 65_536)
    );
    assert!(!lvs[1].thin_pool);
    assert_eq!(lvs[1].metadata_bytes, 0);

    let e = report::parse_vgs_all(&VGS_ALL.replace(
        r#""vg_missing_pv_count":"0""#,
        r#""vg_missing_pv_count":"x""#,
    ))
    .unwrap_err();
    assert!(e.to_string().contains("vg_missing_pv_count"), "{e}");
}
