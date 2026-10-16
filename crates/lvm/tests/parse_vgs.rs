use lvm::ParseError;
use lvm::report::{VgRow, vgs};

// A vgs output sample with the 7 thin fields the agent requests, one row per LV, with the --noheadings indent.
const SAMPLE: &str = "  vg0;4096.00;20967424.00;10477568.00;tp0;10485760.00;25.50
  vg0;4096.00;20967424.00;10477568.00;pvc-2;1052672.00;
";

#[test]
fn thin_sample() {
    let rows = vgs(SAMPLE).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0],
        VgRow {
            vg_name: "vg0".to_owned(),
            vg_extent_size: 4096,
            vg_size: 20_967_424,
            vg_free: 10_477_568,
            lv_name: "tp0".to_owned(),
            lv_size: 10_485_760,
            data_percent: 25.5,
        }
    );
}

#[test]
fn empty_data_percent_reads_as_100() {
    let rows = vgs(SAMPLE).unwrap();
    assert!((rows[1].data_percent - 100.0).abs() < f32::EPSILON);
}

#[test]
fn comma_decimals_and_wrong_counts() {
    let out = "  vg0;4096,00;20967424,00;0,00\n  vg0;4096,00;20967424,00;0,00;tp0;1024,00;50,00\n";
    let rows = vgs(out).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].vg_extent_size, 4096);
    assert!((rows[0].data_percent - 50.0).abs() < f32::EPSILON);
}

#[test]
fn error_message() {
    assert_eq!(
        vgs("vg0;4096.00;x;0.00;tp0;1024.00;1.00").unwrap_err(),
        ParseError::Field {
            what: "Unable to parse 'vg_size' of vgs for volume group 'vg0'".to_owned(),
            value: "x".to_owned(),
        }
    );
    assert_eq!(
        vgs("vg0;4096.00;1.00;0.00;tp0;1024.00;n/a").unwrap_err(),
        ParseError::Field {
            what: "Unable to parse 'data_percent' of vgs for volume group 'vg0'".to_owned(),
            value: "n/a".to_owned(),
        }
    );
}

// Row shape of a VG without LVs, as given in the review.
#[test]
fn lvless_vg_row_skipped() {
    let out = "  vg0;4096.00;20967424.00;10477568.00;tp0;10485760.00;25.50
  vg1;4096.00;20967424.00;20967424.00;;;
  vg2;4096.00;20967424.00;10477568.00;tp2;10485760.00;10.00
";
    let rows = vgs(out).unwrap();
    let names: Vec<&str> = rows.iter().map(|r| r.vg_name.as_str()).collect();
    assert_eq!(names, ["vg0", "vg2"]);
}
