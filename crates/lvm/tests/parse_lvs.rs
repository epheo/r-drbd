use lvm::ParseError;
use lvm::report::{LvRow, lvs};

// An lvs output sample with the 11 fields the agent requests, with the leading indent LVM prints under --noheadings.
const SAMPLE: &str = "  tp0;/dev/vg0/tp0;10485760.00;vg0;;25.50;twi-aotz--;3.12;64.00;1;
  pvc-1;/dev/vg0/pvc-1;1052672.00;vg0;tp0;12.00;Vwi-aotz--;;0.00;1;
  pvc-1_snap1;/dev/vg0/pvc-1_snap1;1052672.00;vg0;tp0;12.00;Vri---tz-k;;0.00;1;pvc-1
  pvc-2;/dev/vg0/pvc-2;1052672.00;vg0;;;-wi-------;;0.00;1;
";

#[test]
fn sample_rows() {
    let rows = lvs(SAMPLE).unwrap();
    assert_eq!(rows.len(), 4);
    assert_eq!(
        rows[0],
        LvRow {
            lv_name: "tp0".to_owned(),
            lv_path: "/dev/vg0/tp0".to_owned(),
            lv_size: 10_485_760,
            vg_name: "vg0".to_owned(),
            pool_lv: None,
            data_percent: 25.5,
            lv_attr: "twi-aotz--".to_owned(),
            metadata_percent: "3.12".to_owned(),
            chunk_size: 64,
            stripes: 1,
            origin: None,
        }
    );
    assert!(rows[0].is_active());
    assert!(rows[0].is_zeroing());
    assert_eq!(rows[1].pool_lv.as_deref(), Some("tp0"));
    assert_eq!(rows[1].metadata_percent, "");
    assert_eq!(rows[2].origin.as_deref(), Some("pvc-1"));
    assert!(!rows[2].is_active());
}

#[test]
fn empty_pool_data_percent_and_origin() {
    let rows = lvs(SAMPLE).unwrap();
    let r = &rows[3];
    assert_eq!(r.pool_lv, None);
    assert!((r.data_percent - 100.0).abs() < f32::EPSILON);
    assert_eq!(r.origin, None);
    assert!(!r.is_active());
}

#[test]
fn comma_decimals() {
    let rows =
        lvs("  pvc-1;/dev/vg0/pvc-1;1052672,00;vg0;tp0;12,50;Vwi-aotz--;;64,00;1;\n").unwrap();
    assert_eq!(rows[0].lv_size, 1_052_672);
    assert!((rows[0].data_percent - 12.5).abs() < f32::EPSILON);
    assert_eq!(rows[0].chunk_size, 64);
}

#[test]
fn wrong_field_counts_ignored() {
    let out = "\n  garbage\n  a;b;c\n  pvc-1;/dev/vg0/pvc-1;4096.00;vg0;tp0;1.00;Vwi-a-tz--;;0.00;1;;extra\n  pvc-2;/dev/vg0/pvc-2;4096.00;vg0;tp0;1.00;Vwi-a-tz--;;0.00;1;\n";
    let rows = lvs(out).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].lv_name, "pvc-2");
    assert_eq!(lvs("").unwrap(), Vec::new());
}

fn field_err(line: &str) -> ParseError {
    lvs(line).unwrap_err()
}

#[test]
fn errors_carry_spec_messages() {
    assert_eq!(
        field_err("a;/dev/v/a;x;v;;1.0;-wi-a-----;;0.00;1;"),
        ParseError::Field {
            what: "Unable to parse logical volume size".to_owned(),
            value: "x".to_owned(),
        }
    );
    assert_eq!(
        field_err("a;/dev/v/a;4096.00;v;tp;abc;Vwi-a-tz--;;0.00;1;"),
        ParseError::Field {
            what: "Unable to parse data_percent of thin lv".to_owned(),
            value: "abc".to_owned(),
        }
    );
    assert_eq!(
        field_err("a;/dev/v/a;4096.00;v;tp;1.0;Vwi-a-tz--;;?;1;"),
        ParseError::Field {
            what: "Unable to parse logical chunk size".to_owned(),
            value: "?".to_owned(),
        }
    );
    let e = field_err("a;/dev/v/a;4096.00;v;tp;1.0;Vwi-a-tz--;;0.00;two;");
    assert_eq!(
        e,
        ParseError::Field {
            what: "Unable to parse stripes".to_owned(),
            value: "two".to_owned(),
        }
    );
    assert_eq!(e.to_string(), "Unable to parse stripes: 'two'");
}
