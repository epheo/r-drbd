use lvm::report::{VgRow, blockdev_size, thin_free};

fn pool(lv_size: u64, data_percent: f32) -> VgRow {
    VgRow {
        vg_name: "vg0".to_owned(),
        vg_extent_size: 4096,
        vg_size: 0,
        vg_free: 0,
        lv_name: "tp0".to_owned(),
        lv_size,
        data_percent,
    }
}

#[test]
fn quarter_full_pool() {
    assert_eq!(thin_free(&pool(1_048_576, 25.5)), 781_189);
}

#[test]
fn edges() {
    assert_eq!(thin_free(&pool(1_048_576, 0.0)), 1_048_576);
    assert_eq!(thin_free(&pool(1_048_576, 100.0)), 0);
    assert_eq!(thin_free(&pool(0, 50.0)), 0);
}

#[test]
fn blockdev_bytes_to_kib_truncating() {
    assert_eq!(blockdev_size("1077936128\n"), Ok(1_052_672));
    assert_eq!(blockdev_size("2047"), Ok(1));
    assert!(blockdev_size("").is_err());
}

#[test]
fn out_of_range_percent_is_clamped() {
    assert_eq!(thin_free(&pool(1_048_576, -5.0)), 1_048_576);
    assert_eq!(thin_free(&pool(1_048_576, 150.0)), 0);
    assert_eq!(thin_free(&pool(1_048_576, f32::INFINITY)), 0);
    assert_eq!(thin_free(&pool(1_048_576, f32::NEG_INFINITY)), 1_048_576);
}

#[test]
fn nan_percent_counts_as_full() {
    assert_eq!(thin_free(&pool(1_048_576, f32::NAN)), 0);
}
