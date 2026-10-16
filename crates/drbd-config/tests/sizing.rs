use drbd_config::{Error, MetaParams, gross_size_kib, net_size_kib};

#[test]
fn sizing_worked_example() {
    let p = MetaParams::default();
    assert_eq!(gross_size_kib(1_048_576, p), Ok(1_048_840));
    let extent = 4096;
    let storage = 1_048_840_u64.div_ceil(extent) * extent;
    assert_eq!(storage, 1_052_672);
    assert_eq!(net_size_kib(storage, p), Ok(1_052_408));
}

#[test]
fn sizing_net_of_own_gross_covers_net() {
    let p = MetaParams::default();
    for n in [4, 100, 4096, 1_000_003, 10_485_760, 1 << 30] {
        let g = gross_size_kib(n, p).unwrap();
        assert!(net_size_kib(g, p).unwrap() >= n, "n={n} g={g}");
    }
}

#[test]
fn sizing_bitmap_block_64_at_1_gib() {
    // N 1048576, P 7, B 64, AL 32:
    // g = 1048576 + 32 + 4 = 1048612
    // pb = align8(ceil(1048612 / 512)) = align8(2049) = 2056; cover = 2056 * 512 = 1052672 >= g
    // bm = align4(ceil(2056 * 7 / 1024)) = align4(ceil(14.05)) = 16
    // g = 1048576 + align4(32 + 4 + 16) = 1048628; still covered, loop ends
    // MD = align4(16 + 32 + 4) = 52; gross = align64(1048576) + 52 = 1048628
    let p = MetaParams {
        bitmap_block_kib: 64,
        ..MetaParams::default()
    };
    assert_eq!(gross_size_kib(1_048_576, p), Ok(1_048_628));
    // Net of the same gross: aG 1048628, pb align8(ceil(1048628 / 512)) = 2056, bm 16, MD 52,
    // net = floor4(1048628) - 52 = 1048576.
    assert_eq!(net_size_kib(1_048_628, p), Ok(1_048_576));
}

#[test]
fn sizing_minimum_68() {
    let p = MetaParams::default();
    assert_eq!(gross_size_kib(4, p), Ok(68));
    assert_eq!(gross_size_kib(8, p), Ok(68));
    assert!(matches!(gross_size_kib(3, p), Err(Error::SizeRange(_))));
    assert!(matches!(gross_size_kib(0, p), Err(Error::SizeRange(_))));
}

#[test]
fn sizing_bitmap_block_validation() {
    for bad in [0, 1, 2, 3, 5, 6, 12, 2048, 1023, u64::MAX] {
        let p = MetaParams {
            bitmap_block_kib: bad,
            ..MetaParams::default()
        };
        assert_eq!(
            gross_size_kib(1_048_576, p),
            Err(Error::BitmapBlockSize(bad))
        );
        assert_eq!(net_size_kib(1_052_672, p), Err(Error::BitmapBlockSize(bad)));
    }
    for good in [4, 8, 16, 64, 512, 1024] {
        let p = MetaParams {
            bitmap_block_kib: good,
            ..MetaParams::default()
        };
        let g = gross_size_kib(1_048_576, p).unwrap();
        assert_eq!(g % 4, 0);
        assert!(net_size_kib(g, p).unwrap() >= 1_048_576);
    }
}

#[test]
fn sizing_range() {
    let p = MetaParams::default();
    assert!(matches!(
        gross_size_kib(u64::MAX, p),
        Err(Error::SizeRange(_))
    ));
    assert!(matches!(
        gross_size_kib(1 << 40, p),
        Err(Error::SizeRange(_))
    ));
    assert!(gross_size_kib((1 << 40) - (1 << 30), p).is_ok());
    assert!(matches!(
        net_size_kib(u64::MAX, p),
        Err(Error::SizeRange(_))
    ));
    assert!(matches!(net_size_kib(0, p), Err(Error::SizeRange(_))));
    assert!(matches!(net_size_kib(36, p), Err(Error::SizeRange(_))));
    let huge_al = MetaParams {
        al_stripes: u32::MAX,
        al_stripe_size_kib: u64::MAX,
        ..p
    };
    assert!(matches!(
        gross_size_kib(1_048_576, huge_al),
        Err(Error::SizeRange(_))
    ));
    assert!(matches!(
        net_size_kib(1_052_672, huge_al),
        Err(Error::SizeRange(_))
    ));
}
