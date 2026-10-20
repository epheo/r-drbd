use drbd_config::{gross_bytes, metadata_bytes, usable_bytes};

const KIB: u64 = 1024;
const MIB: u64 = 1024 * KIB;
const GIB: u64 = 1024 * MIB;

/// Sizes create-md reported on a test host (bitmap size printed by
/// drbdmeta, usable size from the kernel log).
#[test]
fn matches_create_md() {
    assert_eq!(metadata_bytes(GIB, 3), 132 * KIB);
    assert_eq!(usable_bytes(GIB, 3), 1_048_444 * KIB);
    assert_eq!(metadata_bytes(132 * MIB, 1), 36 * KIB + 8 * KIB);
    assert_eq!(metadata_bytes(132 * MIB, 3), 36 * KIB + 16 * KIB);
}

/// The smallest gross size whose usable size reaches `net`, by search.
fn smallest(net: u64, p: u8) -> u64 {
    let mut g = net.next_multiple_of(4096);
    while usable_bytes(g, p) < net {
        g += 4096;
    }
    g
}

#[test]
fn closed_form_is_enough_and_tight() {
    let nets = [
        1,
        4096,
        MIB,
        128 * MIB - 40 * KIB,
        128 * MIB,
        128 * MIB + 1,
        GIB,
        10 * GIB + 12345,
        TIB_ISH,
    ];
    for p in [1u8, 2, 3, 7, 31] {
        for net in nets {
            let g = gross_bytes(net, p);
            assert_eq!(g % 4096, 0);
            assert!(usable_bytes(g, p) >= net, "net {net} p {p}");
            let s = smallest(net, p);
            assert!(g - s <= 4096, "net {net} p {p}: {g} vs {s}");
        }
    }
}

const TIB_ISH: u64 = 64 * GIB + 7;

#[test]
fn sweep_around_bitmap_block_edges() {
    // One bitmap block per slot covers 128 MiB.
    for k in 1..40u64 {
        for d in [0, 4096, 36 * KIB, 64 * KIB] {
            let net = (k * 128 * MIB).saturating_sub(d);
            let g = gross_bytes(net, 3);
            assert!(usable_bytes(g, 3) >= net);
            assert!(g - smallest(net, 3) <= 4096);
        }
    }
}

/// The agent rounds the gross size up to the VG extent; the device must
/// still reach the requested size, or a fresh volume never becomes Ready
/// (Ready waits for usable >= requested). Six peer slots as the agent uses.
#[test]
fn extent_rounded_backing_reaches_every_requested_size() {
    const EXTENT: u64 = 4 * MIB;
    const SLOTS: u8 = 6;
    let mut nets = vec![];
    let mut net = 64 * MIB;
    while net < 4 * 1024 * GIB {
        nets.push(net);
        net = net * 3 / 2;
    }
    nets.push(4 * 1024 * GIB);
    for net in nets {
        for n in [net - 1, net, net + 1, net + 4096, net + 3 * MIB + 12345] {
            let backing = gross_bytes(n, SLOTS).next_multiple_of(EXTENT);
            assert!(
                usable_bytes(backing, SLOTS) >= n,
                "requested {n}: backing {backing} gives {}",
                usable_bytes(backing, SLOTS)
            );
        }
    }
}
