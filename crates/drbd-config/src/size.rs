//! DRBD 9 internal metadata sits at the end of the backing device:
//! a 4 KiB superblock, a 32 KiB activity log (one stripe of the default
//! size) and a bitmap with one bit per 4 KiB of the device for each peer
//! slot, the whole bitmap rounded up to 4 KiB.

const BLOCK: u64 = 4096;
const FIXED: u64 = BLOCK + 32 * 1024;
/// Bytes of device covered by one 64-bit bitmap word.
const WORD_SPAN: u64 = BLOCK * 64;

/// Metadata bytes for a backing device of `gross` bytes. The bitmap is
/// counted over the whole device in whole 64-bit words per slot, which is
/// at least what DRBD allocates.
pub fn metadata_bytes(gross: u64, peer_slots: u8) -> u64 {
    let per_slot = gross.div_ceil(WORD_SPAN) * 8;
    FIXED + (u64::from(peer_slots) * per_slot).next_multiple_of(BLOCK)
}

/// What DRBD exposes on a backing device of `gross` bytes.
pub fn usable_bytes(gross: u64, peer_slots: u8) -> u64 {
    gross.saturating_sub(metadata_bytes(gross, peer_slots))
}

/// Smallest backing size, give or take one bitmap block, whose usable size
/// is at least `net`, in closed form:
/// with b bitmap blocks and G = net + FIXED + 4096 b, the bitmap fits when
/// P (G / 32768 + 8) <= 4096 b, which solves to
/// b >= P (net + FIXED + 262144) / (128 MiB - 4096 P).
pub fn gross_bytes(net: u64, peer_slots: u8) -> u64 {
    let net = net.next_multiple_of(BLOCK);
    let p = u64::from(peer_slots);
    if p == 0 {
        return net + FIXED;
    }
    // 32 slots at most, so the divisor stays near 128 MiB.
    let blocks = (p * (net + FIXED + 8 * 32768)).div_ceil(32768 * BLOCK - BLOCK * p);
    net + FIXED + BLOCK * blocks
}
