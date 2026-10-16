# lvm

Pure crate: argument vectors for the thin LVM commands the agent runs, parsers for their reports, LV naming.
Thin paths only; thick LVM is out of scope.

Tests under tests/:
- argv_exact: every argv builder keeps the exact argument order, with and without a PV filter, including the `--units k` position difference between lvs and vgs.
- filter: PvFilter from an empty and a two-PV list matches the expected string.
- parse_lvs: an 11-field sample with empty pool_lv, data_percent and origin; comma decimals accepted; wrong field counts ignored; parse errors carry the expected messages.
- parse_vgs: the 7-field thin sample; empty data_percent reads as 100.
- thin_free: 1048576 KiB pool at 25.5 percent gives 781189.

Dependencies allowed: thiserror only.
