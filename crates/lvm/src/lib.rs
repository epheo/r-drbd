//! Thin LVM for the agent: argv for every command it runs, parsers for the
//! reports, LV names. No I/O; the agent runs the argv.

pub mod cmd;
pub mod filter;
pub mod names;
pub mod report;
