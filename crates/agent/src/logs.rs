//! Log capture for tests.

use std::io::Write;
use std::sync::{Arc, Mutex};

#[derive(Clone, Default)]
pub(crate) struct Captured(Arc<Mutex<Vec<u8>>>);

impl Captured {
    /// Lines like `INFO agent::reconcile: message key=value`.
    pub(crate) fn lines(&self) -> Vec<String> {
        let buf = self.0.lock().unwrap();
        String::from_utf8_lossy(&buf)
            .lines()
            .map(|l| l.trim().to_owned())
            .collect()
    }

    /// Lines at `level` holding `text`.
    pub(crate) fn find(&self, level: &str, text: &str) -> Vec<String> {
        self.lines()
            .into_iter()
            .filter(|l| l.starts_with(level) && l.contains(text))
            .collect()
    }
}

impl Write for Captured {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Captures this thread's events until the guard drops; a current-thread
/// runtime keeps every await of the test on it.
pub(crate) fn capture() -> (Captured, tracing::subscriber::DefaultGuard) {
    let c = Captured::default();
    let w = c.clone();
    let sub = tracing_subscriber::fmt()
        .with_writer(move || w.clone())
        .with_ansi(false)
        .without_time()
        .with_max_level(tracing::Level::DEBUG)
        .finish();
    (c, tracing::subscriber::set_default(sub))
}
