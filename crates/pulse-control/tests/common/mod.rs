//! Pulse has no mailer: verification, invite and reset links are written to the node log.
//! Tests read them from there, the way an operator would.

use std::io;
use std::sync::{Arc, Mutex};

#[derive(Clone, Default)]
pub struct Logs(Arc<Mutex<Vec<u8>>>);

impl io::Write for Logs {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Logs {
    /// The token of the most recent `/#/<kind>/<token>` link in the log.
    pub fn last_token(&self, kind: &str) -> String {
        let text = String::from_utf8_lossy(&self.0.lock().unwrap()).to_string();
        let marker = format!("/#/{kind}/");
        let tail = text.rsplit(marker.as_str()).next().filter(|t| t.len() < text.len());
        tail.expect("no such link in the log")
            .chars()
            .take_while(|c| c.is_ascii_hexdigit())
            .collect()
    }
}

/// Captures this thread's log lines while the guard lives (`#[tokio::test]` runs on one thread).
pub fn capture_logs() -> (Logs, tracing::subscriber::DefaultGuard) {
    let logs = Logs::default();
    let writer = logs.clone();
    let sub = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_writer(move || writer.clone())
        .finish();
    (logs, tracing::subscriber::set_default(sub))
}
