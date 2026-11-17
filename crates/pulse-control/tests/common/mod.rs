//! Pulse has no mailer: verification and reset links go to the owner-only `outbox` file in the
//! data directory, never the log. Tests read them from there, the way an operator would, and
//! capture the log to check no link leaks into it.

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
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap()).to_string()
    }
}

/// The outbox file a test's control plane writes links to (`setup_app` points it here).
pub struct Outbox(pub std::path::PathBuf);

impl Outbox {
    pub fn at(data_dir: &std::path::Path) -> Self {
        Outbox(data_dir.join("outbox"))
    }

    pub fn text(&self) -> String {
        std::fs::read_to_string(&self.0).unwrap_or_default()
    }

    /// The token of the most recent `/#/<kind>/<token>` link in the outbox.
    pub fn last_token(&self, kind: &str) -> String {
        let text = self.text();
        let marker = format!("/#/{kind}/");
        let tail = text
            .rsplit(marker.as_str())
            .next()
            .filter(|t| t.len() < text.len());
        tail.expect("no such link in the outbox")
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
