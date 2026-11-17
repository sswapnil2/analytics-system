//! Per-query budgets and admission control.
//!
//! Each top-level query runs inside a [`QueryCtx`] (a tokio task-local) that carries its deadline
//! and a row budget. CPU phases run on rayon inside `block_in_place`, where a `tokio::time::timeout`
//! is never polled, so the compute loops check [`Stop::live`] themselves and stop early; the engine
//! then returns `QueryTimeout`. Admission caps concurrent queries per node and per project so one
//! tenant cannot take every core (and every tokio worker, via `block_in_place`) from the others.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::scanner::{ExecError, Result};

/// Rows a single query may scan before it is rejected (`PULSE_QUERY_MAX_ROWS`).
pub const DEFAULT_MAX_ROWS: u64 = 250_000_000;

tokio::task_local! {
    static CTX: Arc<QueryCtx>;
}

#[derive(Debug)]
pub struct QueryCtx {
    deadline: Instant,
    timeout_ms: u64,
    max_rows: u64,
    rows: AtomicU64,
    stopped: AtomicBool,
}

impl QueryCtx {
    pub fn new(timeout: Duration, max_rows: u64) -> Self {
        Self {
            deadline: Instant::now() + timeout,
            timeout_ms: timeout.as_millis() as u64,
            max_rows,
            rows: AtomicU64::new(0),
            stopped: AtomicBool::new(false),
        }
    }

    /// True once the deadline has passed; sticky so later checks skip the clock read.
    pub fn expired(&self) -> bool {
        if self.stopped.load(Ordering::Relaxed) {
            return true;
        }
        if Instant::now() >= self.deadline {
            self.stopped.store(true, Ordering::Relaxed);
            return true;
        }
        false
    }

    pub fn check(&self) -> Result<()> {
        if self.expired() {
            Err(ExecError::QueryTimeout(self.timeout_ms))
        } else {
            Ok(())
        }
    }

    /// Adds `n` rows to the query's total and fails once it passes the budget.
    pub fn charge_rows(&self, n: u64) -> Result<()> {
        let total = self.rows.fetch_add(n, Ordering::Relaxed) + n;
        if total > self.max_rows {
            return Err(ExecError::QueryTooLarge {
                rows: total,
                limit: self.max_rows,
            });
        }
        Ok(())
    }
}

/// The running query's context, if any (none in direct engine calls from unit tests).
pub fn current() -> Option<Arc<QueryCtx>> {
    CTX.try_with(Arc::clone).ok()
}

/// Runs `fut` inside `ctx`.
pub async fn scope<F: std::future::Future>(ctx: Arc<QueryCtx>, fut: F) -> F::Output {
    CTX.scope(ctx, fut).await
}

/// Cheap, `Send + Sync` stop check for rayon closures (task-locals are not visible on rayon threads).
#[derive(Clone, Default)]
pub struct Stop(Option<Arc<QueryCtx>>);

impl Stop {
    pub fn current() -> Self {
        Self(current())
    }

    #[inline]
    pub fn live(&self) -> bool {
        self.0.as_ref().is_none_or(|c| !c.expired())
    }

    /// Like [`Stop::live`] but reads the clock only every 1024 calls, for per-row and per-user loops.
    #[inline]
    pub fn live_every(&self, i: usize) -> bool {
        i & 1023 != 0 || self.live()
    }
}

/// Node-wide and per-project caps on concurrently running queries.
pub struct Admission {
    global: Arc<Semaphore>,
    per_project_limit: usize,
    projects: DashMap<String, Arc<Semaphore>>,
}

pub struct Permits {
    _project: OwnedSemaphorePermit,
    _global: OwnedSemaphorePermit,
}

impl Admission {
    pub fn new(global_limit: usize, per_project_limit: usize) -> Self {
        let global_limit = global_limit.max(1);
        Self {
            global: Arc::new(Semaphore::new(global_limit)),
            per_project_limit: per_project_limit.clamp(1, global_limit),
            projects: DashMap::new(),
        }
    }

    /// Defaults: half the cores node-wide, so queries (which hold a tokio worker through
    /// `block_in_place`) always leave workers for ingest; one project gets at most three quarters
    /// of that, so another tenant can still run. Override with `PULSE_QUERY_MAX_CONCURRENT` and
    /// `PULSE_QUERY_MAX_CONCURRENT_PER_PROJECT`.
    pub fn from_env() -> Self {
        let env = |k: &str| {
            std::env::var(k)
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .filter(|&n| n > 0)
        };
        let cores = std::thread::available_parallelism().map_or(2, |n| n.get());
        let global = env("PULSE_QUERY_MAX_CONCURRENT").unwrap_or((cores / 2).max(1));
        let per_project =
            env("PULSE_QUERY_MAX_CONCURRENT_PER_PROJECT").unwrap_or((global - global / 4).max(1));
        Self::new(global, per_project)
    }

    /// Waits up to `wait` for a slot; `Overloaded` if none frees up. The project slot is taken
    /// first so a tenant's queued queries never hold node-wide slots while they wait.
    pub async fn acquire(&self, project_id: &str, wait: Duration) -> Result<Permits> {
        // ponytail: one semaphore per project ever queried, never evicted; fine up to ~1M projects.
        let project = self
            .projects
            .entry(project_id.to_string())
            .or_insert_with(|| Arc::new(Semaphore::new(self.per_project_limit)))
            .clone();
        let overloaded = |scope: &'static str| ExecError::Overloaded(scope);
        let project = tokio::time::timeout(wait, project.acquire_owned())
            .await
            .map_err(|_| overloaded("project"))?
            .map_err(|_| overloaded("project"))?;
        let global = tokio::time::timeout(wait, self.global.clone().acquire_owned())
            .await
            .map_err(|_| overloaded("node"))?
            .map_err(|_| overloaded("node"))?;
        Ok(Permits {
            _project: project,
            _global: global,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_budget_and_deadline() {
        let ctx = QueryCtx::new(Duration::from_secs(60), 100);
        assert!(ctx.charge_rows(60).is_ok());
        assert!(matches!(
            ctx.charge_rows(60),
            Err(ExecError::QueryTooLarge {
                rows: 120,
                limit: 100
            })
        ));
        assert!(ctx.check().is_ok());

        let expired = Arc::new(QueryCtx::new(Duration::ZERO, 100));
        assert!(matches!(expired.check(), Err(ExecError::QueryTimeout(0))));
        assert!(!Stop(Some(expired)).live());
        assert!(Stop::default().live());
    }

    #[tokio::test]
    async fn one_project_cannot_take_every_slot() {
        let adm = Admission::new(2, 1);
        let wait = Duration::from_millis(20);
        let _a = adm.acquire("p1", wait).await.unwrap();
        // p1 is at its own cap, but p2 still gets the second node slot.
        assert!(matches!(
            adm.acquire("p1", wait).await,
            Err(ExecError::Overloaded("project"))
        ));
        let _b = adm.acquire("p2", wait).await.unwrap();
        // Node is full now.
        assert!(matches!(
            adm.acquire("p3", wait).await,
            Err(ExecError::Overloaded("node"))
        ));
        drop(_a);
        assert!(adm.acquire("p1", wait).await.is_ok());
    }
}
