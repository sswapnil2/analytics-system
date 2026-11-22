//! MetaStore persistence & query layer for AI Agent Observability & Evaluation Control-Plane.
//!
//! SQL (`MetaStore`: SQLite WAL & PostgreSQL via `pg_rewrite`) is strictly reserved for
//! control-plane configuration, post-hoc evaluations & rollups, datasets, experiments,
//! alerts, privacy policies, pricing overrides, and 3-level erasure tombstones.
//! Trace and span payloads live exclusively in `AgentTraceStore` (`PTWL`/`PLSS`).

use std::collections::{BTreeSet, HashMap};

use chrono::Utc;
use pulse_agent::{
    AgentAlertEvent, AgentAlertRule, AgentDataset, AgentDatasetItem, AgentErasureTombstone,
    AgentEvalRollup, AgentEvaluation, AgentExperiment, AgentExperimentRun, AgentPrivacyPolicy,
    ErasureLevel, EvalTargetType, EvaluatorConfig, EvaluatorType, ModelPricingRule, ScoreType,
};
use serde_json::Value;
use sqlx::Row;

use crate::db::{pg_rewrite, MetaError, MetaStore};

const IN_CHUNK_SIZE: usize = 400;

macro_rules! a_exec {
    ($store:expr, $sql:expr $(, $arg:expr)* $(,)?) => {{
        match $store {
            MetaStore::Sqlite(pool) => {
                sqlx::query($sql)$(.bind($arg))*.execute(pool).await?.rows_affected()
            }
            MetaStore::Postgres(pool) => {
                let pg = pg_rewrite($sql);
                sqlx::query(&pg)$(.bind($arg))*.execute(pool).await?.rows_affected()
            }
        }
    }};
}

macro_rules! a_map_all {
    ($store:expr, $sql:expr, |$row:ident| $body:block $(, $arg:expr)* $(,)?) => {{
        match $store {
            MetaStore::Sqlite(pool) => {
                let rows = sqlx::query($sql)$(.bind($arg))*.fetch_all(pool).await?;
                rows.into_iter().map(|$row| $body).collect::<Vec<_>>()
            }
            MetaStore::Postgres(pool) => {
                let pg = pg_rewrite($sql);
                let rows = sqlx::query(&pg)$(.bind($arg))*.fetch_all(pool).await?;
                rows.into_iter().map(|$row| $body).collect::<Vec<_>>()
            }
        }
    }};
}

macro_rules! a_map_opt {
    ($store:expr, $sql:expr, |$row:ident| $body:block $(, $arg:expr)* $(,)?) => {{
        match $store {
            MetaStore::Sqlite(pool) => {
                let opt = sqlx::query($sql)$(.bind($arg))*.fetch_optional(pool).await?;
                opt.map(|$row| $body)
            }
            MetaStore::Postgres(pool) => {
                let pg = pg_rewrite($sql);
                let opt = sqlx::query(&pg)$(.bind($arg))*.fetch_optional(pool).await?;
                opt.map(|$row| $body)
            }
        }
    }};
}

macro_rules! tx_exec {
    ($tx:ident, $is_pg:ident, $sql:expr $(, $arg:expr)* $(,)?) => {{
        let pg_owned;
        let sql_ref: &str = if $is_pg {
            pg_owned = pg_rewrite($sql);
            pg_owned.as_str()
        } else {
            $sql
        };
        sqlx::query(sql_ref)$(.bind($arg))*.execute(&mut *$tx).await?.rows_affected()
    }};
}

macro_rules! tx_fetch_all {
    ($tx:ident, $is_pg:ident, $sql:expr, |$row:ident| $body:block $(, $arg:expr)* $(,)?) => {{
        let pg_owned;
        let sql_ref: &str = if $is_pg {
            pg_owned = pg_rewrite($sql);
            pg_owned.as_str()
        } else {
            $sql
        };
        let rows = sqlx::query(sql_ref)$(.bind($arg))*.fetch_all(&mut *$tx).await?;
        rows.into_iter().map(|$row| $body).collect::<Vec<_>>()
    }};
}

macro_rules! tx_fetch_opt {
    ($tx:ident, $is_pg:ident, $sql:expr, |$row:ident| $body:block $(, $arg:expr)* $(,)?) => {{
        let pg_owned;
        let sql_ref: &str = if $is_pg {
            pg_owned = pg_rewrite($sql);
            pg_owned.as_str()
        } else {
            $sql
        };
        let opt = sqlx::query(sql_ref)$(.bind($arg))*.fetch_optional(&mut *$tx).await?;
        opt.map(|$row| $body)
    }};
}

macro_rules! run_in_tx {
    ($store:expr, |$tx:ident, $is_pg:ident| $body:block) => {{
        match $store {
            MetaStore::Sqlite(pool) => {
                let mut $tx = pool.begin().await?;
                let $is_pg = false;
                let res: Result<_, MetaError> = $body;
                let val = res?;
                $tx.commit().await?;
                Ok(val)
            }
            MetaStore::Postgres(pool) => {
                let mut $tx = pool.begin().await?;
                let $is_pg = true;
                let res: Result<_, MetaError> = $body;
                let val = res?;
                $tx.commit().await?;
                Ok(val)
            }
        }
    }};
}

macro_rules! eval_from_row {
    ($r:ident) => {{
        let tt_str: String = $r.get("target_type");
        let et_str: String = $r.get("evaluator_type");
        let st_str: String = $r.get("score_type");
        let passed_i: Option<i32> = $r.get("passed");
        let meta_str: String = $r.get("metadata_json");
        AgentEvaluation {
            id: $r.get("id"),
            project_id: $r.get("project_id"),
            target_type: EvalTargetType::parse(&tt_str),
            session_id: $r.get("session_id"),
            trace_id: $r.get("trace_id"),
            span_id: $r.get("span_id"),
            experiment_run_id: $r.get("experiment_run_id"),
            evaluator_id: $r.get("evaluator_id"),
            evaluator_name: $r.get("evaluator_name"),
            evaluator_type: EvaluatorType::parse(&et_str),
            evaluator_version: $r.get("evaluator_version"),
            score: $r.get("score"),
            score_type: ScoreType::parse(&st_str),
            passed: passed_i.map(|v| v != 0),
            label: $r.get("label"),
            reasoning: $r.get("reasoning"),
            execution_ms: $r.get("execution_ms"),
            error: $r.get("error"),
            metadata: serde_json::from_str(&meta_str).unwrap_or(Value::Null),
            created_at_ms: $r.get("created_at_ms"),
        }
    }};
}

macro_rules! rollup_from_row {
    ($r:ident) => {{
        let tt_str: String = $r.get("target_type");
        AgentEvalRollup {
            project_id: $r.get("project_id"),
            target_type: EvalTargetType::parse(&tt_str),
            target_id: $r.get("target_id"),
            avg_score: $r.get("avg_score"),
            pass_rate: $r.get("pass_rate"),
            eval_count: $r.get("eval_count"),
            updated_at: $r.get("updated_at_ms"),
        }
    }};
}

macro_rules! evaluator_cfg_from_row {
    ($r:ident) => {{
        let tt: String = $r.get("target_type");
        let et: String = $r.get("evaluator_type");
        let cfg_str: String = $r.get("config_json");
        EvaluatorConfig {
            id: $r.get("id"),
            project_id: $r.get("project_id"),
            name: $r.get("name"),
            description: $r.get("description"),
            target_type: EvalTargetType::parse(&tt),
            evaluator_type: EvaluatorType::parse(&et),
            kind: $r.get("kind"),
            version: $r.get("version"),
            enabled: $r.get::<i32, _>("enabled") != 0,
            agent_name: $r.get("agent_name"),
            environment: $r.get("environment"),
            sample_rate: $r.get("sample_rate"),
            config: serde_json::from_str(&cfg_str).unwrap_or(Value::Null),
            created_at_ms: $r.get("created_at_ms"),
            updated_at_ms: $r.get("updated_at_ms"),
        }
    }};
}

macro_rules! dataset_item_from_row {
    ($r:ident) => {{
        let input_s: String = $r.get("input_json");
        let out_s: String = $r.get("expected_output_json");
        let beh_s: String = $r.get("expected_behavior_json");
        let meta_s: String = $r.get("metadata_json");
        let tags_s: String = $r.get("tags_json");
        AgentDatasetItem {
            id: $r.get("id"),
            project_id: $r.get("project_id"),
            dataset_id: $r.get("dataset_id"),
            dataset_version: $r.get("dataset_version"),
            name: $r.get("name"),
            input: serde_json::from_str(&input_s).unwrap_or(Value::Null),
            expected_output: serde_json::from_str(&out_s).unwrap_or(Value::Null),
            expected_behavior: serde_json::from_str(&beh_s).unwrap_or(Value::Null),
            source_trace_id: $r.get("source_trace_id"),
            metadata: serde_json::from_str(&meta_s).unwrap_or(Value::Null),
            tags: serde_json::from_str(&tags_s).unwrap_or_default(),
            created_at_ms: $r.get("created_at_ms"),
            updated_at_ms: $r.get("updated_at_ms"),
        }
    }};
}

macro_rules! experiment_from_row {
    ($r:ident) => {{
        let eids_s: String = $r.get("evaluator_ids_json");
        let meta_s: String = $r.get("metadata_json");
        AgentExperiment {
            id: $r.get("id"),
            project_id: $r.get("project_id"),
            name: $r.get("name"),
            description: $r.get("description"),
            dataset_id: $r.get("dataset_id"),
            dataset_version: $r.get("dataset_version"),
            agent_name: $r.get("agent_name"),
            agent_version: $r.get("agent_version"),
            prompt_version: $r.get("prompt_version"),
            model: $r.get("model"),
            evaluator_ids: serde_json::from_str(&eids_s).unwrap_or_default(),
            status: $r.get("status"),
            total_items: $r.get("total_items"),
            completed_items: $r.get("completed_items"),
            passed_items: $r.get("passed_items"),
            avg_score: $r.get("avg_score"),
            avg_latency_ms: $r.get("avg_latency_ms"),
            total_cost_usd: $r.get("total_cost_usd"),
            metadata: serde_json::from_str(&meta_s).unwrap_or(Value::Null),
            created_at_ms: $r.get("created_at_ms"),
            completed_at_ms: $r.get("completed_at_ms"),
        }
    }};
}

macro_rules! alert_rule_from_row {
    ($r:ident) => {{
        AgentAlertRule {
            id: $r.get("id"),
            project_id: $r.get("project_id"),
            name: $r.get("name"),
            agent_name: $r.get("agent_name"),
            environment: $r.get("environment"),
            metric: $r.get("metric"),
            operator: $r.get("operator"),
            threshold: $r.get("threshold"),
            window_minutes: $r.get("window_minutes"),
            cooldown_minutes: $r.get("cooldown_minutes"),
            enabled: $r.get::<i32, _>("enabled") != 0,
            state: $r.get("state"),
            webhook_url: $r.get("webhook_url"),
            last_evaluated_ms: $r.get("last_evaluated_ms"),
            last_triggered_ms: $r.get("last_triggered_ms"),
            last_resolved_ms: $r.get("last_resolved_ms"),
            last_value: $r.get("last_value"),
            created_at_ms: $r.get("created_at_ms"),
        }
    }};
}

macro_rules! tombstone_from_row {
    ($r:ident) => {{
        let lvl_s: String = $r.get("level");
        AgentErasureTombstone {
            id: $r.get("id"),
            project_id: $r.get("project_id"),
            level: ErasureLevel::parse(&lvl_s),
            target_id: $r.get("target_id"),
            created_at: $r.get("created_at_ms"),
            expires_at: $r.get("expires_at_ms"),
        }
    }};
}

macro_rules! recompute_rollup_in_tx {
    ($tx:ident, $is_pg:ident, $project_id:expr, $target_type:expr, $target_id:expr) => {{
        let tt: EvalTargetType = $target_type;
        let tid: &str = $target_id;
        let pid: &str = $project_id;
        if tid.is_empty() {
            None
        } else {
            let agg_sql = match tt {
                EvalTargetType::Session => {
                    "SELECT COUNT(*) AS eval_count, AVG(score) AS avg_score, \
                     COALESCE(SUM(CASE WHEN passed = 1 THEN 1 ELSE 0 END), 0) AS passed_count, \
                     COALESCE(SUM(CASE WHEN passed IS NOT NULL THEN 1 ELSE 0 END), 0) AS graded_count \
                     FROM agent_evaluations \
                     WHERE project_id = ? AND target_type = ? AND session_id = ? AND error IS NULL"
                }
                EvalTargetType::Trace => {
                    "SELECT COUNT(*) AS eval_count, AVG(score) AS avg_score, \
                     COALESCE(SUM(CASE WHEN passed = 1 THEN 1 ELSE 0 END), 0) AS passed_count, \
                     COALESCE(SUM(CASE WHEN passed IS NOT NULL THEN 1 ELSE 0 END), 0) AS graded_count \
                     FROM agent_evaluations \
                     WHERE project_id = ? AND target_type = ? AND trace_id = ? AND error IS NULL"
                }
                EvalTargetType::Span => {
                    "SELECT COUNT(*) AS eval_count, AVG(score) AS avg_score, \
                     COALESCE(SUM(CASE WHEN passed = 1 THEN 1 ELSE 0 END), 0) AS passed_count, \
                     COALESCE(SUM(CASE WHEN passed IS NOT NULL THEN 1 ELSE 0 END), 0) AS graded_count \
                     FROM agent_evaluations \
                     WHERE project_id = ? AND target_type = ? AND span_id = ? AND error IS NULL"
                }
            };
            let tt_str = tt.as_str();
            let stats = tx_fetch_opt!(
                $tx,
                $is_pg,
                agg_sql,
                |r| {
                    let count: i64 = r.get("eval_count");
                    let avg_score: Option<f64> = r.get("avg_score");
                    let passed_count: i64 = r.get("passed_count");
                    let graded_count: i64 = r.get("graded_count");
                    (count, avg_score, passed_count, graded_count)
                },
                pid,
                tt_str,
                tid
            );
            match stats {
                Some((count, Some(avg_score), passed_count, graded_count)) if count > 0 => {
                    let pass_rate = (graded_count > 0)
                        .then(|| passed_count as f64 / graded_count as f64);
                    let updated_at = Utc::now().timestamp_millis();
                    let upsert_sql = r#"
                        INSERT INTO agent_eval_rollups (
                            project_id, target_type, target_id, avg_score, pass_rate, eval_count, updated_at_ms
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(project_id, target_type, target_id) DO UPDATE SET
                            avg_score = excluded.avg_score,
                            pass_rate = excluded.pass_rate,
                            eval_count = excluded.eval_count,
                            updated_at_ms = excluded.updated_at_ms
                    "#;
                    tx_exec!(
                        $tx,
                        $is_pg,
                        upsert_sql,
                        pid,
                        tt_str,
                        tid,
                        avg_score,
                        pass_rate,
                        count,
                        updated_at
                    );
                    Some(AgentEvalRollup {
                        project_id: pid.to_string(),
                        target_type: tt,
                        target_id: tid.to_string(),
                        avg_score,
                        pass_rate,
                        eval_count: count,
                        updated_at,
                    })
                }
                _ => {
                    tx_exec!(
                        $tx,
                        $is_pg,
                        "DELETE FROM agent_eval_rollups WHERE project_id = ? AND target_type = ? AND target_id = ?",
                        pid,
                        tt_str,
                        tid
                    );
                    None
                }
            }
        }
    }};
}

fn eval_target_id(e: &AgentEvaluation) -> Option<&str> {
    match e.target_type {
        EvalTargetType::Session => (!e.session_id.is_empty()).then_some(e.session_id.as_str()),
        EvalTargetType::Trace => e.trace_id.as_deref().filter(|s| !s.is_empty()),
        EvalTargetType::Span => e.span_id.as_deref().filter(|s| !s.is_empty()),
    }
}

fn collect_affected_targets(
    rows: &[(String, String, Option<String>, Option<String>)],
) -> BTreeSet<(String, String)> {
    let mut targets = BTreeSet::new();
    for (tt_str, session_id, trace_id, span_id) in rows {
        let tt = EvalTargetType::parse(tt_str);
        let tid_opt = match tt {
            EvalTargetType::Session => (!session_id.is_empty()).then_some(session_id.as_str()),
            EvalTargetType::Trace => trace_id.as_deref().filter(|s| !s.is_empty()),
            EvalTargetType::Span => span_id.as_deref().filter(|s| !s.is_empty()),
        };
        if let Some(tid) = tid_opt {
            targets.insert((tt.as_str().to_string(), tid.to_string()));
        }
    }
    targets
}

impl MetaStore {
    // -----------------------------------------------------------------------
    // Multi-Level Evaluations & Eval Rollups
    // -----------------------------------------------------------------------

    /// Inserts an evaluation and atomically recomputes `agent_eval_rollups` for its target.
    pub async fn insert_agent_evaluation(&self, e: &AgentEvaluation) -> Result<(), MetaError> {
        let tt = e.target_type.as_str();
        let et = e.evaluator_type.as_str();
        let st = e.score_type.as_str();
        let passed = e.passed.map(|b| if b { 1i32 } else { 0i32 });
        let meta = serde_json::to_string(&e.metadata).unwrap_or_else(|_| "null".into());
        let target_id = eval_target_id(e).map(str::to_string);

        run_in_tx!(self, |tx, is_pg| {
            let affected = tx_exec!(
                tx,
                is_pg,
                r#"
                INSERT INTO agent_evaluations (
                    id, project_id, target_type, session_id, trace_id, span_id, experiment_run_id,
                    evaluator_id, evaluator_name, evaluator_type, evaluator_version, score,
                    score_type, passed, label, reasoning, execution_ms, error, metadata_json,
                    created_at_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO NOTHING
                "#,
                &e.id,
                &e.project_id,
                tt,
                &e.session_id,
                &e.trace_id,
                &e.span_id,
                &e.experiment_run_id,
                &e.evaluator_id,
                &e.evaluator_name,
                et,
                &e.evaluator_version,
                e.score,
                st,
                passed,
                &e.label,
                &e.reasoning,
                e.execution_ms,
                &e.error,
                &meta,
                e.created_at_ms
            );
            if affected == 0 {
                return Err(MetaError::Conflict(format!(
                    "agent evaluation '{}' already exists",
                    e.id
                )));
            }

            if let Some(tid) = target_id.as_deref() {
                recompute_rollup_in_tx!(tx, is_pg, &e.project_id, e.target_type, tid);
            }
            Ok(())
        })
    }

    pub async fn recompute_eval_rollup(
        &self,
        project_id: &str,
        target_type: EvalTargetType,
        target_id: &str,
    ) -> Result<Option<AgentEvalRollup>, MetaError> {
        run_in_tx!(self, |tx, is_pg| {
            let res = recompute_rollup_in_tx!(tx, is_pg, project_id, target_type, target_id);
            Ok(res)
        })
    }

    pub async fn upsert_agent_eval_rollup(&self, r: &AgentEvalRollup) -> Result<(), MetaError> {
        let tt = r.target_type.as_str();
        a_exec!(
            self,
            r#"
            INSERT INTO agent_eval_rollups (
                project_id, target_type, target_id, avg_score, pass_rate, eval_count, updated_at_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(project_id, target_type, target_id) DO UPDATE SET
                avg_score = excluded.avg_score,
                pass_rate = excluded.pass_rate,
                eval_count = excluded.eval_count,
                updated_at_ms = excluded.updated_at_ms
            "#,
            &r.project_id,
            tt,
            &r.target_id,
            r.avg_score,
            r.pass_rate,
            r.eval_count,
            r.updated_at
        );
        Ok(())
    }

    pub async fn list_agent_eval_rollups(
        &self,
        project_id: &str,
        target_type: Option<EvalTargetType>,
    ) -> Result<HashMap<(EvalTargetType, String), AgentEvalRollup>, MetaError> {
        let rows = match target_type {
            Some(tt) => a_map_all!(
                self,
                "SELECT * FROM agent_eval_rollups WHERE project_id = ? AND target_type = ?",
                |r| { rollup_from_row!(r) },
                project_id,
                tt.as_str()
            ),
            None => a_map_all!(
                self,
                "SELECT * FROM agent_eval_rollups WHERE project_id = ?",
                |r| { rollup_from_row!(r) },
                project_id
            ),
        };
        Ok(rows
            .into_iter()
            .map(|r| ((r.target_type, r.target_id.clone()), r))
            .collect())
    }

    pub async fn get_eval_rollups_batch(
        &self,
        project_id: &str,
        target_type: EvalTargetType,
        target_ids: &[String],
    ) -> Result<HashMap<(EvalTargetType, String), AgentEvalRollup>, MetaError> {
        let mut out = HashMap::new();
        if target_ids.is_empty() {
            return Ok(out);
        }
        let tt = target_type.as_str();
        for chunk in target_ids.chunks(IN_CHUNK_SIZE) {
            let placeholders = vec!["?"; chunk.len()].join(", ");
            let sql = format!(
                "SELECT * FROM agent_eval_rollups \
                 WHERE project_id = ? AND target_type = ? AND target_id IN ({placeholders})"
            );
            let rows = match self {
                MetaStore::Sqlite(pool) => {
                    let mut q = sqlx::query(&sql).bind(project_id).bind(tt);
                    for id in chunk {
                        q = q.bind(id);
                    }
                    q.fetch_all(pool)
                        .await?
                        .into_iter()
                        .map(|r| rollup_from_row!(r))
                        .collect::<Vec<_>>()
                }
                MetaStore::Postgres(pool) => {
                    let pg = pg_rewrite(&sql);
                    let mut q = sqlx::query(&pg).bind(project_id).bind(tt);
                    for id in chunk {
                        q = q.bind(id);
                    }
                    q.fetch_all(pool)
                        .await?
                        .into_iter()
                        .map(|r| rollup_from_row!(r))
                        .collect::<Vec<_>>()
                }
            };
            for r in rows {
                out.insert((r.target_type, r.target_id.clone()), r);
            }
        }
        Ok(out)
    }

    pub async fn list_trace_evaluations(
        &self,
        project_id: &str,
        trace_id: &str,
    ) -> Result<Vec<AgentEvaluation>, MetaError> {
        let rows = a_map_all!(
            self,
            "SELECT * FROM agent_evaluations WHERE project_id = ? AND trace_id = ? ORDER BY created_at_ms DESC",
            |r| { eval_from_row!(r) },
            project_id,
            trace_id
        );
        Ok(rows)
    }

    pub async fn list_session_evaluations(
        &self,
        project_id: &str,
        session_id: &str,
    ) -> Result<Vec<AgentEvaluation>, MetaError> {
        let rows = a_map_all!(
            self,
            "SELECT * FROM agent_evaluations WHERE project_id = ? AND session_id = ? ORDER BY created_at_ms DESC",
            |r| { eval_from_row!(r) },
            project_id,
            session_id
        );
        Ok(rows)
    }

    pub async fn list_span_evaluations(
        &self,
        project_id: &str,
        span_id: &str,
    ) -> Result<Vec<AgentEvaluation>, MetaError> {
        let rows = a_map_all!(
            self,
            "SELECT * FROM agent_evaluations WHERE project_id = ? AND span_id = ? ORDER BY created_at_ms DESC",
            |r| { eval_from_row!(r) },
            project_id,
            span_id
        );
        Ok(rows)
    }

    pub async fn list_project_evaluations(
        &self,
        project_id: &str,
        limit: usize,
    ) -> Result<Vec<AgentEvaluation>, MetaError> {
        self.list_project_evaluations_by_target(project_id, None, limit)
            .await
    }

    pub async fn list_project_evaluations_by_target(
        &self,
        project_id: &str,
        target_type: Option<EvalTargetType>,
        limit: usize,
    ) -> Result<Vec<AgentEvaluation>, MetaError> {
        let lim = limit.clamp(1, 500) as i64;
        let rows = match target_type {
            Some(tt) => a_map_all!(
                self,
                "SELECT * FROM agent_evaluations WHERE project_id = ? AND target_type = ? ORDER BY created_at_ms DESC LIMIT ?",
                |r| { eval_from_row!(r) },
                project_id,
                tt.as_str(),
                lim
            ),
            None => a_map_all!(
                self,
                "SELECT * FROM agent_evaluations WHERE project_id = ? ORDER BY created_at_ms DESC LIMIT ?",
                |r| { eval_from_row!(r) },
                project_id,
                lim
            ),
        };
        Ok(rows)
    }

    // -----------------------------------------------------------------------
    // Evaluator Configs
    // -----------------------------------------------------------------------

    pub async fn upsert_evaluator_config(&self, c: &EvaluatorConfig) -> Result<(), MetaError> {
        let tt = c.target_type.as_str();
        let et = c.evaluator_type.as_str();
        let enabled = if c.enabled { 1i32 } else { 0i32 };
        let cfg_json = serde_json::to_string(&c.config).unwrap_or_else(|_| "{}".into());
        let affected = a_exec!(
            self,
            r#"
            INSERT INTO agent_evaluator_configs (
                id, project_id, name, description, target_type, evaluator_type, kind, version,
                enabled, agent_name, environment, sample_rate, config_json,
                created_at_ms, updated_at_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                description = excluded.description,
                target_type = excluded.target_type,
                evaluator_type = excluded.evaluator_type,
                kind = excluded.kind,
                version = excluded.version,
                enabled = excluded.enabled,
                agent_name = excluded.agent_name,
                environment = excluded.environment,
                sample_rate = excluded.sample_rate,
                config_json = excluded.config_json,
                updated_at_ms = excluded.updated_at_ms
            WHERE agent_evaluator_configs.project_id = excluded.project_id
            "#,
            &c.id,
            &c.project_id,
            &c.name,
            &c.description,
            tt,
            et,
            &c.kind,
            &c.version,
            enabled,
            &c.agent_name,
            &c.environment,
            c.sample_rate,
            cfg_json,
            c.created_at_ms,
            c.updated_at_ms
        );
        if affected == 0 {
            return Err(MetaError::Conflict(format!(
                "evaluator config '{}' belongs to a different project",
                c.id
            )));
        }
        Ok(())
    }

    pub async fn get_evaluator_config(
        &self,
        project_id: &str,
        id: &str,
    ) -> Result<Option<EvaluatorConfig>, MetaError> {
        let opt = a_map_opt!(
            self,
            "SELECT * FROM agent_evaluator_configs WHERE project_id = ? AND id = ?",
            |r| { evaluator_cfg_from_row!(r) },
            project_id,
            id
        );
        Ok(opt)
    }

    pub async fn list_evaluator_configs(
        &self,
        project_id: &str,
    ) -> Result<Vec<EvaluatorConfig>, MetaError> {
        let rows = a_map_all!(
            self,
            "SELECT * FROM agent_evaluator_configs WHERE project_id = ? ORDER BY created_at_ms DESC",
            |r| { evaluator_cfg_from_row!(r) },
            project_id
        );
        Ok(rows)
    }

    pub async fn list_enabled_evaluator_configs(
        &self,
        project_id: &str,
    ) -> Result<Vec<EvaluatorConfig>, MetaError> {
        let rows = a_map_all!(
            self,
            "SELECT * FROM agent_evaluator_configs WHERE project_id = ? AND enabled = 1 ORDER BY created_at_ms DESC",
            |r| { evaluator_cfg_from_row!(r) },
            project_id
        );
        Ok(rows)
    }

    pub async fn delete_evaluator_config(
        &self,
        project_id: &str,
        id: &str,
    ) -> Result<bool, MetaError> {
        let n = a_exec!(
            self,
            "DELETE FROM agent_evaluator_configs WHERE project_id = ? AND id = ?",
            project_id,
            id
        );
        Ok(n > 0)
    }

    // -----------------------------------------------------------------------
    // Datasets & Experiments
    // -----------------------------------------------------------------------

    pub async fn upsert_agent_dataset(&self, d: &AgentDataset) -> Result<(), MetaError> {
        let tags_json = serde_json::to_string(&d.tags).unwrap_or_else(|_| "[]".into());
        let affected = a_exec!(
            self,
            r#"
            INSERT INTO agent_datasets (
                id, project_id, name, description, version, tags_json, created_at_ms, updated_at_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                description = excluded.description,
                version = excluded.version,
                tags_json = excluded.tags_json,
                updated_at_ms = excluded.updated_at_ms
            WHERE agent_datasets.project_id = excluded.project_id
            "#,
            &d.id,
            &d.project_id,
            &d.name,
            &d.description,
            d.version,
            tags_json,
            d.created_at_ms,
            d.updated_at_ms
        );
        if affected == 0 {
            return Err(MetaError::Conflict(format!(
                "dataset '{}' belongs to a different project",
                d.id
            )));
        }
        Ok(())
    }

    pub async fn get_agent_dataset(
        &self,
        project_id: &str,
        dataset_id: &str,
    ) -> Result<Option<AgentDataset>, MetaError> {
        let sql = r#"
            SELECT d.*, (SELECT COUNT(*) FROM agent_dataset_items i WHERE i.project_id = d.project_id AND i.dataset_id = d.id) AS item_count
            FROM agent_datasets d
            WHERE d.project_id = ? AND d.id = ?
        "#;
        let opt = a_map_opt!(
            self,
            sql,
            |r| {
                let tags_str: String = r.get("tags_json");
                AgentDataset {
                    id: r.get("id"),
                    project_id: r.get("project_id"),
                    name: r.get("name"),
                    description: r.get("description"),
                    version: r.get("version"),
                    tags: serde_json::from_str(&tags_str).unwrap_or_default(),
                    item_count: r.get("item_count"),
                    created_at_ms: r.get("created_at_ms"),
                    updated_at_ms: r.get("updated_at_ms"),
                }
            },
            project_id,
            dataset_id
        );
        Ok(opt)
    }

    pub async fn list_agent_datasets(
        &self,
        project_id: &str,
    ) -> Result<Vec<AgentDataset>, MetaError> {
        let sql = r#"
            SELECT d.*, (SELECT COUNT(*) FROM agent_dataset_items i WHERE i.project_id = d.project_id AND i.dataset_id = d.id) AS item_count
            FROM agent_datasets d
            WHERE d.project_id = ?
            ORDER BY d.updated_at_ms DESC
        "#;
        let rows = a_map_all!(
            self,
            sql,
            |r| {
                let tags_str: String = r.get("tags_json");
                AgentDataset {
                    id: r.get("id"),
                    project_id: r.get("project_id"),
                    name: r.get("name"),
                    description: r.get("description"),
                    version: r.get("version"),
                    tags: serde_json::from_str(&tags_str).unwrap_or_default(),
                    item_count: r.get("item_count"),
                    created_at_ms: r.get("created_at_ms"),
                    updated_at_ms: r.get("updated_at_ms"),
                }
            },
            project_id
        );
        Ok(rows)
    }

    pub async fn delete_agent_dataset(
        &self,
        project_id: &str,
        dataset_id: &str,
    ) -> Result<bool, MetaError> {
        run_in_tx!(self, |tx, is_pg| {
            tx_exec!(
                tx,
                is_pg,
                "DELETE FROM agent_dataset_items WHERE project_id = ? AND dataset_id = ?",
                project_id,
                dataset_id
            );
            let n = tx_exec!(
                tx,
                is_pg,
                "DELETE FROM agent_datasets WHERE project_id = ? AND id = ?",
                project_id,
                dataset_id
            );
            Ok(n > 0)
        })
    }

    pub async fn upsert_dataset_item(&self, item: &AgentDatasetItem) -> Result<(), MetaError> {
        let input_json = serde_json::to_string(&item.input).unwrap_or_else(|_| "null".into());
        let exp_out =
            serde_json::to_string(&item.expected_output).unwrap_or_else(|_| "null".into());
        let exp_beh =
            serde_json::to_string(&item.expected_behavior).unwrap_or_else(|_| "null".into());
        let meta_json = serde_json::to_string(&item.metadata).unwrap_or_else(|_| "null".into());
        let tags_json = serde_json::to_string(&item.tags).unwrap_or_else(|_| "[]".into());
        let affected = a_exec!(
            self,
            r#"
            INSERT INTO agent_dataset_items (
                id, project_id, dataset_id, dataset_version, name, input_json,
                expected_output_json, expected_behavior_json, source_trace_id,
                metadata_json, tags_json, created_at_ms, updated_at_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                input_json = excluded.input_json,
                expected_output_json = excluded.expected_output_json,
                expected_behavior_json = excluded.expected_behavior_json,
                metadata_json = excluded.metadata_json,
                tags_json = excluded.tags_json,
                updated_at_ms = excluded.updated_at_ms
            WHERE agent_dataset_items.project_id = excluded.project_id
              AND agent_dataset_items.dataset_id = excluded.dataset_id
            "#,
            &item.id,
            &item.project_id,
            &item.dataset_id,
            item.dataset_version,
            &item.name,
            input_json,
            exp_out,
            exp_beh,
            &item.source_trace_id,
            meta_json,
            tags_json,
            item.created_at_ms,
            item.updated_at_ms
        );
        if affected == 0 {
            return Err(MetaError::Conflict(format!(
                "dataset item '{}' belongs to a different project or dataset",
                item.id
            )));
        }
        Ok(())
    }

    pub async fn list_dataset_items(
        &self,
        project_id: &str,
        dataset_id: &str,
    ) -> Result<Vec<AgentDatasetItem>, MetaError> {
        let rows = a_map_all!(
            self,
            "SELECT * FROM agent_dataset_items WHERE project_id = ? AND dataset_id = ? ORDER BY created_at_ms DESC",
            |r| { dataset_item_from_row!(r) },
            project_id,
            dataset_id
        );
        Ok(rows)
    }

    pub async fn delete_dataset_item(
        &self,
        project_id: &str,
        dataset_id: &str,
        item_id: &str,
    ) -> Result<bool, MetaError> {
        let n = a_exec!(
            self,
            "DELETE FROM agent_dataset_items WHERE project_id = ? AND dataset_id = ? AND id = ?",
            project_id,
            dataset_id,
            item_id
        );
        Ok(n > 0)
    }

    pub async fn upsert_agent_experiment(&self, e: &AgentExperiment) -> Result<(), MetaError> {
        let eval_ids = serde_json::to_string(&e.evaluator_ids).unwrap_or_else(|_| "[]".into());
        let meta_json = serde_json::to_string(&e.metadata).unwrap_or_else(|_| "null".into());
        let affected = a_exec!(
            self,
            r#"
            INSERT INTO agent_experiments (
                id, project_id, name, description, dataset_id, dataset_version,
                agent_name, agent_version, prompt_version, model, evaluator_ids_json,
                status, total_items, completed_items, passed_items, avg_score,
                avg_latency_ms, total_cost_usd, metadata_json, created_at_ms, completed_at_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                status = excluded.status,
                total_items = excluded.total_items,
                completed_items = excluded.completed_items,
                passed_items = excluded.passed_items,
                avg_score = excluded.avg_score,
                avg_latency_ms = excluded.avg_latency_ms,
                total_cost_usd = excluded.total_cost_usd,
                completed_at_ms = excluded.completed_at_ms
            WHERE agent_experiments.project_id = excluded.project_id
            "#,
            &e.id,
            &e.project_id,
            &e.name,
            &e.description,
            &e.dataset_id,
            e.dataset_version,
            &e.agent_name,
            &e.agent_version,
            &e.prompt_version,
            &e.model,
            eval_ids,
            &e.status,
            e.total_items,
            e.completed_items,
            e.passed_items,
            e.avg_score,
            e.avg_latency_ms,
            e.total_cost_usd,
            meta_json,
            e.created_at_ms,
            e.completed_at_ms
        );
        if affected == 0 {
            return Err(MetaError::Conflict(format!(
                "experiment '{}' belongs to a different project",
                e.id
            )));
        }
        Ok(())
    }

    pub async fn get_agent_experiment(
        &self,
        project_id: &str,
        experiment_id: &str,
    ) -> Result<Option<AgentExperiment>, MetaError> {
        let opt = a_map_opt!(
            self,
            "SELECT * FROM agent_experiments WHERE project_id = ? AND id = ?",
            |r| { experiment_from_row!(r) },
            project_id,
            experiment_id
        );
        Ok(opt)
    }

    pub async fn list_agent_experiments(
        &self,
        project_id: &str,
    ) -> Result<Vec<AgentExperiment>, MetaError> {
        let rows = a_map_all!(
            self,
            "SELECT * FROM agent_experiments WHERE project_id = ? ORDER BY created_at_ms DESC",
            |r| { experiment_from_row!(r) },
            project_id
        );
        Ok(rows)
    }

    pub async fn insert_experiment_run(&self, r: &AgentExperimentRun) -> Result<(), MetaError> {
        let out_s = serde_json::to_string(&r.actual_output).unwrap_or_else(|_| "null".into());
        let passed = r.passed.map(|b| if b { 1i32 } else { 0i32 });
        a_exec!(
            self,
            r#"
            INSERT INTO agent_experiment_runs (
                id, project_id, experiment_id, dataset_item_id, trace_id, status,
                actual_output_json, latency_ms, cost_usd, total_tokens, avg_score,
                passed, error, created_at_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
            &r.id,
            &r.project_id,
            &r.experiment_id,
            &r.dataset_item_id,
            &r.trace_id,
            &r.status,
            out_s,
            r.latency_ms,
            r.cost_usd,
            r.total_tokens,
            r.avg_score,
            passed,
            &r.error,
            r.created_at_ms
        );
        Ok(())
    }

    pub async fn list_experiment_runs(
        &self,
        project_id: &str,
        experiment_id: &str,
    ) -> Result<Vec<AgentExperimentRun>, MetaError> {
        let mut runs = a_map_all!(
            self,
            "SELECT * FROM agent_experiment_runs WHERE project_id = ? AND experiment_id = ? ORDER BY created_at_ms ASC",
            |r| {
                let out_s: String = r.get("actual_output_json");
                let passed_i: Option<i32> = r.get("passed");
                AgentExperimentRun {
                    id: r.get("id"),
                    project_id: r.get("project_id"),
                    experiment_id: r.get("experiment_id"),
                    dataset_item_id: r.get("dataset_item_id"),
                    trace_id: r.get("trace_id"),
                    status: r.get("status"),
                    actual_output: serde_json::from_str(&out_s).unwrap_or(Value::Null),
                    latency_ms: r.get("latency_ms"),
                    cost_usd: r.get("cost_usd"),
                    total_tokens: r.get("total_tokens"),
                    avg_score: r.get("avg_score"),
                    passed: passed_i.map(|v| v != 0),
                    error: r.get("error"),
                    evaluations: Vec::new(),
                    created_at_ms: r.get("created_at_ms"),
                }
            },
            project_id,
            experiment_id
        );

        if runs.is_empty() {
            return Ok(runs);
        }

        let run_ids: Vec<String> = runs.iter().map(|r| r.id.clone()).collect();
        let mut evals_by_run: HashMap<String, Vec<AgentEvaluation>> = HashMap::new();
        for chunk in run_ids.chunks(IN_CHUNK_SIZE) {
            let placeholders = vec!["?"; chunk.len()].join(", ");
            let sql = format!(
                "SELECT * FROM agent_evaluations \
                 WHERE project_id = ? AND experiment_run_id IN ({placeholders}) \
                 ORDER BY created_at_ms ASC"
            );
            let evals = match self {
                MetaStore::Sqlite(pool) => {
                    let mut q = sqlx::query(&sql).bind(project_id);
                    for id in chunk {
                        q = q.bind(id);
                    }
                    q.fetch_all(pool)
                        .await?
                        .into_iter()
                        .map(|r| eval_from_row!(r))
                        .collect::<Vec<_>>()
                }
                MetaStore::Postgres(pool) => {
                    let pg = pg_rewrite(&sql);
                    let mut q = sqlx::query(&pg).bind(project_id);
                    for id in chunk {
                        q = q.bind(id);
                    }
                    q.fetch_all(pool)
                        .await?
                        .into_iter()
                        .map(|r| eval_from_row!(r))
                        .collect::<Vec<_>>()
                }
            };
            for ev in evals {
                if let Some(ref run_id) = ev.experiment_run_id {
                    evals_by_run.entry(run_id.clone()).or_default().push(ev);
                }
            }
        }
        for run in &mut runs {
            if let Some(evals) = evals_by_run.remove(&run.id) {
                run.evaluations = evals;
            }
        }
        Ok(runs)
    }

    // -----------------------------------------------------------------------
    // Monitoring Alerts, Privacy Policies & Model Pricing
    // -----------------------------------------------------------------------

    pub async fn upsert_agent_alert_rule(&self, a: &AgentAlertRule) -> Result<(), MetaError> {
        let enabled = if a.enabled { 1i32 } else { 0i32 };
        let affected = a_exec!(
            self,
            r#"
            INSERT INTO agent_alert_rules (
                id, project_id, name, agent_name, environment, metric, operator,
                threshold, window_minutes, cooldown_minutes, enabled, state, webhook_url,
                last_evaluated_ms, last_triggered_ms, last_resolved_ms, last_value, created_at_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                agent_name = excluded.agent_name,
                environment = excluded.environment,
                metric = excluded.metric,
                operator = excluded.operator,
                threshold = excluded.threshold,
                window_minutes = excluded.window_minutes,
                cooldown_minutes = excluded.cooldown_minutes,
                enabled = excluded.enabled,
                state = excluded.state,
                webhook_url = excluded.webhook_url,
                last_evaluated_ms = excluded.last_evaluated_ms,
                last_triggered_ms = excluded.last_triggered_ms,
                last_resolved_ms = excluded.last_resolved_ms,
                last_value = excluded.last_value
            WHERE agent_alert_rules.project_id = excluded.project_id
            "#,
            &a.id,
            &a.project_id,
            &a.name,
            &a.agent_name,
            &a.environment,
            &a.metric,
            &a.operator,
            a.threshold,
            a.window_minutes,
            a.cooldown_minutes,
            enabled,
            &a.state,
            &a.webhook_url,
            a.last_evaluated_ms,
            a.last_triggered_ms,
            a.last_resolved_ms,
            a.last_value,
            a.created_at_ms
        );
        if affected == 0 {
            return Err(MetaError::Conflict(format!(
                "agent alert rule '{}' belongs to a different project",
                a.id
            )));
        }
        Ok(())
    }

    /// Claims an alert state transition: moves the rule from `from_state` to `to_state` only if
    /// it is still in `from_state`. Returns false when another node made the transition first.
    pub async fn claim_agent_alert_transition(
        &self,
        id: &str,
        from_state: &str,
        to_state: &str,
        now_ms: i64,
    ) -> Result<bool, MetaError> {
        let affected = if to_state == "firing" {
            a_exec!(
                self,
                "UPDATE agent_alert_rules SET state = ?, last_triggered_ms = ? WHERE id = ? AND state = ?",
                to_state,
                now_ms,
                id,
                from_state
            )
        } else {
            a_exec!(
                self,
                "UPDATE agent_alert_rules SET state = ?, last_resolved_ms = ? WHERE id = ? AND state = ?",
                to_state,
                now_ms,
                id,
                from_state
            )
        };
        Ok(affected == 1)
    }

    /// Records an evaluation without touching the rule's state, so a node that lost a claim
    /// cannot write a stale state back.
    pub async fn record_agent_alert_evaluation(
        &self,
        id: &str,
        evaluated_ms: i64,
        value: Option<f64>,
    ) -> Result<(), MetaError> {
        a_exec!(
            self,
            "UPDATE agent_alert_rules SET last_evaluated_ms = ?, last_value = COALESCE(?, last_value) WHERE id = ?",
            evaluated_ms,
            value,
            id
        );
        Ok(())
    }

    pub async fn get_agent_alert_rule(
        &self,
        project_id: &str,
        id: &str,
    ) -> Result<Option<AgentAlertRule>, MetaError> {
        let opt = a_map_opt!(
            self,
            "SELECT * FROM agent_alert_rules WHERE project_id = ? AND id = ?",
            |r| { alert_rule_from_row!(r) },
            project_id,
            id
        );
        Ok(opt)
    }

    pub async fn list_agent_alert_rules(
        &self,
        project_id: &str,
    ) -> Result<Vec<AgentAlertRule>, MetaError> {
        let rows = a_map_all!(
            self,
            "SELECT * FROM agent_alert_rules WHERE project_id = ? ORDER BY created_at_ms DESC",
            |r| { alert_rule_from_row!(r) },
            project_id
        );
        Ok(rows)
    }

    pub async fn delete_agent_alert_rule(
        &self,
        project_id: &str,
        id: &str,
    ) -> Result<bool, MetaError> {
        let n = a_exec!(
            self,
            "DELETE FROM agent_alert_rules WHERE project_id = ? AND id = ?",
            project_id,
            id
        );
        Ok(n > 0)
    }

    pub async fn insert_agent_alert_event(&self, ev: &AgentAlertEvent) -> Result<(), MetaError> {
        let delivered = if ev.delivered { 1i32 } else { 0i32 };
        a_exec!(
            self,
            r#"
            INSERT INTO agent_alert_events (
                id, project_id, rule_id, rule_name, event_type, metric,
                observed_value, threshold, delivered, delivery_error, created_at_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
            &ev.id,
            &ev.project_id,
            &ev.rule_id,
            &ev.rule_name,
            &ev.event_type,
            &ev.metric,
            ev.observed_value,
            ev.threshold,
            delivered,
            &ev.delivery_error,
            ev.created_at_ms
        );
        Ok(())
    }

    pub async fn list_agent_alert_events(
        &self,
        project_id: &str,
        limit: usize,
    ) -> Result<Vec<AgentAlertEvent>, MetaError> {
        let rows = a_map_all!(
            self,
            "SELECT * FROM agent_alert_events WHERE project_id = ? ORDER BY created_at_ms DESC LIMIT ?",
            |r| {
                AgentAlertEvent {
                    id: r.get("id"),
                    project_id: r.get("project_id"),
                    rule_id: r.get("rule_id"),
                    rule_name: r.get("rule_name"),
                    event_type: r.get("event_type"),
                    metric: r.get("metric"),
                    observed_value: r.get("observed_value"),
                    threshold: r.get("threshold"),
                    delivered: r.get::<i32, _>("delivered") != 0,
                    delivery_error: r.get("delivery_error"),
                    created_at_ms: r.get("created_at_ms"),
                }
            },
            project_id,
            limit.clamp(1, 200) as i64
        );
        Ok(rows)
    }

    pub async fn get_agent_privacy_policy(
        &self,
        project_id: &str,
    ) -> Result<AgentPrivacyPolicy, MetaError> {
        let opt = a_map_opt!(
            self,
            "SELECT * FROM agent_privacy_policies WHERE project_id = ?",
            |r| {
                let rk: String = r.get("redact_keys_json");
                let ex: String = r.get("exclude_attributes_json");
                AgentPrivacyPolicy {
                    project_id: r.get("project_id"),
                    capture_inputs: r.get::<i32, _>("capture_inputs") != 0,
                    capture_outputs: r.get::<i32, _>("capture_outputs") != 0,
                    capture_tool_args: r.get::<i32, _>("capture_tool_args") != 0,
                    capture_tool_results: r.get::<i32, _>("capture_tool_results") != 0,
                    capture_prompts: r.get::<i32, _>("capture_prompts") != 0,
                    capture_responses: r.get::<i32, _>("capture_responses") != 0,
                    capture_user_id: r.get::<i32, _>("capture_user_id") != 0,
                    redact_pii: r.get::<i32, _>("redact_pii") != 0,
                    redact_keys: serde_json::from_str(&rk).unwrap_or_default(),
                    exclude_attributes: serde_json::from_str(&ex).unwrap_or_default(),
                    retention_days: r.get("retention_days"),
                    sample_rate: r.get("sample_rate"),
                    updated_at_ms: r.get("updated_at_ms"),
                }
            },
            project_id
        );
        Ok(opt.unwrap_or_else(|| AgentPrivacyPolicy::default_for_project(project_id)))
    }

    pub async fn upsert_agent_privacy_policy(
        &self,
        p: &AgentPrivacyPolicy,
    ) -> Result<(), MetaError> {
        let rk = serde_json::to_string(&p.redact_keys).unwrap_or_else(|_| "[]".into());
        let ex = serde_json::to_string(&p.exclude_attributes).unwrap_or_else(|_| "[]".into());
        a_exec!(
            self,
            r#"
            INSERT INTO agent_privacy_policies (
                project_id, capture_inputs, capture_outputs, capture_tool_args,
                capture_tool_results, capture_prompts, capture_responses, capture_user_id,
                redact_pii, redact_keys_json, exclude_attributes_json, retention_days,
                sample_rate, updated_at_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(project_id) DO UPDATE SET
                capture_inputs = excluded.capture_inputs,
                capture_outputs = excluded.capture_outputs,
                capture_tool_args = excluded.capture_tool_args,
                capture_tool_results = excluded.capture_tool_results,
                capture_prompts = excluded.capture_prompts,
                capture_responses = excluded.capture_responses,
                capture_user_id = excluded.capture_user_id,
                redact_pii = excluded.redact_pii,
                redact_keys_json = excluded.redact_keys_json,
                exclude_attributes_json = excluded.exclude_attributes_json,
                retention_days = excluded.retention_days,
                sample_rate = excluded.sample_rate,
                updated_at_ms = excluded.updated_at_ms
            "#,
            &p.project_id,
            if p.capture_inputs { 1i32 } else { 0 },
            if p.capture_outputs { 1i32 } else { 0 },
            if p.capture_tool_args { 1i32 } else { 0 },
            if p.capture_tool_results { 1i32 } else { 0 },
            if p.capture_prompts { 1i32 } else { 0 },
            if p.capture_responses { 1i32 } else { 0 },
            if p.capture_user_id { 1i32 } else { 0 },
            if p.redact_pii { 1i32 } else { 0 },
            rk,
            ex,
            p.retention_days.clamp(1, 3650),
            p.sample_rate.clamp(0.0, 1.0),
            p.updated_at_ms
        );
        Ok(())
    }

    pub async fn list_model_pricing(
        &self,
        project_id: &str,
    ) -> Result<Vec<ModelPricingRule>, MetaError> {
        let rows = a_map_all!(
            self,
            "SELECT * FROM agent_model_pricing WHERE project_id = ? ORDER BY model_pattern ASC",
            |r| {
                ModelPricingRule {
                    id: r.get("id"),
                    project_id: r.get("project_id"),
                    provider: r.get("provider"),
                    model_pattern: r.get("model_pattern"),
                    input_per_1m_usd: r.get("input_per_1m_usd"),
                    output_per_1m_usd: r.get("output_per_1m_usd"),
                    cached_input_per_1m_usd: r.get("cached_input_per_1m_usd"),
                    updated_at_ms: r.get("updated_at_ms"),
                }
            },
            project_id
        );
        Ok(rows)
    }

    pub async fn upsert_model_pricing(&self, rule: &ModelPricingRule) -> Result<(), MetaError> {
        let affected = a_exec!(
            self,
            r#"
            INSERT INTO agent_model_pricing (
                id, project_id, provider, model_pattern, input_per_1m_usd,
                output_per_1m_usd, cached_input_per_1m_usd, updated_at_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                provider = excluded.provider,
                model_pattern = excluded.model_pattern,
                input_per_1m_usd = excluded.input_per_1m_usd,
                output_per_1m_usd = excluded.output_per_1m_usd,
                cached_input_per_1m_usd = excluded.cached_input_per_1m_usd,
                updated_at_ms = excluded.updated_at_ms
            WHERE agent_model_pricing.project_id = excluded.project_id
            "#,
            &rule.id,
            &rule.project_id,
            &rule.provider,
            &rule.model_pattern,
            rule.input_per_1m_usd,
            rule.output_per_1m_usd,
            rule.cached_input_per_1m_usd,
            rule.updated_at_ms
        );
        if affected == 0 {
            return Err(MetaError::Conflict(format!(
                "model pricing rule '{}' belongs to a different project",
                rule.id
            )));
        }
        Ok(())
    }

    pub async fn delete_model_pricing(
        &self,
        project_id: &str,
        id: &str,
    ) -> Result<bool, MetaError> {
        let n = a_exec!(
            self,
            "DELETE FROM agent_model_pricing WHERE project_id = ? AND id = ?",
            project_id,
            id
        );
        Ok(n > 0)
    }

    // -----------------------------------------------------------------------
    // 3-Level Erasure Tombstones & Retention Pruning
    // -----------------------------------------------------------------------

    pub async fn insert_erasure_tombstone(
        &self,
        tb: &AgentErasureTombstone,
    ) -> Result<(), MetaError> {
        let lvl = tb.level.as_str();
        let affected = a_exec!(
            self,
            r#"
            INSERT INTO agent_erasure_tombstones (
                id, project_id, level, target_id, created_at_ms, expires_at_ms
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                level = excluded.level,
                target_id = excluded.target_id,
                created_at_ms = excluded.created_at_ms,
                expires_at_ms = excluded.expires_at_ms
            WHERE agent_erasure_tombstones.project_id = excluded.project_id
            "#,
            &tb.id,
            &tb.project_id,
            lvl,
            &tb.target_id,
            tb.created_at,
            tb.expires_at
        );
        if affected == 0 {
            return Err(MetaError::Conflict(format!(
                "erasure tombstone '{}' belongs to a different project",
                tb.id
            )));
        }
        Ok(())
    }

    pub async fn list_active_erasure_tombstones(
        &self,
        project_id: &str,
        now_ms: i64,
    ) -> Result<Vec<AgentErasureTombstone>, MetaError> {
        let rows = a_map_all!(
            self,
            "SELECT * FROM agent_erasure_tombstones \
             WHERE project_id = ? AND (expires_at_ms IS NULL OR expires_at_ms > ?) \
             ORDER BY created_at_ms DESC",
            |r| { tombstone_from_row!(r) },
            project_id,
            now_ms
        );
        Ok(rows)
    }

    pub async fn list_all_active_erasure_tombstones(
        &self,
        now_ms: i64,
    ) -> Result<Vec<AgentErasureTombstone>, MetaError> {
        let rows = a_map_all!(
            self,
            "SELECT * FROM agent_erasure_tombstones \
             WHERE expires_at_ms IS NULL OR expires_at_ms > ? \
             ORDER BY created_at_ms DESC",
            |r| { tombstone_from_row!(r) },
            now_ms
        );
        Ok(rows)
    }

    pub async fn prune_expired_erasure_tombstones(
        &self,
        project_id: Option<&str>,
        now_ms: i64,
    ) -> Result<u64, MetaError> {
        let n = match project_id {
            Some(pid) => a_exec!(
                self,
                "DELETE FROM agent_erasure_tombstones \
                 WHERE project_id = ? AND expires_at_ms IS NOT NULL AND expires_at_ms <= ?",
                pid,
                now_ms
            ),
            None => a_exec!(
                self,
                "DELETE FROM agent_erasure_tombstones \
                 WHERE expires_at_ms IS NOT NULL AND expires_at_ms <= ?",
                now_ms
            ),
        };
        Ok(n)
    }

    /// Purges SQL evaluations & rollups for `Trace` or `Session` level erasures.
    /// For `ErasureLevel::User`, resolve user session/trace IDs from `AgentTraceStore` first
    /// and call `purge_evaluations_for_targets`.
    pub async fn purge_erased_evaluations(
        &self,
        project_id: &str,
        level: ErasureLevel,
        target_id: &str,
    ) -> Result<u64, MetaError> {
        if target_id.is_empty() || level == ErasureLevel::User {
            return Ok(0);
        }
        run_in_tx!(self, |tx, is_pg| {
            let (select_sql, delete_sql, root_tt) = match level {
                ErasureLevel::Trace => (
                    "SELECT DISTINCT target_type, session_id, trace_id, span_id \
                     FROM agent_evaluations WHERE project_id = ? AND trace_id = ?",
                    "DELETE FROM agent_evaluations WHERE project_id = ? AND trace_id = ?",
                    EvalTargetType::Trace,
                ),
                ErasureLevel::Session => (
                    "SELECT DISTINCT target_type, session_id, trace_id, span_id \
                     FROM agent_evaluations WHERE project_id = ? AND session_id = ?",
                    "DELETE FROM agent_evaluations WHERE project_id = ? AND session_id = ?",
                    EvalTargetType::Session,
                ),
                ErasureLevel::User => unreachable!(),
            };

            let raw_rows = tx_fetch_all!(
                tx,
                is_pg,
                select_sql,
                |r| {
                    (
                        r.get::<String, _>("target_type"),
                        r.get::<String, _>("session_id"),
                        r.get::<Option<String>, _>("trace_id"),
                        r.get::<Option<String>, _>("span_id"),
                    )
                },
                project_id,
                target_id
            );
            let mut affected_targets = collect_affected_targets(&raw_rows);
            affected_targets.insert((root_tt.as_str().to_string(), target_id.to_string()));

            let deleted = tx_exec!(tx, is_pg, delete_sql, project_id, target_id);
            for (tt_s, tid) in affected_targets {
                let tt = EvalTargetType::parse(&tt_s);
                recompute_rollup_in_tx!(tx, is_pg, project_id, tt, &tid);
            }
            Ok(deleted)
        })
    }

    /// Purges SQL evaluations & rollups matching any of the provided `session_ids` or `trace_ids`
    /// (used for `ErasureLevel::User` after resolving a user's sessions/traces from `AgentTraceStore`).
    pub async fn purge_evaluations_for_targets(
        &self,
        project_id: &str,
        session_ids: &[String],
        trace_ids: &[String],
    ) -> Result<u64, MetaError> {
        if session_ids.is_empty() && trace_ids.is_empty() {
            return Ok(0);
        }
        run_in_tx!(self, |tx, is_pg| {
            let mut deleted = 0u64;
            let mut affected_targets: BTreeSet<(String, String)> = BTreeSet::new();

            for chunk in session_ids.chunks(IN_CHUNK_SIZE) {
                let placeholders = vec!["?"; chunk.len()].join(", ");
                let sel_sql = format!(
                    "SELECT DISTINCT target_type, session_id, trace_id, span_id \
                     FROM agent_evaluations WHERE project_id = ? AND session_id IN ({placeholders})"
                );
                let sel_owned = if is_pg { pg_rewrite(&sel_sql) } else { sel_sql };
                let mut q = sqlx::query(&sel_owned).bind(project_id);
                for id in chunk {
                    q = q.bind(id);
                }
                let rows = q
                    .fetch_all(&mut *tx)
                    .await?
                    .into_iter()
                    .map(|r| {
                        (
                            r.get::<String, _>("target_type"),
                            r.get::<String, _>("session_id"),
                            r.get::<Option<String>, _>("trace_id"),
                            r.get::<Option<String>, _>("span_id"),
                        )
                    })
                    .collect::<Vec<_>>();
                affected_targets.extend(collect_affected_targets(&rows));
                for sid in chunk {
                    if !sid.is_empty() {
                        affected_targets.insert(("session".to_string(), sid.clone()));
                    }
                }

                let del_sql = format!(
                    "DELETE FROM agent_evaluations WHERE project_id = ? AND session_id IN ({placeholders})"
                );
                let del_owned = if is_pg { pg_rewrite(&del_sql) } else { del_sql };
                let mut dq = sqlx::query(&del_owned).bind(project_id);
                for id in chunk {
                    dq = dq.bind(id);
                }
                deleted += dq.execute(&mut *tx).await?.rows_affected();
            }

            for chunk in trace_ids.chunks(IN_CHUNK_SIZE) {
                let placeholders = vec!["?"; chunk.len()].join(", ");
                let sel_sql = format!(
                    "SELECT DISTINCT target_type, session_id, trace_id, span_id \
                     FROM agent_evaluations WHERE project_id = ? AND trace_id IN ({placeholders})"
                );
                let sel_owned = if is_pg { pg_rewrite(&sel_sql) } else { sel_sql };
                let mut q = sqlx::query(&sel_owned).bind(project_id);
                for id in chunk {
                    q = q.bind(id);
                }
                let rows = q
                    .fetch_all(&mut *tx)
                    .await?
                    .into_iter()
                    .map(|r| {
                        (
                            r.get::<String, _>("target_type"),
                            r.get::<String, _>("session_id"),
                            r.get::<Option<String>, _>("trace_id"),
                            r.get::<Option<String>, _>("span_id"),
                        )
                    })
                    .collect::<Vec<_>>();
                affected_targets.extend(collect_affected_targets(&rows));
                for tid in chunk {
                    if !tid.is_empty() {
                        affected_targets.insert(("trace".to_string(), tid.clone()));
                    }
                }

                let del_sql = format!(
                    "DELETE FROM agent_evaluations WHERE project_id = ? AND trace_id IN ({placeholders})"
                );
                let del_owned = if is_pg { pg_rewrite(&del_sql) } else { del_sql };
                let mut dq = sqlx::query(&del_owned).bind(project_id);
                for id in chunk {
                    dq = dq.bind(id);
                }
                deleted += dq.execute(&mut *tx).await?.rows_affected();
            }

            for (tt_s, tid) in affected_targets {
                let tt = EvalTargetType::parse(&tt_s);
                recompute_rollup_in_tx!(tx, is_pg, project_id, tt, &tid);
            }
            Ok(deleted)
        })
    }

    /// Prunes evaluations older than `cutoff_ms` for a project and updates affected rollups.
    pub async fn prune_expired_agent_evaluations(
        &self,
        project_id: &str,
        cutoff_ms: i64,
    ) -> Result<u64, MetaError> {
        run_in_tx!(self, |tx, is_pg| {
            let raw_rows = tx_fetch_all!(
                tx,
                is_pg,
                "SELECT DISTINCT target_type, session_id, trace_id, span_id \
                 FROM agent_evaluations WHERE project_id = ? AND created_at_ms < ?",
                |r| {
                    (
                        r.get::<String, _>("target_type"),
                        r.get::<String, _>("session_id"),
                        r.get::<Option<String>, _>("trace_id"),
                        r.get::<Option<String>, _>("span_id"),
                    )
                },
                project_id,
                cutoff_ms
            );
            let affected_targets = collect_affected_targets(&raw_rows);

            let deleted = tx_exec!(
                tx,
                is_pg,
                "DELETE FROM agent_evaluations WHERE project_id = ? AND created_at_ms < ?",
                project_id,
                cutoff_ms
            );

            for (tt_s, tid) in affected_targets {
                let tt = EvalTargetType::parse(&tt_s);
                recompute_rollup_in_tx!(tx, is_pg, project_id, tt, &tid);
            }
            Ok(deleted)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[allow(clippy::too_many_arguments)]
    fn sample_eval(
        id: &str,
        project_id: &str,
        target_type: EvalTargetType,
        session_id: &str,
        trace_id: Option<&str>,
        span_id: Option<&str>,
        score: f64,
        passed: Option<bool>,
        created_at_ms: i64,
    ) -> AgentEvaluation {
        AgentEvaluation {
            id: id.to_string(),
            project_id: project_id.to_string(),
            target_type,
            session_id: session_id.to_string(),
            trace_id: trace_id.map(str::to_string),
            span_id: span_id.map(str::to_string),
            experiment_run_id: None,
            evaluator_id: "ev_cfg_1".to_string(),
            evaluator_name: "Test Eval".to_string(),
            evaluator_type: EvaluatorType::Deterministic,
            evaluator_version: "1.0.0".to_string(),
            score,
            score_type: ScoreType::Numeric,
            passed,
            label: passed.map(|p| if p { "pass" } else { "fail" }.to_string()),
            reasoning: Some("ok".to_string()),
            execution_ms: 5,
            error: None,
            metadata: json!({ "k": "v" }),
            created_at_ms,
        }
    }

    #[tokio::test]
    async fn schema_split_excludes_trace_and_span_tables() {
        let meta = MetaStore::connect_url("sqlite::memory:").await.unwrap();
        let MetaStore::Sqlite(pool) = &meta else {
            panic!("expected sqlite");
        };
        let rows: Vec<String> =
            sqlx::query_scalar("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
                .fetch_all(pool)
                .await
                .unwrap();

        assert!(!rows.contains(&"agent_traces".to_string()));
        assert!(!rows.contains(&"agent_spans".to_string()));

        for expected in [
            "agent_evaluations",
            "agent_eval_rollups",
            "agent_evaluator_configs",
            "agent_datasets",
            "agent_dataset_items",
            "agent_experiments",
            "agent_experiment_runs",
            "agent_alert_rules",
            "agent_alert_events",
            "agent_privacy_policies",
            "agent_model_pricing",
            "agent_erasure_tombstones",
        ] {
            assert!(
                rows.contains(&expected.to_string()),
                "missing expected control-plane table {expected}"
            );
        }
    }

    #[tokio::test]
    async fn multi_level_evaluations_and_rollups_stay_consistent() {
        let meta = MetaStore::connect_url("sqlite::memory:").await.unwrap();

        let e_sess_1 = sample_eval(
            "eval_s1",
            "p1",
            EvalTargetType::Session,
            "sess_1",
            None,
            None,
            1.0,
            Some(true),
            1000,
        );
        let e_sess_2 = sample_eval(
            "eval_s2",
            "p1",
            EvalTargetType::Session,
            "sess_1",
            None,
            None,
            0.5,
            None, // ungraded — must not dilute pass_rate denominator
            1100,
        );
        let e_tr_1 = sample_eval(
            "eval_t1",
            "p1",
            EvalTargetType::Trace,
            "sess_1",
            Some("tr_1"),
            None,
            0.8,
            Some(true),
            1200,
        );
        let e_tr_2 = sample_eval(
            "eval_t2",
            "p1",
            EvalTargetType::Trace,
            "sess_1",
            Some("tr_1"),
            None,
            0.2,
            Some(false),
            1300,
        );
        let e_sp_1 = sample_eval(
            "eval_sp1",
            "p1",
            EvalTargetType::Span,
            "sess_1",
            Some("tr_1"),
            Some("sp_1"),
            0.9,
            Some(true),
            1400,
        );

        for ev in [&e_sess_1, &e_sess_2, &e_tr_1, &e_tr_2, &e_sp_1] {
            meta.insert_agent_evaluation(ev).await.unwrap();
        }

        let sess_evals = meta.list_session_evaluations("p1", "sess_1").await.unwrap();
        assert_eq!(sess_evals.len(), 5);
        let tr_evals = meta.list_trace_evaluations("p1", "tr_1").await.unwrap();
        assert_eq!(tr_evals.len(), 3);
        let sp_evals = meta.list_span_evaluations("p1", "sp_1").await.unwrap();
        assert_eq!(sp_evals.len(), 1);
        assert_eq!(sp_evals[0].target_type, EvalTargetType::Span);

        let by_session = meta
            .list_project_evaluations_by_target("p1", Some(EvalTargetType::Session), 50)
            .await
            .unwrap();
        assert_eq!(by_session.len(), 2);

        let all_rollups = meta.list_agent_eval_rollups("p1", None).await.unwrap();
        assert_eq!(all_rollups.len(), 3);

        let sr = &all_rollups[&(EvalTargetType::Session, "sess_1".to_string())];
        assert!((sr.avg_score - 0.75).abs() < 1e-9);
        assert_eq!(sr.pass_rate, Some(1.0));
        assert_eq!(sr.eval_count, 2);

        let tr = &all_rollups[&(EvalTargetType::Trace, "tr_1".to_string())];
        assert!((tr.avg_score - 0.5).abs() < 1e-9);
        assert_eq!(tr.pass_rate, Some(0.5));
        assert_eq!(tr.eval_count, 2);

        let batch = meta
            .get_eval_rollups_batch(
                "p1",
                EvalTargetType::Span,
                &["sp_1".to_string(), "missing".to_string()],
            )
            .await
            .unwrap();
        assert_eq!(batch.len(), 1);
        let spr = &batch[&(EvalTargetType::Span, "sp_1".to_string())];
        assert!((spr.avg_score - 0.9).abs() < 1e-9);
        assert_eq!(spr.eval_count, 1);
    }

    #[tokio::test]
    async fn evaluator_target_type_and_p0_03_cross_tenant_conflicts() {
        let meta = MetaStore::connect_url("sqlite::memory:").await.unwrap();

        let cfg = EvaluatorConfig {
            id: "cfg_shared".to_string(),
            project_id: "proj_a".to_string(),
            name: "Span Cost".to_string(),
            description: "desc".to_string(),
            target_type: EvalTargetType::Span,
            evaluator_type: EvaluatorType::Performance,
            kind: "cost_budget".to_string(),
            version: "1.0.0".to_string(),
            enabled: true,
            agent_name: None,
            environment: None,
            sample_rate: 1.0,
            config: json!({ "max_cost_usd": 0.05 }),
            created_at_ms: 100,
            updated_at_ms: 100,
        };
        meta.upsert_evaluator_config(&cfg).await.unwrap();

        let fetched = meta
            .get_evaluator_config("proj_a", "cfg_shared")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fetched.target_type, EvalTargetType::Span);

        let mut cfg_b = cfg.clone();
        cfg_b.project_id = "proj_b".to_string();
        assert!(matches!(
            meta.upsert_evaluator_config(&cfg_b).await,
            Err(MetaError::Conflict(_))
        ));

        let ds = AgentDataset {
            id: "ds_shared".to_string(),
            project_id: "proj_a".to_string(),
            name: "DS".to_string(),
            description: "".to_string(),
            version: 1,
            tags: vec![],
            item_count: 0,
            created_at_ms: 100,
            updated_at_ms: 100,
        };
        meta.upsert_agent_dataset(&ds).await.unwrap();
        let mut ds_b = ds.clone();
        ds_b.project_id = "proj_b".to_string();
        assert!(matches!(
            meta.upsert_agent_dataset(&ds_b).await,
            Err(MetaError::Conflict(_))
        ));

        let item = AgentDatasetItem {
            id: "item_shared".to_string(),
            project_id: "proj_a".to_string(),
            dataset_id: "ds_shared".to_string(),
            dataset_version: 1,
            name: None,
            input: json!({"q": "hi"}),
            expected_output: Value::Null,
            expected_behavior: Value::Null,
            source_trace_id: None,
            metadata: Value::Null,
            tags: vec![],
            created_at_ms: 100,
            updated_at_ms: 100,
        };
        meta.upsert_dataset_item(&item).await.unwrap();
        let mut item_b = item.clone();
        item_b.project_id = "proj_b".to_string();
        assert!(matches!(
            meta.upsert_dataset_item(&item_b).await,
            Err(MetaError::Conflict(_))
        ));

        let exp = AgentExperiment {
            id: "exp_shared".to_string(),
            project_id: "proj_a".to_string(),
            name: "Exp".to_string(),
            description: "".to_string(),
            dataset_id: "ds_shared".to_string(),
            dataset_version: 1,
            agent_name: "bot".to_string(),
            agent_version: "v1".to_string(),
            prompt_version: None,
            model: None,
            evaluator_ids: vec![],
            status: "completed".to_string(),
            total_items: 1,
            completed_items: 1,
            passed_items: 1,
            avg_score: Some(1.0),
            avg_latency_ms: Some(10.0),
            total_cost_usd: 0.0,
            metadata: Value::Null,
            created_at_ms: 100,
            completed_at_ms: Some(200),
        };
        meta.upsert_agent_experiment(&exp).await.unwrap();
        let mut exp_b = exp.clone();
        exp_b.project_id = "proj_b".to_string();
        assert!(matches!(
            meta.upsert_agent_experiment(&exp_b).await,
            Err(MetaError::Conflict(_))
        ));

        let rule = AgentAlertRule {
            id: "rule_shared".to_string(),
            project_id: "proj_a".to_string(),
            name: "Alert".to_string(),
            agent_name: None,
            environment: None,
            metric: "failure_rate".to_string(),
            operator: "gt".to_string(),
            threshold: 0.1,
            window_minutes: 15,
            cooldown_minutes: 30,
            enabled: true,
            state: "ok".to_string(),
            webhook_url: None,
            last_evaluated_ms: None,
            last_triggered_ms: None,
            last_resolved_ms: None,
            last_value: None,
            created_at_ms: 100,
        };
        meta.upsert_agent_alert_rule(&rule).await.unwrap();
        let mut rule_b = rule.clone();
        rule_b.project_id = "proj_b".to_string();
        assert!(matches!(
            meta.upsert_agent_alert_rule(&rule_b).await,
            Err(MetaError::Conflict(_))
        ));

        let price = ModelPricingRule {
            id: "price_shared".to_string(),
            project_id: "proj_a".to_string(),
            provider: "openai".to_string(),
            model_pattern: "gpt-4o".to_string(),
            input_per_1m_usd: 2.5,
            output_per_1m_usd: 10.0,
            cached_input_per_1m_usd: 1.25,
            updated_at_ms: 100,
        };
        meta.upsert_model_pricing(&price).await.unwrap();
        let mut price_b = price.clone();
        price_b.project_id = "proj_b".to_string();
        assert!(matches!(
            meta.upsert_model_pricing(&price_b).await,
            Err(MetaError::Conflict(_))
        ));

        let tb = AgentErasureTombstone {
            id: "tb_shared".to_string(),
            project_id: "proj_a".to_string(),
            level: ErasureLevel::Trace,
            target_id: "tr_1".to_string(),
            created_at: 100,
            expires_at: None,
        };
        meta.insert_erasure_tombstone(&tb).await.unwrap();
        let mut tb_b = tb.clone();
        tb_b.project_id = "proj_b".to_string();
        assert!(matches!(
            meta.insert_erasure_tombstone(&tb_b).await,
            Err(MetaError::Conflict(_))
        ));
    }

    #[tokio::test]
    async fn three_level_erasure_tombstones_purge_and_retention() {
        let meta = MetaStore::connect_url("sqlite::memory:").await.unwrap();

        let tb_active_perm = AgentErasureTombstone {
            id: "tb_1".to_string(),
            project_id: "p1".to_string(),
            level: ErasureLevel::Trace,
            target_id: "tr_del".to_string(),
            created_at: 100,
            expires_at: None,
        };
        let tb_active_future = AgentErasureTombstone {
            id: "tb_2".to_string(),
            project_id: "p1".to_string(),
            level: ErasureLevel::Session,
            target_id: "sess_del".to_string(),
            created_at: 100,
            expires_at: Some(5000),
        };
        let tb_expired = AgentErasureTombstone {
            id: "tb_3".to_string(),
            project_id: "p1".to_string(),
            level: ErasureLevel::User,
            target_id: "usr_old".to_string(),
            created_at: 100,
            expires_at: Some(1500),
        };
        for tb in [&tb_active_perm, &tb_active_future, &tb_expired] {
            meta.insert_erasure_tombstone(tb).await.unwrap();
        }

        let active = meta
            .list_active_erasure_tombstones("p1", 2000)
            .await
            .unwrap();
        assert_eq!(active.len(), 2);
        assert_eq!(
            meta.list_all_active_erasure_tombstones(2000)
                .await
                .unwrap()
                .len(),
            2
        );

        let pruned_tb = meta
            .prune_expired_erasure_tombstones(Some("p1"), 2000)
            .await
            .unwrap();
        assert_eq!(pruned_tb, 1);

        // Seed evaluations across sess_keep (tr_del + tr_keep) and sess_del (tr_s2) and usr_target (tr_u1)
        for ev in [
            sample_eval(
                "e_s_keep",
                "p1",
                EvalTargetType::Session,
                "sess_keep",
                None,
                None,
                0.9,
                Some(true),
                100,
            ),
            sample_eval(
                "e_tr_del",
                "p1",
                EvalTargetType::Trace,
                "sess_keep",
                Some("tr_del"),
                None,
                0.4,
                Some(false),
                200,
            ),
            sample_eval(
                "e_sp_del",
                "p1",
                EvalTargetType::Span,
                "sess_keep",
                Some("tr_del"),
                Some("sp_del"),
                0.3,
                Some(false),
                300,
            ),
            sample_eval(
                "e_tr_keep",
                "p1",
                EvalTargetType::Trace,
                "sess_keep",
                Some("tr_keep"),
                None,
                0.8,
                Some(true),
                400,
            ),
            sample_eval(
                "e_s_del",
                "p1",
                EvalTargetType::Session,
                "sess_del",
                None,
                None,
                0.5,
                Some(true),
                500,
            ),
            sample_eval(
                "e_tr_s2",
                "p1",
                EvalTargetType::Trace,
                "sess_del",
                Some("tr_s2"),
                None,
                0.6,
                Some(true),
                600,
            ),
            sample_eval(
                "e_tr_u1",
                "p1",
                EvalTargetType::Trace,
                "sess_u",
                Some("tr_u1"),
                None,
                0.7,
                Some(true),
                700,
            ),
        ] {
            meta.insert_agent_evaluation(&ev).await.unwrap();
        }

        // Trace-level purge clears tr_del + sp_del, leaving sess_keep and tr_keep untouched
        let purged_tr = meta
            .purge_erased_evaluations("p1", ErasureLevel::Trace, "tr_del")
            .await
            .unwrap();
        assert_eq!(purged_tr, 2);
        let rollups = meta.list_agent_eval_rollups("p1", None).await.unwrap();
        assert!(!rollups.contains_key(&(EvalTargetType::Trace, "tr_del".to_string())));
        assert!(!rollups.contains_key(&(EvalTargetType::Span, "sp_del".to_string())));
        assert!(rollups.contains_key(&(EvalTargetType::Session, "sess_keep".to_string())));
        assert!(rollups.contains_key(&(EvalTargetType::Trace, "tr_keep".to_string())));

        // Session-level purge clears sess_del + tr_s2
        let purged_sess = meta
            .purge_erased_evaluations("p1", ErasureLevel::Session, "sess_del")
            .await
            .unwrap();
        assert_eq!(purged_sess, 2);
        let rollups = meta.list_agent_eval_rollups("p1", None).await.unwrap();
        assert!(!rollups.contains_key(&(EvalTargetType::Session, "sess_del".to_string())));
        assert!(!rollups.contains_key(&(EvalTargetType::Trace, "tr_s2".to_string())));

        // User-level purge via resolved session/trace IDs
        let purged_usr = meta
            .purge_evaluations_for_targets("p1", &["sess_u".to_string()], &["tr_u1".to_string()])
            .await
            .unwrap();
        assert_eq!(purged_usr, 1);

        // Retention pruning: add an older eval for tr_keep and prune cutoff = 350
        meta.insert_agent_evaluation(&sample_eval(
            "e_tr_keep_old",
            "p1",
            EvalTargetType::Trace,
            "sess_keep",
            Some("tr_keep"),
            None,
            0.2,
            Some(false),
            150,
        ))
        .await
        .unwrap();
        let pruned_evals = meta
            .prune_expired_agent_evaluations("p1", 350)
            .await
            .unwrap();
        assert_eq!(pruned_evals, 2); // e_s_keep (100) + e_tr_keep_old (150)

        let rollups = meta.list_agent_eval_rollups("p1", None).await.unwrap();
        assert!(!rollups.contains_key(&(EvalTargetType::Session, "sess_keep".to_string())));
        let tr_keep_rollup = &rollups[&(EvalTargetType::Trace, "tr_keep".to_string())];
        assert!((tr_keep_rollup.avg_score - 0.8).abs() < 1e-9);
        assert_eq!(tr_keep_rollup.eval_count, 1);
    }

    #[tokio::test]
    async fn alert_rules_events_and_experiment_run_evals_roundtrip() {
        let meta = MetaStore::connect_url("sqlite::memory:").await.unwrap();

        let mut rule = AgentAlertRule {
            id: "r1".to_string(),
            project_id: "p1".to_string(),
            name: "High Failure".to_string(),
            agent_name: None,
            environment: Some("production".to_string()),
            metric: "failure_rate".to_string(),
            operator: "gt".to_string(),
            threshold: 0.1,
            window_minutes: 15,
            cooldown_minutes: 30,
            enabled: true,
            state: "ok".to_string(),
            webhook_url: Some("https://example.com/hook".to_string()),
            last_evaluated_ms: None,
            last_triggered_ms: None,
            last_resolved_ms: None,
            last_value: None,
            created_at_ms: 100,
        };
        meta.upsert_agent_alert_rule(&rule).await.unwrap();
        rule.state = "firing".to_string();
        rule.last_evaluated_ms = Some(200);
        rule.last_triggered_ms = Some(200);
        rule.last_value = Some(0.25);
        meta.upsert_agent_alert_rule(&rule).await.unwrap();

        let loaded = meta
            .get_agent_alert_rule("p1", "r1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.state, "firing");
        assert_eq!(loaded.last_value, Some(0.25));

        meta.insert_agent_alert_event(&AgentAlertEvent {
            id: "ev1".to_string(),
            project_id: "p1".to_string(),
            rule_id: "r1".to_string(),
            rule_name: "High Failure".to_string(),
            event_type: "triggered".to_string(),
            metric: "failure_rate".to_string(),
            observed_value: 0.25,
            threshold: 0.1,
            delivered: true,
            delivery_error: None,
            created_at_ms: 200,
        })
        .await
        .unwrap();
        assert_eq!(
            meta.list_agent_alert_events("p1", 10).await.unwrap().len(),
            1
        );

        // Experiment run + linked evaluation hydration
        let run = AgentExperimentRun {
            id: "run_1".to_string(),
            project_id: "p1".to_string(),
            experiment_id: "exp_1".to_string(),
            dataset_item_id: "item_1".to_string(),
            trace_id: Some("tr_exp".to_string()),
            status: "ok".to_string(),
            actual_output: json!({"ans": "42"}),
            latency_ms: 120,
            cost_usd: 0.001,
            total_tokens: 50,
            avg_score: Some(1.0),
            passed: Some(true),
            error: None,
            evaluations: vec![],
            created_at_ms: 300,
        };
        meta.insert_experiment_run(&run).await.unwrap();
        let mut run_eval = sample_eval(
            "eval_exp_1",
            "p1",
            EvalTargetType::Trace,
            "",
            Some("tr_exp"),
            None,
            1.0,
            Some(true),
            301,
        );
        run_eval.experiment_run_id = Some("run_1".to_string());
        meta.insert_agent_evaluation(&run_eval).await.unwrap();

        let runs = meta.list_experiment_runs("p1", "exp_1").await.unwrap();
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].evaluations.len(), 1);
        assert_eq!(runs[0].evaluations[0].id, "eval_exp_1");
    }

    #[test]
    fn pg_rewrite_rewrites_all_db_agent_queries_cleanly() {
        let src = include_str!("db_agent.rs");
        let prod_src = src.split("#[cfg(test)]").next().unwrap_or(src);
        // Spot-check critical SQL snippets with string literals containing '?' or SQL expressions
        let sample_queries = [
            "SELECT COUNT(*) AS eval_count, AVG(score) AS avg_score, \
             COALESCE(SUM(CASE WHEN passed = 1 THEN 1 ELSE 0 END), 0) AS passed_count \
             FROM agent_evaluations WHERE project_id = ? AND target_type = ? AND session_id = ?",
            "DELETE FROM agent_erasure_tombstones WHERE project_id = ? AND expires_at_ms IS NOT NULL AND expires_at_ms <= ?",
        ];
        for sql in sample_queries {
            let pg = pg_rewrite(sql);
            assert!(!pg.contains('?'));
            assert!(pg.contains("$1"));
        }
        assert!(!prod_src.contains("MIN(agent_traces"));
        assert!(!prod_src.contains("INSERT INTO agent_traces"));
        assert!(!prod_src.contains("INSERT INTO agent_spans"));
    }

    #[tokio::test]
    async fn error_evaluations_are_excluded_from_rollups() {
        let meta = MetaStore::connect_url("sqlite::memory:").await.unwrap();

        let mut err_only = sample_eval(
            "eval_err_only",
            "p1",
            EvalTargetType::Trace,
            "sess_err",
            Some("tr_err_only"),
            None,
            0.0,
            Some(false),
            1000,
        );
        err_only.error = Some("malformed judge json".to_string());
        meta.insert_agent_evaluation(&err_only).await.unwrap();

        // Error-only target must not create a rollup row, while diagnostic row stays persisted.
        assert_eq!(
            meta.list_trace_evaluations("p1", "tr_err_only")
                .await
                .unwrap()
                .len(),
            1
        );
        let rollups = meta.list_agent_eval_rollups("p1", None).await.unwrap();
        assert!(!rollups.contains_key(&(EvalTargetType::Trace, "tr_err_only".to_string())));

        // Healthy evaluation + subsequent error evaluation keeps healthy rollup intact.
        let healthy = sample_eval(
            "eval_ok",
            "p1",
            EvalTargetType::Trace,
            "sess_mix",
            Some("tr_mix"),
            None,
            0.9,
            Some(true),
            1100,
        );
        meta.insert_agent_evaluation(&healthy).await.unwrap();

        let mut bad_judge = sample_eval(
            "eval_bad",
            "p1",
            EvalTargetType::Trace,
            "sess_mix",
            Some("tr_mix"),
            None,
            0.0,
            Some(false),
            1200,
        );
        bad_judge.error = Some("expected value at line 1 column 1".to_string());
        meta.insert_agent_evaluation(&bad_judge).await.unwrap();

        let trace_evals = meta.list_trace_evaluations("p1", "tr_mix").await.unwrap();
        assert_eq!(trace_evals.len(), 2);

        let rollups = meta.list_agent_eval_rollups("p1", None).await.unwrap();
        let tr = &rollups[&(EvalTargetType::Trace, "tr_mix".to_string())];
        assert!((tr.avg_score - 0.9).abs() < 1e-9);
        assert_eq!(tr.pass_rate, Some(1.0));
        assert_eq!(tr.eval_count, 1);
    }
}
