use std::path::Path;
use std::str::FromStr;

use pulse_exec::{CustomEventDef, CustomPropertyDef, LookupTableDef};
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions};
use sqlx::Row;
use thiserror::Error;

use crate::models::{
    ApiKeyRecord, CohortRecord, Dashboard, Experiment, FeatureFlag, LexiconEvent, LexiconProperty,
    LookupTableSummary, Organization, Project, SavedReport,
};

#[derive(Debug, Error)]
pub enum MetaError {
    #[error("database error: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("migration error: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),
    #[error("conflict or cross-tenant modification rejected: {0}")]
    Conflict(String),
}

#[derive(Clone)]
pub enum MetaStore {
    Sqlite(SqlitePool),
    Postgres(PgPool),
}

/// Converts `?` placeholders (outside single-quoted SQL string literals) into PostgreSQL `$1, $2, ...`.
fn pg_rewrite(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len() + 16);
    let mut idx = 0usize;
    let mut in_quotes = false;
    for ch in sql.chars() {
        if ch == '\'' {
            in_quotes = !in_quotes;
            out.push(ch);
        } else if ch == '?' && !in_quotes {
            idx += 1;
            out.push('$');
            out.push_str(&idx.to_string());
        } else {
            out.push(ch);
        }
    }
    out
}

macro_rules! db_exec {
    ($store:expr, $sql:expr $(, $arg:expr)* $(,)?) => {{
        match $store {
            MetaStore::Sqlite(pool) => {
                let q = sqlx::query($sql)$(.bind($arg))*;
                q.execute(pool).await?.rows_affected()
            }
            MetaStore::Postgres(pool) => {
                let pg_q = pg_rewrite($sql);
                let q = sqlx::query(&pg_q)$(.bind($arg))*;
                q.execute(pool).await?.rows_affected()
            }
        }
    }};
}

macro_rules! db_map_all {
    ($store:expr, $sql:expr, |$row:ident| $body:block $(, $arg:expr)* $(,)?) => {{
        match $store {
            MetaStore::Sqlite(pool) => {
                let rows = sqlx::query($sql)$(.bind($arg))*.fetch_all(pool).await?;
                rows.into_iter().map(|$row| $body).collect::<Vec<_>>()
            }
            MetaStore::Postgres(pool) => {
                let pg_q = pg_rewrite($sql);
                let rows = sqlx::query(&pg_q)$(.bind($arg))*.fetch_all(pool).await?;
                rows.into_iter().map(|$row| $body).collect::<Vec<_>>()
            }
        }
    }};
}

macro_rules! db_map_opt {
    ($store:expr, $sql:expr, |$row:ident| $body:block $(, $arg:expr)* $(,)?) => {{
        match $store {
            MetaStore::Sqlite(pool) => {
                let opt = sqlx::query($sql)$(.bind($arg))*.fetch_optional(pool).await?;
                opt.map(|$row| $body)
            }
            MetaStore::Postgres(pool) => {
                let pg_q = pg_rewrite($sql);
                let opt = sqlx::query(&pg_q)$(.bind($arg))*.fetch_optional(pool).await?;
                opt.map(|$row| $body)
            }
        }
    }};
}

impl MetaStore {
    /// Connects to `DATABASE_URL` (`postgres://...` or `sqlite://...`) or falls back to `<data_dir>/pulse_meta.db`.
    pub async fn connect_auto(data_dir: &Path) -> Result<Self, MetaError> {
        if let Ok(url) = std::env::var("DATABASE_URL") {
            let trimmed = url.trim();
            if !trimmed.is_empty() {
                return Self::connect_url(trimmed).await;
            }
        }
        std::fs::create_dir_all(data_dir).ok();
        let sqlite_path = data_dir.join("pulse_meta.db");
        let url = format!("sqlite://{}", sqlite_path.display());
        Self::connect_url(&url).await
    }

    pub async fn connect_url(url: &str) -> Result<Self, MetaError> {
        let store = if url.starts_with("postgres://") || url.starts_with("postgresql://") {
            let pool = PgPoolOptions::new()
                .max_connections(16)
                .connect(url)
                .await?;
            sqlx::migrate!("./migrations").run(&pool).await?;
            Self::Postgres(pool)
        } else {
            let opts = SqliteConnectOptions::from_str(url)?
                .create_if_missing(true)
                .journal_mode(SqliteJournalMode::Wal)
                .busy_timeout(std::time::Duration::from_secs(10));
            let pool = SqlitePoolOptions::new()
                .max_connections(8)
                .connect_with(opts)
                .await?;
            sqlx::migrate!("./migrations").run(&pool).await?;
            Self::Sqlite(pool)
        };
        Ok(store)
    }

    // ------------------- Organizations & Projects -------------------

    pub async fn ensure_org(&self, org: &Organization) -> Result<(), MetaError> {
        let _ = db_exec!(
            self,
            "INSERT INTO organizations (id, name, slug, created_at_ms) VALUES (?, ?, ?, ?)
             ON CONFLICT(id) DO NOTHING",
            &org.id,
            &org.name,
            &org.slug,
            org.created_at_ms,
        );
        Ok(())
    }

    /// Inserts a brand-new project; NEVER overwrites an existing project (`P0-03`).
    pub async fn insert_project(&self, proj: &Project) -> Result<(), MetaError> {
        let affected = db_exec!(
            self,
            "INSERT INTO projects (id, org_id, name, timezone, created_at_ms) VALUES (?, ?, ?, ?, ?)
             ON CONFLICT(id) DO NOTHING",
            &proj.id,
            &proj.org_id,
            &proj.name,
            &proj.timezone,
            proj.created_at_ms,
        );
        if affected == 0 {
            return Err(MetaError::Conflict(format!(
                "project '{}' already exists",
                proj.id
            )));
        }
        Ok(())
    }

    /// Idempotent bootstrap helper used ONLY on node startup to ensure the default project exists.
    pub async fn ensure_project(&self, proj: &Project) -> Result<(), MetaError> {
        let _ = db_exec!(
            self,
            "INSERT INTO projects (id, org_id, name, timezone, created_at_ms) VALUES (?, ?, ?, ?, ?)
             ON CONFLICT(id) DO NOTHING",
            &proj.id,
            &proj.org_id,
            &proj.name,
            &proj.timezone,
            proj.created_at_ms,
        );
        Ok(())
    }

    pub async fn get_project(&self, project_id: &str) -> Result<Option<Project>, MetaError> {
        let proj = db_map_opt!(
            self,
            "SELECT id, org_id, name, timezone, created_at_ms FROM projects WHERE id = ?",
            |r| {
                Project {
                    id: r.get("id"),
                    org_id: r.get("org_id"),
                    name: r.get("name"),
                    timezone: r.get("timezone"),
                    created_at_ms: r.get("created_at_ms"),
                }
            },
            project_id,
        );
        Ok(proj)
    }

    pub async fn list_projects(&self) -> Result<Vec<Project>, MetaError> {
        let list = db_map_all!(
            self,
            "SELECT id, org_id, name, timezone, created_at_ms FROM projects ORDER BY created_at_ms ASC",
            |r| {
                Project {
                    id: r.get("id"),
                    org_id: r.get("org_id"),
                    name: r.get("name"),
                    timezone: r.get("timezone"),
                    created_at_ms: r.get("created_at_ms"),
                }
            }
        );
        Ok(list)
    }

    // ------------------- Hashed API Keys (`P0-02`, `P0-03`) -------------------

    pub async fn insert_api_key(&self, rec: &ApiKeyRecord) -> Result<(), MetaError> {
        let revoked = i32::from(rec.revoked);
        let affected = db_exec!(
            self,
            "INSERT INTO api_keys (id, key_hash, key_prefix, project_id, label, role, created_at_ms, revoked)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(key_hash) DO UPDATE SET
                label = excluded.label,
                revoked = excluded.revoked
             WHERE api_keys.project_id = excluded.project_id",
            &rec.id,
            &rec.key_hash,
            &rec.key_prefix,
            &rec.project_id,
            &rec.label,
            &rec.role,
            rec.created_at_ms,
            revoked,
        );
        if affected == 0 {
            return Err(MetaError::Conflict(
                "api key already bound to another project".to_string(),
            ));
        }
        Ok(())
    }

    pub async fn list_api_keys(
        &self,
        project_id: Option<&str>,
    ) -> Result<Vec<ApiKeyRecord>, MetaError> {
        let list = if let Some(pid) = project_id {
            db_map_all!(
                self,
                "SELECT id, key_hash, key_prefix, project_id, label, role, created_at_ms, revoked
                 FROM api_keys WHERE project_id = ? ORDER BY created_at_ms ASC",
                |r| {
                    ApiKeyRecord {
                        id: r.get("id"),
                        key_hash: r.get("key_hash"),
                        key_prefix: r.get("key_prefix"),
                        project_id: r.get("project_id"),
                        label: r.get("label"),
                        role: r.get("role"),
                        created_at_ms: r.get("created_at_ms"),
                        revoked: r.get::<i32, _>("revoked") != 0,
                    }
                },
                pid,
            )
        } else {
            db_map_all!(
                self,
                "SELECT id, key_hash, key_prefix, project_id, label, role, created_at_ms, revoked
                 FROM api_keys ORDER BY created_at_ms ASC",
                |r| {
                    ApiKeyRecord {
                        id: r.get("id"),
                        key_hash: r.get("key_hash"),
                        key_prefix: r.get("key_prefix"),
                        project_id: r.get("project_id"),
                        label: r.get("label"),
                        role: r.get("role"),
                        created_at_ms: r.get("created_at_ms"),
                        revoked: r.get::<i32, _>("revoked") != 0,
                    }
                }
            )
        };
        Ok(list)
    }

    // ------------------- Lexicon Governance -------------------

    pub async fn upsert_lexicon_event(&self, ev: &LexiconEvent) -> Result<(), MetaError> {
        let _ = db_exec!(
            self,
            "INSERT INTO lexicon_events (project_id, name, display_name, description, status, owner, volume_24h, updated_at_ms)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(project_id, name) DO UPDATE SET
                display_name = excluded.display_name,
                description = excluded.description,
                status = excluded.status,
                owner = excluded.owner,
                volume_24h = CASE WHEN lexicon_events.volume_24h >= excluded.volume_24h
                                  THEN lexicon_events.volume_24h ELSE excluded.volume_24h END,
                updated_at_ms = excluded.updated_at_ms",
            &ev.project_id,
            &ev.name,
            &ev.display_name,
            &ev.description,
            &ev.status,
            &ev.owner,
            ev.volume_24h,
            ev.updated_at_ms,
        );
        Ok(())
    }

    pub async fn increment_lexicon_discovery(
        &self,
        project_id: &str,
        event_name: &str,
        add_vol: i64,
        props: &[(String, &'static str)],
        now_ms: i64,
    ) -> Result<(), MetaError> {
        let _ = db_exec!(
            self,
            "INSERT INTO lexicon_events (project_id, name, display_name, description, status, owner, volume_24h, updated_at_ms)
             VALUES (?, ?, ?, 'Auto-discovered telemetry event', 'active', 'auto', ?, ?)
             ON CONFLICT(project_id, name) DO UPDATE SET
                volume_24h = lexicon_events.volume_24h + excluded.volume_24h,
                updated_at_ms = excluded.updated_at_ms",
            project_id,
            event_name,
            event_name,
            add_vol,
            now_ms,
        );

        for (pname, ptype) in props {
            let _ = db_exec!(
                self,
                "INSERT INTO lexicon_properties (project_id, name, prop_scope, inferred_types, status, pii_masked, updated_at_ms)
                 VALUES (?, ?, 'event', ?, 'active', 0, ?)
                 ON CONFLICT(project_id, name) DO UPDATE SET updated_at_ms = excluded.updated_at_ms",
                project_id,
                pname,
                *ptype,
                now_ms,
            );
        }
        Ok(())
    }

    pub async fn upsert_lexicon_property(&self, prop: &LexiconProperty) -> Result<(), MetaError> {
        let pii = i32::from(prop.pii_masked);
        let _ = db_exec!(
            self,
            "INSERT INTO lexicon_properties (project_id, name, prop_scope, inferred_types, status, pii_masked, updated_at_ms)
             VALUES (?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(project_id, name) DO UPDATE SET
                prop_scope = excluded.prop_scope,
                inferred_types = excluded.inferred_types,
                status = excluded.status,
                pii_masked = excluded.pii_masked,
                updated_at_ms = excluded.updated_at_ms",
            &prop.project_id,
            &prop.name,
            &prop.prop_scope,
            &prop.inferred_types,
            &prop.status,
            pii,
            prop.updated_at_ms,
        );
        Ok(())
    }

    pub async fn list_lexicon_events(
        &self,
        project_id: &str,
    ) -> Result<Vec<LexiconEvent>, MetaError> {
        let list = db_map_all!(
            self,
            "SELECT project_id, name, display_name, description, status, owner, volume_24h, updated_at_ms
             FROM lexicon_events WHERE project_id = ? ORDER BY name ASC",
            |r| {
                LexiconEvent {
                    project_id: r.get("project_id"),
                    name: r.get("name"),
                    display_name: r.get("display_name"),
                    description: r.get("description"),
                    status: r.get("status"),
                    owner: r.get("owner"),
                    volume_24h: r.get("volume_24h"),
                    updated_at_ms: r.get("updated_at_ms"),
                }
            },
            project_id,
        );
        Ok(list)
    }

    pub async fn list_lexicon_properties(
        &self,
        project_id: &str,
    ) -> Result<Vec<LexiconProperty>, MetaError> {
        let list = db_map_all!(
            self,
            "SELECT project_id, name, prop_scope, inferred_types, status, pii_masked, updated_at_ms
             FROM lexicon_properties WHERE project_id = ? ORDER BY name ASC",
            |r| {
                LexiconProperty {
                    project_id: r.get("project_id"),
                    name: r.get("name"),
                    prop_scope: r.get("prop_scope"),
                    inferred_types: r.get("inferred_types"),
                    status: r.get("status"),
                    pii_masked: r.get::<i32, _>("pii_masked") != 0,
                    updated_at_ms: r.get("updated_at_ms"),
                }
            },
            project_id,
        );
        Ok(list)
    }

    // ------------------- Cohorts & Virtual Schema (`P0-03` Tenant-Checked Upserts) -------------------

    pub async fn upsert_cohort(&self, c: &CohortRecord) -> Result<(), MetaError> {
        let def_json = serde_json::to_string(&c.definition).unwrap_or_else(|_| "{}".into());
        let affected = db_exec!(
            self,
            "INSERT INTO cohorts (id, project_id, name, description, definition_json, created_at_ms)
             VALUES (?, ?, ?, ?, ?, ?)
             ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                description = excluded.description,
                definition_json = excluded.definition_json
             WHERE cohorts.project_id = excluded.project_id",
            &c.id,
            &c.project_id,
            &c.name,
            &c.description,
            &def_json,
            c.created_at_ms,
        );
        if affected == 0 {
            return Err(MetaError::Conflict(format!(
                "cohort '{}' belongs to a different project",
                c.id
            )));
        }
        Ok(())
    }

    pub async fn list_cohorts(&self, project_id: &str) -> Result<Vec<CohortRecord>, MetaError> {
        let rows = db_map_all!(
            self,
            "SELECT id, project_id, name, description, definition_json, created_at_ms
             FROM cohorts WHERE project_id = ? ORDER BY created_at_ms DESC",
            |r| {
                let def_str: String = r.get("definition_json");
                serde_json::from_str(&def_str)
                    .ok()
                    .map(|definition| CohortRecord {
                        id: r.get("id"),
                        project_id: r.get("project_id"),
                        name: r.get("name"),
                        description: r.get("description"),
                        definition,
                        created_at_ms: r.get("created_at_ms"),
                    })
            },
            project_id,
        );
        Ok(rows.into_iter().flatten().collect())
    }

    pub async fn upsert_lookup_table(
        &self,
        id: &str,
        project_id: &str,
        def: &LookupTableDef,
        now_ms: i64,
    ) -> Result<(), MetaError> {
        let payload_json = serde_json::to_string(def).unwrap_or_else(|_| "{}".into());
        let row_count = def.entries.len() as i64;
        let affected = db_exec!(
            self,
            "INSERT INTO lookup_tables (id, project_id, name, join_prop, payload_json, row_count, updated_at_ms)
             VALUES (?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                join_prop = excluded.join_prop,
                payload_json = excluded.payload_json,
                row_count = excluded.row_count,
                updated_at_ms = excluded.updated_at_ms
             WHERE lookup_tables.project_id = excluded.project_id",
            id,
            project_id,
            &def.name,
            &def.join_prop,
            &payload_json,
            row_count,
            now_ms,
        );
        if affected == 0 {
            return Err(MetaError::Conflict(format!(
                "lookup_table '{id}' belongs to a different project"
            )));
        }
        Ok(())
    }

    pub async fn list_lookup_tables(
        &self,
        project_id: &str,
    ) -> Result<Vec<(LookupTableSummary, LookupTableDef)>, MetaError> {
        let rows = db_map_all!(
            self,
            "SELECT id, project_id, name, join_prop, payload_json, row_count, updated_at_ms
             FROM lookup_tables WHERE project_id = ? ORDER BY name ASC",
            |r| {
                let pjson: String = r.get("payload_json");
                serde_json::from_str::<LookupTableDef>(&pjson)
                    .ok()
                    .map(|def| {
                        let mut cols: Vec<String> = def
                            .entries
                            .values()
                            .next()
                            .map(|m| m.keys().cloned().collect())
                            .unwrap_or_default();
                        cols.sort();
                        let sum = LookupTableSummary {
                            id: r.get("id"),
                            project_id: r.get("project_id"),
                            name: r.get("name"),
                            join_prop: r.get("join_prop"),
                            row_count: r.get::<i64, _>("row_count") as usize,
                            sample_columns: cols,
                            updated_at_ms: r.get("updated_at_ms"),
                        };
                        (sum, def)
                    })
            },
            project_id,
        );
        Ok(rows.into_iter().flatten().collect())
    }

    pub async fn upsert_custom_event(
        &self,
        id: &str,
        project_id: &str,
        def: &CustomEventDef,
        now_ms: i64,
    ) -> Result<(), MetaError> {
        let djson = serde_json::to_string(def).unwrap_or_else(|_| "{}".into());
        let affected = db_exec!(
            self,
            "INSERT INTO custom_events (id, project_id, name, definition_json, created_at_ms)
             VALUES (?, ?, ?, ?, ?)
             ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                definition_json = excluded.definition_json
             WHERE custom_events.project_id = excluded.project_id",
            id,
            project_id,
            &def.name,
            &djson,
            now_ms,
        );
        if affected == 0 {
            return Err(MetaError::Conflict(format!(
                "custom_event '{id}' belongs to a different project"
            )));
        }
        Ok(())
    }

    pub async fn list_custom_events(
        &self,
        project_id: &str,
    ) -> Result<Vec<CustomEventDef>, MetaError> {
        let rows = db_map_all!(
            self,
            "SELECT definition_json FROM custom_events WHERE project_id = ? ORDER BY name ASC",
            |r| {
                let s: String = r.get("definition_json");
                serde_json::from_str::<CustomEventDef>(&s).ok()
            },
            project_id,
        );
        Ok(rows.into_iter().flatten().collect())
    }

    pub async fn upsert_custom_property(
        &self,
        id: &str,
        project_id: &str,
        def: &CustomPropertyDef,
        now_ms: i64,
    ) -> Result<(), MetaError> {
        let refs_json =
            serde_json::to_string(&def.referenced_props).unwrap_or_else(|_| "[]".into());
        let affected = db_exec!(
            self,
            "INSERT INTO custom_properties (id, project_id, name, sql_expr, referenced_props_json, created_at_ms)
             VALUES (?, ?, ?, ?, ?, ?)
             ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                sql_expr = excluded.sql_expr,
                referenced_props_json = excluded.referenced_props_json
             WHERE custom_properties.project_id = excluded.project_id",
            id,
            project_id,
            &def.name,
            &def.sql_expr,
            &refs_json,
            now_ms,
        );
        if affected == 0 {
            return Err(MetaError::Conflict(format!(
                "custom_property '{id}' belongs to a different project"
            )));
        }
        Ok(())
    }

    pub async fn list_custom_properties(
        &self,
        project_id: &str,
    ) -> Result<Vec<CustomPropertyDef>, MetaError> {
        let rows = db_map_all!(
            self,
            "SELECT name, sql_expr, referenced_props_json
             FROM custom_properties WHERE project_id = ? ORDER BY name ASC",
            |r| {
                let refs_s: String = r.get("referenced_props_json");
                CustomPropertyDef {
                    name: r.get("name"),
                    sql_expr: r.get("sql_expr"),
                    referenced_props: serde_json::from_str(&refs_s).unwrap_or_default(),
                }
            },
            project_id,
        );
        Ok(rows)
    }

    // ------------------- Feature Flags & Experiments (`P0-03`, `P0-04`, `P2-10`) -------------------

    pub async fn upsert_feature_flag(&self, f: &FeatureFlag) -> Result<(), MetaError> {
        let en = i32::from(f.enabled);
        let vjson = serde_json::to_string(&f.variants).unwrap_or_else(|_| "[]".into());
        let affected = db_exec!(
            self,
            "INSERT INTO feature_flags (id, project_id, flag_key, description, enabled, rollout_pct, targeting_cohort_id, variants_json, updated_at_ms)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(id) DO UPDATE SET
                flag_key = excluded.flag_key,
                description = excluded.description,
                enabled = excluded.enabled,
                rollout_pct = excluded.rollout_pct,
                targeting_cohort_id = excluded.targeting_cohort_id,
                variants_json = excluded.variants_json,
                updated_at_ms = excluded.updated_at_ms
             WHERE feature_flags.project_id = excluded.project_id",
            &f.id,
            &f.project_id,
            &f.key,
            &f.description,
            en,
            f.rollout_pct,
            &f.targeting_cohort_id,
            &vjson,
            f.updated_at_ms,
        );
        if affected == 0 {
            return Err(MetaError::Conflict(format!(
                "feature_flag '{}' belongs to a different project",
                f.id
            )));
        }
        Ok(())
    }

    pub async fn list_feature_flags(
        &self,
        project_id: &str,
    ) -> Result<Vec<FeatureFlag>, MetaError> {
        let rows = db_map_all!(
            self,
            "SELECT id, project_id, flag_key, description, enabled, rollout_pct, targeting_cohort_id, variants_json, updated_at_ms
             FROM feature_flags WHERE project_id = ? ORDER BY flag_key ASC",
            |r| {
                let vstr: String = r.get("variants_json");
                FeatureFlag {
                    id: r.get("id"),
                    project_id: r.get("project_id"),
                    key: r.get("flag_key"),
                    description: r.get("description"),
                    enabled: r.get::<i32, _>("enabled") != 0,
                    rollout_pct: r.get("rollout_pct"),
                    targeting_cohort_id: r.get("targeting_cohort_id"),
                    variants: serde_json::from_str(&vstr).unwrap_or_default(),
                    updated_at_ms: r.get("updated_at_ms"),
                }
            },
            project_id,
        );
        Ok(rows)
    }

    pub async fn upsert_experiment(&self, exp: &Experiment) -> Result<(), MetaError> {
        let pjson = serde_json::to_string(&exp.primary_metric).unwrap_or_else(|_| "{}".into());
        let gjson = serde_json::to_string(&exp.guardrail_metrics).unwrap_or_else(|_| "[]".into());
        let affected = db_exec!(
            self,
            "INSERT INTO experiments (id, project_id, name, flag_id, flag_key, hypothesis, status, start_ts_ms, end_ts_ms, primary_metric_json, guardrail_metrics_json, created_at_ms)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                flag_id = excluded.flag_id,
                flag_key = excluded.flag_key,
                hypothesis = excluded.hypothesis,
                status = excluded.status,
                start_ts_ms = excluded.start_ts_ms,
                end_ts_ms = excluded.end_ts_ms,
                primary_metric_json = excluded.primary_metric_json,
                guardrail_metrics_json = excluded.guardrail_metrics_json
             WHERE experiments.project_id = excluded.project_id",
            &exp.id,
            &exp.project_id,
            &exp.name,
            &exp.flag_id,
            &exp.flag_key,
            &exp.hypothesis,
            &exp.status,
            exp.start_ts_ms,
            exp.end_ts_ms,
            &pjson,
            &gjson,
            exp.created_at_ms,
        );
        if affected == 0 {
            return Err(MetaError::Conflict(format!(
                "experiment '{}' belongs to a different project",
                exp.id
            )));
        }
        Ok(())
    }

    pub async fn list_experiments(&self, project_id: &str) -> Result<Vec<Experiment>, MetaError> {
        let rows = db_map_all!(
            self,
            "SELECT id, project_id, name, flag_id, flag_key, hypothesis, status, start_ts_ms, end_ts_ms, primary_metric_json, guardrail_metrics_json, created_at_ms
             FROM experiments WHERE project_id = ? ORDER BY created_at_ms DESC",
            |r| {
                let pstr: String = r.get("primary_metric_json");
                let gstr: String = r.get("guardrail_metrics_json");
                serde_json::from_str(&pstr).ok().map(|primary_metric| Experiment {
                    id: r.get("id"),
                    project_id: r.get("project_id"),
                    name: r.get("name"),
                    flag_id: r.get("flag_id"),
                    flag_key: r.get("flag_key"),
                    hypothesis: r.get("hypothesis"),
                    status: r.get("status"),
                    start_ts_ms: r.get("start_ts_ms"),
                    end_ts_ms: r.get("end_ts_ms"),
                    primary_metric,
                    guardrail_metrics: serde_json::from_str(&gstr).unwrap_or_default(),
                    created_at_ms: r.get("created_at_ms"),
                })
            },
            project_id,
        );
        Ok(rows.into_iter().flatten().collect())
    }

    /// Strict tenant-scoped experiment lookup (`WHERE id = ? AND project_id = ?`, `P0-04`).
    pub async fn get_experiment(
        &self,
        project_id: &str,
        exp_id: &str,
    ) -> Result<Option<Experiment>, MetaError> {
        let row = db_map_opt!(
            self,
            "SELECT id, project_id, name, flag_id, flag_key, hypothesis, status, start_ts_ms, end_ts_ms, primary_metric_json, guardrail_metrics_json, created_at_ms
             FROM experiments WHERE id = ? AND project_id = ?",
            |r| {
                let pstr: String = r.get("primary_metric_json");
                let gstr: String = r.get("guardrail_metrics_json");
                serde_json::from_str(&pstr).ok().map(|primary_metric| Experiment {
                    id: r.get("id"),
                    project_id: r.get("project_id"),
                    name: r.get("name"),
                    flag_id: r.get("flag_id"),
                    flag_key: r.get("flag_key"),
                    hypothesis: r.get("hypothesis"),
                    status: r.get("status"),
                    start_ts_ms: r.get("start_ts_ms"),
                    end_ts_ms: r.get("end_ts_ms"),
                    primary_metric,
                    guardrail_metrics: serde_json::from_str(&gstr).unwrap_or_default(),
                    created_at_ms: r.get("created_at_ms"),
                })
            },
            exp_id,
            project_id,
        );
        Ok(row.flatten())
    }

    // ------------------- Saved Reports & Dashboards (`P0-03`) -------------------

    pub async fn upsert_saved_report(&self, rep: &SavedReport) -> Result<(), MetaError> {
        let qjson = serde_json::to_string(&rep.query_json).unwrap_or_else(|_| "{}".into());
        let affected = db_exec!(
            self,
            "INSERT INTO saved_reports (id, project_id, name, report_kind, query_json, created_at_ms)
             VALUES (?, ?, ?, ?, ?, ?)
             ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                report_kind = excluded.report_kind,
                query_json = excluded.query_json
             WHERE saved_reports.project_id = excluded.project_id",
            &rep.id,
            &rep.project_id,
            &rep.name,
            &rep.report_kind,
            &qjson,
            rep.created_at_ms,
        );
        if affected == 0 {
            return Err(MetaError::Conflict(format!(
                "saved_report '{}' belongs to a different project",
                rep.id
            )));
        }
        Ok(())
    }

    pub async fn list_saved_reports(
        &self,
        project_id: &str,
    ) -> Result<Vec<SavedReport>, MetaError> {
        let rows = db_map_all!(
            self,
            "SELECT id, project_id, name, report_kind, query_json, created_at_ms
             FROM saved_reports WHERE project_id = ? ORDER BY created_at_ms DESC",
            |r| {
                let qstr: String = r.get("query_json");
                SavedReport {
                    id: r.get("id"),
                    project_id: r.get("project_id"),
                    name: r.get("name"),
                    report_kind: r.get("report_kind"),
                    query_json: serde_json::from_str(&qstr).unwrap_or(serde_json::Value::Null),
                    created_at_ms: r.get("created_at_ms"),
                }
            },
            project_id,
        );
        Ok(rows)
    }

    pub async fn upsert_dashboard(&self, d: &Dashboard) -> Result<(), MetaError> {
        let rjson = serde_json::to_string(&d.report_ids).unwrap_or_else(|_| "[]".into());
        let affected = db_exec!(
            self,
            "INSERT INTO dashboards (id, project_id, name, report_ids_json, created_at_ms)
             VALUES (?, ?, ?, ?, ?)
             ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                report_ids_json = excluded.report_ids_json
             WHERE dashboards.project_id = excluded.project_id",
            &d.id,
            &d.project_id,
            &d.name,
            &rjson,
            d.created_at_ms,
        );
        if affected == 0 {
            return Err(MetaError::Conflict(format!(
                "dashboard '{}' belongs to a different project",
                d.id
            )));
        }
        Ok(())
    }

    pub async fn list_dashboards(&self, project_id: &str) -> Result<Vec<Dashboard>, MetaError> {
        let rows = db_map_all!(
            self,
            "SELECT id, project_id, name, report_ids_json, created_at_ms
             FROM dashboards WHERE project_id = ? ORDER BY created_at_ms DESC",
            |r| {
                let rstr: String = r.get("report_ids_json");
                Dashboard {
                    id: r.get("id"),
                    project_id: r.get("project_id"),
                    name: r.get("name"),
                    report_ids: serde_json::from_str(&rstr).unwrap_or_default(),
                    created_at_ms: r.get("created_at_ms"),
                }
            },
            project_id,
        );
        Ok(rows)
    }
}
