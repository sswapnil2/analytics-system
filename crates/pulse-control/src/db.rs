use std::path::Path;
use std::str::FromStr;

use pulse_exec::{CustomEventDef, CustomPropertyDef, LookupTableDef};
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions};
use sqlx::Row;

use crate::models::{
    ApiKeyRecord, CohortRecord, Dashboard, Experiment, FeatureFlag, LexiconEvent, LexiconProperty,
    LookupTableSummary, Organization, Project, SavedReport,
};

#[derive(Clone)]
pub enum MetaStore {
    Sqlite(SqlitePool),
    Postgres(PgPool),
}

const SCHEMA_STATEMENTS: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS organizations (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        slug TEXT NOT NULL,
        created_at_ms BIGINT NOT NULL
    )",
    "CREATE TABLE IF NOT EXISTS projects (
        id TEXT PRIMARY KEY,
        org_id TEXT NOT NULL,
        name TEXT NOT NULL,
        timezone TEXT NOT NULL,
        created_at_ms BIGINT NOT NULL
    )",
    "CREATE TABLE IF NOT EXISTS api_keys (
        key_secret TEXT PRIMARY KEY,
        project_id TEXT NOT NULL,
        label TEXT NOT NULL,
        role TEXT NOT NULL,
        created_at_ms BIGINT NOT NULL,
        revoked INTEGER NOT NULL DEFAULT 0
    )",
    "CREATE TABLE IF NOT EXISTS lexicon_events (
        project_id TEXT NOT NULL,
        name TEXT NOT NULL,
        display_name TEXT NOT NULL,
        description TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'active',
        owner TEXT NOT NULL DEFAULT 'product',
        volume_24h BIGINT NOT NULL DEFAULT 0,
        updated_at_ms BIGINT NOT NULL,
        PRIMARY KEY (project_id, name)
    )",
    "CREATE TABLE IF NOT EXISTS lexicon_properties (
        project_id TEXT NOT NULL,
        name TEXT NOT NULL,
        prop_scope TEXT NOT NULL DEFAULT 'event',
        inferred_types TEXT NOT NULL DEFAULT 'string',
        status TEXT NOT NULL DEFAULT 'active',
        pii_masked INTEGER NOT NULL DEFAULT 0,
        updated_at_ms BIGINT NOT NULL,
        PRIMARY KEY (project_id, name)
    )",
    "CREATE TABLE IF NOT EXISTS custom_events (
        id TEXT PRIMARY KEY,
        project_id TEXT NOT NULL,
        name TEXT NOT NULL,
        definition_json TEXT NOT NULL,
        created_at_ms BIGINT NOT NULL
    )",
    "CREATE TABLE IF NOT EXISTS custom_properties (
        id TEXT PRIMARY KEY,
        project_id TEXT NOT NULL,
        name TEXT NOT NULL,
        sql_expr TEXT NOT NULL,
        referenced_props_json TEXT NOT NULL,
        created_at_ms BIGINT NOT NULL
    )",
    "CREATE TABLE IF NOT EXISTS lookup_tables (
        id TEXT PRIMARY KEY,
        project_id TEXT NOT NULL,
        name TEXT NOT NULL,
        join_prop TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        row_count BIGINT NOT NULL,
        updated_at_ms BIGINT NOT NULL
    )",
    "CREATE TABLE IF NOT EXISTS cohorts (
        id TEXT PRIMARY KEY,
        project_id TEXT NOT NULL,
        name TEXT NOT NULL,
        description TEXT NOT NULL,
        definition_json TEXT NOT NULL,
        created_at_ms BIGINT NOT NULL
    )",
    "CREATE TABLE IF NOT EXISTS saved_reports (
        id TEXT PRIMARY KEY,
        project_id TEXT NOT NULL,
        name TEXT NOT NULL,
        report_kind TEXT NOT NULL,
        query_json TEXT NOT NULL,
        created_at_ms BIGINT NOT NULL
    )",
    "CREATE TABLE IF NOT EXISTS dashboards (
        id TEXT PRIMARY KEY,
        project_id TEXT NOT NULL,
        name TEXT NOT NULL,
        report_ids_json TEXT NOT NULL,
        created_at_ms BIGINT NOT NULL
    )",
    "CREATE TABLE IF NOT EXISTS feature_flags (
        id TEXT PRIMARY KEY,
        project_id TEXT NOT NULL,
        flag_key TEXT NOT NULL,
        description TEXT NOT NULL,
        enabled INTEGER NOT NULL DEFAULT 1,
        rollout_pct DOUBLE PRECISION NOT NULL DEFAULT 100.0,
        targeting_cohort_id TEXT,
        variants_json TEXT NOT NULL,
        updated_at_ms BIGINT NOT NULL
    )",
    "CREATE TABLE IF NOT EXISTS experiments (
        id TEXT PRIMARY KEY,
        project_id TEXT NOT NULL,
        name TEXT NOT NULL,
        flag_id TEXT NOT NULL,
        flag_key TEXT NOT NULL,
        hypothesis TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'running',
        start_ts_ms BIGINT NOT NULL,
        end_ts_ms BIGINT NOT NULL,
        primary_metric_json TEXT NOT NULL,
        guardrail_metrics_json TEXT NOT NULL,
        created_at_ms BIGINT NOT NULL
    )",
];

impl MetaStore {
    /// Connects to `DATABASE_URL` (`postgres://...` or `sqlite://...`) or falls back to local `<data_dir>/pulse_meta.db`.
    pub async fn connect_auto(data_dir: &Path) -> Result<Self, sqlx::Error> {
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

    pub async fn connect_url(url: &str) -> Result<Self, sqlx::Error> {
        let store = if url.starts_with("postgres://") || url.starts_with("postgresql://") {
            let pool = PgPoolOptions::new()
                .max_connections(16)
                .connect(url)
                .await?;
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
            Self::Sqlite(pool)
        };
        store.run_migrations().await?;
        Ok(store)
    }

    async fn run_migrations(&self) -> Result<(), sqlx::Error> {
        for stmt in SCHEMA_STATEMENTS {
            match self {
                MetaStore::Sqlite(p) => {
                    sqlx::query(stmt).execute(p).await?;
                }
                MetaStore::Postgres(p) => {
                    sqlx::query(stmt).execute(p).await?;
                }
            }
        }
        Ok(())
    }

    // ------------------- Organizations & Projects -------------------

    pub async fn upsert_org(&self, org: &Organization) -> Result<(), sqlx::Error> {
        match self {
            MetaStore::Sqlite(p) => {
                sqlx::query(
                    "INSERT INTO organizations (id, name, slug, created_at_ms) VALUES (?, ?, ?, ?)
                     ON CONFLICT(id) DO UPDATE SET name = excluded.name, slug = excluded.slug",
                )
                .bind(&org.id)
                .bind(&org.name)
                .bind(&org.slug)
                .bind(org.created_at_ms)
                .execute(p)
                .await?;
            }
            MetaStore::Postgres(p) => {
                sqlx::query(
                    "INSERT INTO organizations (id, name, slug, created_at_ms) VALUES ($1, $2, $3, $4)
                     ON CONFLICT(id) DO UPDATE SET name = EXCLUDED.name, slug = EXCLUDED.slug",
                )
                .bind(&org.id)
                .bind(&org.name)
                .bind(&org.slug)
                .bind(org.created_at_ms)
                .execute(p)
                .await?;
            }
        }
        Ok(())
    }

    pub async fn upsert_project(&self, proj: &Project) -> Result<(), sqlx::Error> {
        match self {
            MetaStore::Sqlite(p) => {
                sqlx::query(
                    "INSERT INTO projects (id, org_id, name, timezone, created_at_ms) VALUES (?, ?, ?, ?, ?)
                     ON CONFLICT(id) DO UPDATE SET name = excluded.name, timezone = excluded.timezone",
                )
                .bind(&proj.id)
                .bind(&proj.org_id)
                .bind(&proj.name)
                .bind(&proj.timezone)
                .bind(proj.created_at_ms)
                .execute(p)
                .await?;
            }
            MetaStore::Postgres(p) => {
                sqlx::query(
                    "INSERT INTO projects (id, org_id, name, timezone, created_at_ms) VALUES ($1, $2, $3, $4, $5)
                     ON CONFLICT(id) DO UPDATE SET name = EXCLUDED.name, timezone = EXCLUDED.timezone",
                )
                .bind(&proj.id)
                .bind(&proj.org_id)
                .bind(&proj.name)
                .bind(&proj.timezone)
                .bind(proj.created_at_ms)
                .execute(p)
                .await?;
            }
        }
        Ok(())
    }

    pub async fn list_projects(&self) -> Result<Vec<Project>, sqlx::Error> {
        match self {
            MetaStore::Sqlite(p) => {
                let rows = sqlx::query(
                    "SELECT id, org_id, name, timezone, created_at_ms FROM projects ORDER BY created_at_ms ASC",
                )
                .fetch_all(p)
                .await?;
                Ok(rows
                    .into_iter()
                    .map(|r| Project {
                        id: r.get("id"),
                        org_id: r.get("org_id"),
                        name: r.get("name"),
                        timezone: r.get("timezone"),
                        created_at_ms: r.get("created_at_ms"),
                    })
                    .collect())
            }
            MetaStore::Postgres(p) => {
                let rows = sqlx::query(
                    "SELECT id, org_id, name, timezone, created_at_ms FROM projects ORDER BY created_at_ms ASC",
                )
                .fetch_all(p)
                .await?;
                Ok(rows
                    .into_iter()
                    .map(|r| Project {
                        id: r.get("id"),
                        org_id: r.get("org_id"),
                        name: r.get("name"),
                        timezone: r.get("timezone"),
                        created_at_ms: r.get("created_at_ms"),
                    })
                    .collect())
            }
        }
    }

    pub async fn upsert_api_key(&self, rec: &ApiKeyRecord) -> Result<(), sqlx::Error> {
        let rev = i32::from(rec.revoked);
        match self {
            MetaStore::Sqlite(p) => {
                sqlx::query(
                    "INSERT INTO api_keys (key_secret, project_id, label, role, created_at_ms, revoked)
                     VALUES (?, ?, ?, ?, ?, ?)
                     ON CONFLICT(key_secret) DO UPDATE SET revoked = excluded.revoked, label = excluded.label",
                )
                .bind(&rec.key_secret)
                .bind(&rec.project_id)
                .bind(&rec.label)
                .bind(&rec.role)
                .bind(rec.created_at_ms)
                .bind(rev)
                .execute(p)
                .await?;
            }
            MetaStore::Postgres(p) => {
                sqlx::query(
                    "INSERT INTO api_keys (key_secret, project_id, label, role, created_at_ms, revoked)
                     VALUES ($1, $2, $3, $4, $5, $6)
                     ON CONFLICT(key_secret) DO UPDATE SET revoked = EXCLUDED.revoked, label = EXCLUDED.label",
                )
                .bind(&rec.key_secret)
                .bind(&rec.project_id)
                .bind(&rec.label)
                .bind(&rec.role)
                .bind(rec.created_at_ms)
                .bind(rev)
                .execute(p)
                .await?;
            }
        }
        Ok(())
    }

    pub async fn list_api_keys(
        &self,
        project_id: Option<&str>,
    ) -> Result<Vec<ApiKeyRecord>, sqlx::Error> {
        match (self, project_id) {
            (MetaStore::Sqlite(p), Some(pid)) => {
                let rows = sqlx::query(
                    "SELECT key_secret, project_id, label, role, created_at_ms, revoked FROM api_keys WHERE project_id = ?",
                )
                .bind(pid)
                .fetch_all(p)
                .await?;
                Ok(rows
                    .into_iter()
                    .map(|r| ApiKeyRecord {
                        key_secret: r.get("key_secret"),
                        project_id: r.get("project_id"),
                        label: r.get("label"),
                        role: r.get("role"),
                        created_at_ms: r.get("created_at_ms"),
                        revoked: r.get::<i32, _>("revoked") != 0,
                    })
                    .collect())
            }
            (MetaStore::Sqlite(p), None) => {
                let rows = sqlx::query(
                    "SELECT key_secret, project_id, label, role, created_at_ms, revoked FROM api_keys",
                )
                .fetch_all(p)
                .await?;
                Ok(rows
                    .into_iter()
                    .map(|r| ApiKeyRecord {
                        key_secret: r.get("key_secret"),
                        project_id: r.get("project_id"),
                        label: r.get("label"),
                        role: r.get("role"),
                        created_at_ms: r.get("created_at_ms"),
                        revoked: r.get::<i32, _>("revoked") != 0,
                    })
                    .collect())
            }
            (MetaStore::Postgres(p), Some(pid)) => {
                let rows = sqlx::query(
                    "SELECT key_secret, project_id, label, role, created_at_ms, revoked FROM api_keys WHERE project_id = $1",
                )
                .bind(pid)
                .fetch_all(p)
                .await?;
                Ok(rows
                    .into_iter()
                    .map(|r| ApiKeyRecord {
                        key_secret: r.get("key_secret"),
                        project_id: r.get("project_id"),
                        label: r.get("label"),
                        role: r.get("role"),
                        created_at_ms: r.get("created_at_ms"),
                        revoked: r.get::<i32, _>("revoked") != 0,
                    })
                    .collect())
            }
            (MetaStore::Postgres(p), None) => {
                let rows = sqlx::query(
                    "SELECT key_secret, project_id, label, role, created_at_ms, revoked FROM api_keys",
                )
                .fetch_all(p)
                .await?;
                Ok(rows
                    .into_iter()
                    .map(|r| ApiKeyRecord {
                        key_secret: r.get("key_secret"),
                        project_id: r.get("project_id"),
                        label: r.get("label"),
                        role: r.get("role"),
                        created_at_ms: r.get("created_at_ms"),
                        revoked: r.get::<i32, _>("revoked") != 0,
                    })
                    .collect())
            }
        }
    }

    // ------------------- Lexicon Governance -------------------

    pub async fn upsert_lexicon_event(&self, ev: &LexiconEvent) -> Result<(), sqlx::Error> {
        match self {
            MetaStore::Sqlite(p) => {
                sqlx::query(
                    "INSERT INTO lexicon_events (project_id, name, display_name, description, status, owner, volume_24h, updated_at_ms)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                     ON CONFLICT(project_id, name) DO UPDATE SET
                        display_name = excluded.display_name,
                        description = excluded.description,
                        status = excluded.status,
                        owner = excluded.owner,
                        volume_24h = MAX(lexicon_events.volume_24h, excluded.volume_24h),
                        updated_at_ms = excluded.updated_at_ms",
                )
                .bind(&ev.project_id)
                .bind(&ev.name)
                .bind(&ev.display_name)
                .bind(&ev.description)
                .bind(&ev.status)
                .bind(&ev.owner)
                .bind(ev.volume_24h)
                .bind(ev.updated_at_ms)
                .execute(p)
                .await?;
            }
            MetaStore::Postgres(p) => {
                sqlx::query(
                    "INSERT INTO lexicon_events (project_id, name, display_name, description, status, owner, volume_24h, updated_at_ms)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                     ON CONFLICT(project_id, name) DO UPDATE SET
                        display_name = EXCLUDED.display_name,
                        description = EXCLUDED.description,
                        status = EXCLUDED.status,
                        owner = EXCLUDED.owner,
                        volume_24h = GREATEST(lexicon_events.volume_24h, EXCLUDED.volume_24h),
                        updated_at_ms = EXCLUDED.updated_at_ms",
                )
                .bind(&ev.project_id)
                .bind(&ev.name)
                .bind(&ev.display_name)
                .bind(&ev.description)
                .bind(&ev.status)
                .bind(&ev.owner)
                .bind(ev.volume_24h)
                .bind(ev.updated_at_ms)
                .execute(p)
                .await?;
            }
        }
        Ok(())
    }

    pub async fn increment_lexicon_discovery(
        &self,
        project_id: &str,
        event_name: &str,
        add_vol: i64,
        props: &[(String, &'static str)],
        now_ms: i64,
    ) -> Result<(), sqlx::Error> {
        match self {
            MetaStore::Sqlite(p) => {
                sqlx::query(
                    "INSERT INTO lexicon_events (project_id, name, display_name, description, status, owner, volume_24h, updated_at_ms)
                     VALUES (?, ?, ?, 'Auto-discovered telemetry event', 'active', 'auto', ?, ?)
                     ON CONFLICT(project_id, name) DO UPDATE SET
                        volume_24h = lexicon_events.volume_24h + excluded.volume_24h,
                        updated_at_ms = excluded.updated_at_ms",
                )
                .bind(project_id)
                .bind(event_name)
                .bind(event_name)
                .bind(add_vol)
                .bind(now_ms)
                .execute(p)
                .await?;

                for (pname, ptype) in props {
                    sqlx::query(
                        "INSERT INTO lexicon_properties (project_id, name, prop_scope, inferred_types, status, pii_masked, updated_at_ms)
                         VALUES (?, ?, 'event', ?, 'active', 0, ?)
                         ON CONFLICT(project_id, name) DO UPDATE SET updated_at_ms = excluded.updated_at_ms",
                    )
                    .bind(project_id)
                    .bind(pname)
                    .bind(*ptype)
                    .bind(now_ms)
                    .execute(p)
                    .await?;
                }
            }
            MetaStore::Postgres(p) => {
                sqlx::query(
                    "INSERT INTO lexicon_events (project_id, name, display_name, description, status, owner, volume_24h, updated_at_ms)
                     VALUES ($1, $2, $3, 'Auto-discovered telemetry event', 'active', 'auto', $4, $5)
                     ON CONFLICT(project_id, name) DO UPDATE SET
                        volume_24h = lexicon_events.volume_24h + EXCLUDED.volume_24h,
                        updated_at_ms = EXCLUDED.updated_at_ms",
                )
                .bind(project_id)
                .bind(event_name)
                .bind(event_name)
                .bind(add_vol)
                .bind(now_ms)
                .execute(p)
                .await?;

                for (pname, ptype) in props {
                    sqlx::query(
                        "INSERT INTO lexicon_properties (project_id, name, prop_scope, inferred_types, status, pii_masked, updated_at_ms)
                         VALUES ($1, $2, 'event', $3, 'active', 0, $4)
                         ON CONFLICT(project_id, name) DO UPDATE SET updated_at_ms = EXCLUDED.updated_at_ms",
                    )
                    .bind(project_id)
                    .bind(pname)
                    .bind(*ptype)
                    .bind(now_ms)
                    .execute(p)
                    .await?;
                }
            }
        }
        Ok(())
    }

    pub async fn upsert_lexicon_property(&self, prop: &LexiconProperty) -> Result<(), sqlx::Error> {
        let pii = i32::from(prop.pii_masked);
        match self {
            MetaStore::Sqlite(p) => {
                sqlx::query(
                    "INSERT INTO lexicon_properties (project_id, name, prop_scope, inferred_types, status, pii_masked, updated_at_ms)
                     VALUES (?, ?, ?, ?, ?, ?, ?)
                     ON CONFLICT(project_id, name) DO UPDATE SET
                        prop_scope = excluded.prop_scope,
                        inferred_types = excluded.inferred_types,
                        status = excluded.status,
                        pii_masked = excluded.pii_masked,
                        updated_at_ms = excluded.updated_at_ms",
                )
                .bind(&prop.project_id)
                .bind(&prop.name)
                .bind(&prop.prop_scope)
                .bind(&prop.inferred_types)
                .bind(&prop.status)
                .bind(pii)
                .bind(prop.updated_at_ms)
                .execute(p)
                .await?;
            }
            MetaStore::Postgres(p) => {
                sqlx::query(
                    "INSERT INTO lexicon_properties (project_id, name, prop_scope, inferred_types, status, pii_masked, updated_at_ms)
                     VALUES ($1, $2, $3, $4, $5, $6, $7)
                     ON CONFLICT(project_id, name) DO UPDATE SET
                        prop_scope = EXCLUDED.prop_scope,
                        inferred_types = EXCLUDED.inferred_types,
                        status = EXCLUDED.status,
                        pii_masked = EXCLUDED.pii_masked,
                        updated_at_ms = EXCLUDED.updated_at_ms",
                )
                .bind(&prop.project_id)
                .bind(&prop.name)
                .bind(&prop.prop_scope)
                .bind(&prop.inferred_types)
                .bind(&prop.status)
                .bind(pii)
                .bind(prop.updated_at_ms)
                .execute(p)
                .await?;
            }
        }
        Ok(())
    }

    pub async fn list_lexicon_events(
        &self,
        project_id: &str,
    ) -> Result<Vec<LexiconEvent>, sqlx::Error> {
        let rows = match self {
            MetaStore::Sqlite(p) => {
                let r = sqlx::query(
                    "SELECT project_id, name, display_name, description, status, owner, volume_24h, updated_at_ms
                     FROM lexicon_events WHERE project_id = ? ORDER BY volume_24h DESC, name ASC",
                )
                .bind(project_id)
                .fetch_all(p)
                .await?;
                return Ok(r
                    .into_iter()
                    .map(|row| LexiconEvent {
                        project_id: row.get("project_id"),
                        name: row.get("name"),
                        display_name: row.get("display_name"),
                        description: row.get("description"),
                        status: row.get("status"),
                        owner: row.get("owner"),
                        volume_24h: row.get("volume_24h"),
                        updated_at_ms: row.get("updated_at_ms"),
                    })
                    .collect());
            }
            MetaStore::Postgres(p) => {
                sqlx::query(
                    "SELECT project_id, name, display_name, description, status, owner, volume_24h, updated_at_ms
                     FROM lexicon_events WHERE project_id = $1 ORDER BY volume_24h DESC, name ASC",
                )
                .bind(project_id)
                .fetch_all(p)
                .await?
            }
        };
        Ok(rows
            .into_iter()
            .map(|row| LexiconEvent {
                project_id: row.get("project_id"),
                name: row.get("name"),
                display_name: row.get("display_name"),
                description: row.get("description"),
                status: row.get("status"),
                owner: row.get("owner"),
                volume_24h: row.get("volume_24h"),
                updated_at_ms: row.get("updated_at_ms"),
            })
            .collect())
    }

    pub async fn list_lexicon_properties(
        &self,
        project_id: &str,
    ) -> Result<Vec<LexiconProperty>, sqlx::Error> {
        match self {
            MetaStore::Sqlite(p) => {
                let r = sqlx::query(
                    "SELECT project_id, name, prop_scope, inferred_types, status, pii_masked, updated_at_ms
                     FROM lexicon_properties WHERE project_id = ? ORDER BY name ASC",
                )
                .bind(project_id)
                .fetch_all(p)
                .await?;
                Ok(r.into_iter()
                    .map(|row| LexiconProperty {
                        project_id: row.get("project_id"),
                        name: row.get("name"),
                        prop_scope: row.get("prop_scope"),
                        inferred_types: row.get("inferred_types"),
                        status: row.get("status"),
                        pii_masked: row.get::<i32, _>("pii_masked") != 0,
                        updated_at_ms: row.get("updated_at_ms"),
                    })
                    .collect())
            }
            MetaStore::Postgres(p) => {
                let r = sqlx::query(
                    "SELECT project_id, name, prop_scope, inferred_types, status, pii_masked, updated_at_ms
                     FROM lexicon_properties WHERE project_id = $1 ORDER BY name ASC",
                )
                .bind(project_id)
                .fetch_all(p)
                .await?;
                Ok(r.into_iter()
                    .map(|row| LexiconProperty {
                        project_id: row.get("project_id"),
                        name: row.get("name"),
                        prop_scope: row.get("prop_scope"),
                        inferred_types: row.get("inferred_types"),
                        status: row.get("status"),
                        pii_masked: row.get::<i32, _>("pii_masked") != 0,
                        updated_at_ms: row.get("updated_at_ms"),
                    })
                    .collect())
            }
        }
    }

    // ------------------- Cohorts, Custom Events/Props & Lookup Tables -------------------

    pub async fn upsert_cohort(&self, c: &CohortRecord) -> Result<(), sqlx::Error> {
        let def_json = serde_json::to_string(&c.definition).unwrap_or_default();
        match self {
            MetaStore::Sqlite(p) => {
                sqlx::query(
                    "INSERT INTO cohorts (id, project_id, name, description, definition_json, created_at_ms)
                     VALUES (?, ?, ?, ?, ?, ?)
                     ON CONFLICT(id) DO UPDATE SET name = excluded.name, description = excluded.description, definition_json = excluded.definition_json",
                )
                .bind(&c.id)
                .bind(&c.project_id)
                .bind(&c.name)
                .bind(&c.description)
                .bind(&def_json)
                .bind(c.created_at_ms)
                .execute(p)
                .await?;
            }
            MetaStore::Postgres(p) => {
                sqlx::query(
                    "INSERT INTO cohorts (id, project_id, name, description, definition_json, created_at_ms)
                     VALUES ($1, $2, $3, $4, $5, $6)
                     ON CONFLICT(id) DO UPDATE SET name = EXCLUDED.name, description = EXCLUDED.description, definition_json = EXCLUDED.definition_json",
                )
                .bind(&c.id)
                .bind(&c.project_id)
                .bind(&c.name)
                .bind(&c.description)
                .bind(&def_json)
                .bind(c.created_at_ms)
                .execute(p)
                .await?;
            }
        }
        Ok(())
    }

    pub async fn list_cohorts(&self, project_id: &str) -> Result<Vec<CohortRecord>, sqlx::Error> {
        let raw: Vec<(String, String, String, String, String, i64)> = match self {
            MetaStore::Sqlite(p) => {
                sqlx::query_as(
                    "SELECT id, project_id, name, description, definition_json, created_at_ms
                     FROM cohorts WHERE project_id = ? ORDER BY created_at_ms DESC",
                )
                .bind(project_id)
                .fetch_all(p)
                .await?
            }
            MetaStore::Postgres(p) => {
                sqlx::query_as(
                    "SELECT id, project_id, name, description, definition_json, created_at_ms
                     FROM cohorts WHERE project_id = $1 ORDER BY created_at_ms DESC",
                )
                .bind(project_id)
                .fetch_all(p)
                .await?
            }
        };
        let mut out = Vec::with_capacity(raw.len());
        for (id, pid, name, desc, djson, ts) in raw {
            if let Ok(def) = serde_json::from_str(&djson) {
                out.push(CohortRecord {
                    id,
                    project_id: pid,
                    name,
                    description: desc,
                    definition: def,
                    created_at_ms: ts,
                });
            }
        }
        Ok(out)
    }

    pub async fn upsert_lookup_table(
        &self,
        id: &str,
        project_id: &str,
        def: &LookupTableDef,
        now_ms: i64,
    ) -> Result<(), sqlx::Error> {
        let payload_json = serde_json::to_string(def).unwrap_or_default();
        let row_count = def.entries.len() as i64;
        match self {
            MetaStore::Sqlite(p) => {
                sqlx::query(
                    "INSERT INTO lookup_tables (id, project_id, name, join_prop, payload_json, row_count, updated_at_ms)
                     VALUES (?, ?, ?, ?, ?, ?, ?)
                     ON CONFLICT(id) DO UPDATE SET join_prop = excluded.join_prop, payload_json = excluded.payload_json, row_count = excluded.row_count, updated_at_ms = excluded.updated_at_ms",
                )
                .bind(id)
                .bind(project_id)
                .bind(&def.name)
                .bind(&def.join_prop)
                .bind(&payload_json)
                .bind(row_count)
                .bind(now_ms)
                .execute(p)
                .await?;
            }
            MetaStore::Postgres(p) => {
                sqlx::query(
                    "INSERT INTO lookup_tables (id, project_id, name, join_prop, payload_json, row_count, updated_at_ms)
                     VALUES ($1, $2, $3, $4, $5, $6, $7)
                     ON CONFLICT(id) DO UPDATE SET join_prop = EXCLUDED.join_prop, payload_json = EXCLUDED.payload_json, row_count = EXCLUDED.row_count, updated_at_ms = EXCLUDED.updated_at_ms",
                )
                .bind(id)
                .bind(project_id)
                .bind(&def.name)
                .bind(&def.join_prop)
                .bind(&payload_json)
                .bind(row_count)
                .bind(now_ms)
                .execute(p)
                .await?;
            }
        }
        Ok(())
    }

    pub async fn list_lookup_tables(
        &self,
        project_id: &str,
    ) -> Result<Vec<(LookupTableSummary, LookupTableDef)>, sqlx::Error> {
        let raw: Vec<(String, String, String, String, String, i64, i64)> =
            match self {
                MetaStore::Sqlite(p) => sqlx::query_as(
                    "SELECT id, project_id, name, join_prop, payload_json, row_count, updated_at_ms
                     FROM lookup_tables WHERE project_id = ? ORDER BY updated_at_ms DESC",
                )
                .bind(project_id)
                .fetch_all(p)
                .await?,
                MetaStore::Postgres(p) => sqlx::query_as(
                    "SELECT id, project_id, name, join_prop, payload_json, row_count, updated_at_ms
                     FROM lookup_tables WHERE project_id = $1 ORDER BY updated_at_ms DESC",
                )
                .bind(project_id)
                .fetch_all(p)
                .await?,
            };
        let mut out = Vec::new();
        for (id, pid, name, jprop, pjson, rcnt, uts) in raw {
            if let Ok(def) = serde_json::from_str::<LookupTableDef>(&pjson) {
                let mut cols: Vec<String> = def
                    .entries
                    .values()
                    .next()
                    .map(|m| m.keys().cloned().collect())
                    .unwrap_or_default();
                cols.sort();
                out.push((
                    LookupTableSummary {
                        id,
                        project_id: pid,
                        name,
                        join_prop: jprop,
                        row_count: rcnt as usize,
                        sample_columns: cols,
                        updated_at_ms: uts,
                    },
                    def,
                ));
            }
        }
        Ok(out)
    }

    pub async fn upsert_custom_event(
        &self,
        id: &str,
        project_id: &str,
        def: &CustomEventDef,
        now_ms: i64,
    ) -> Result<(), sqlx::Error> {
        let djson = serde_json::to_string(def).unwrap_or_default();
        match self {
            MetaStore::Sqlite(p) => {
                sqlx::query(
                    "INSERT INTO custom_events (id, project_id, name, definition_json, created_at_ms)
                     VALUES (?, ?, ?, ?, ?)
                     ON CONFLICT(id) DO UPDATE SET definition_json = excluded.definition_json",
                )
                .bind(id)
                .bind(project_id)
                .bind(&def.name)
                .bind(&djson)
                .bind(now_ms)
                .execute(p)
                .await?;
            }
            MetaStore::Postgres(p) => {
                sqlx::query(
                    "INSERT INTO custom_events (id, project_id, name, definition_json, created_at_ms)
                     VALUES ($1, $2, $3, $4, $5)
                     ON CONFLICT(id) DO UPDATE SET definition_json = EXCLUDED.definition_json",
                )
                .bind(id)
                .bind(project_id)
                .bind(&def.name)
                .bind(&djson)
                .bind(now_ms)
                .execute(p)
                .await?;
            }
        }
        Ok(())
    }

    pub async fn list_custom_events(
        &self,
        project_id: &str,
    ) -> Result<Vec<CustomEventDef>, sqlx::Error> {
        let raw: Vec<(String,)> = match self {
            MetaStore::Sqlite(p) => {
                sqlx::query_as(
                    "SELECT definition_json FROM custom_events WHERE project_id = ? ORDER BY created_at_ms ASC",
                )
                .bind(project_id)
                .fetch_all(p)
                .await?
            }
            MetaStore::Postgres(p) => {
                sqlx::query_as(
                    "SELECT definition_json FROM custom_events WHERE project_id = $1 ORDER BY created_at_ms ASC",
                )
                .bind(project_id)
                .fetch_all(p)
                .await?
            }
        };
        Ok(raw
            .into_iter()
            .filter_map(|(s,)| serde_json::from_str(&s).ok())
            .collect())
    }

    pub async fn upsert_custom_property(
        &self,
        id: &str,
        project_id: &str,
        def: &CustomPropertyDef,
        now_ms: i64,
    ) -> Result<(), sqlx::Error> {
        let refs_json = serde_json::to_string(&def.referenced_props).unwrap_or_default();
        match self {
            MetaStore::Sqlite(p) => {
                sqlx::query(
                    "INSERT INTO custom_properties (id, project_id, name, sql_expr, referenced_props_json, created_at_ms)
                     VALUES (?, ?, ?, ?, ?, ?)
                     ON CONFLICT(id) DO UPDATE SET sql_expr = excluded.sql_expr, referenced_props_json = excluded.referenced_props_json",
                )
                .bind(id)
                .bind(project_id)
                .bind(&def.name)
                .bind(&def.sql_expr)
                .bind(&refs_json)
                .bind(now_ms)
                .execute(p)
                .await?;
            }
            MetaStore::Postgres(p) => {
                sqlx::query(
                    "INSERT INTO custom_properties (id, project_id, name, sql_expr, referenced_props_json, created_at_ms)
                     VALUES ($1, $2, $3, $4, $5, $6)
                     ON CONFLICT(id) DO UPDATE SET sql_expr = EXCLUDED.sql_expr, referenced_props_json = EXCLUDED.referenced_props_json",
                )
                .bind(id)
                .bind(project_id)
                .bind(&def.name)
                .bind(&def.sql_expr)
                .bind(&refs_json)
                .bind(now_ms)
                .execute(p)
                .await?;
            }
        }
        Ok(())
    }

    pub async fn list_custom_properties(
        &self,
        project_id: &str,
    ) -> Result<Vec<CustomPropertyDef>, sqlx::Error> {
        let raw: Vec<(String, String, String)> = match self {
            MetaStore::Sqlite(p) => {
                sqlx::query_as(
                    "SELECT name, sql_expr, referenced_props_json FROM custom_properties WHERE project_id = ? ORDER BY created_at_ms ASC",
                )
                .bind(project_id)
                .fetch_all(p)
                .await?
            }
            MetaStore::Postgres(p) => {
                sqlx::query_as(
                    "SELECT name, sql_expr, referenced_props_json FROM custom_properties WHERE project_id = $1 ORDER BY created_at_ms ASC",
                )
                .bind(project_id)
                .fetch_all(p)
                .await?
            }
        };
        Ok(raw
            .into_iter()
            .map(|(name, sql_expr, rj)| CustomPropertyDef {
                name,
                sql_expr,
                referenced_props: serde_json::from_str(&rj).unwrap_or_default(),
            })
            .collect())
    }

    // ------------------- Feature Flags & Experiments -------------------

    pub async fn upsert_feature_flag(&self, f: &FeatureFlag) -> Result<(), sqlx::Error> {
        let en = i32::from(f.enabled);
        let vjson = serde_json::to_string(&f.variants).unwrap_or_default();
        match self {
            MetaStore::Sqlite(p) => {
                sqlx::query(
                    "INSERT INTO feature_flags (id, project_id, flag_key, description, enabled, rollout_pct, targeting_cohort_id, variants_json, updated_at_ms)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                     ON CONFLICT(id) DO UPDATE SET
                        enabled = excluded.enabled,
                        rollout_pct = excluded.rollout_pct,
                        targeting_cohort_id = excluded.targeting_cohort_id,
                        variants_json = excluded.variants_json,
                        updated_at_ms = excluded.updated_at_ms",
                )
                .bind(&f.id)
                .bind(&f.project_id)
                .bind(&f.key)
                .bind(&f.description)
                .bind(en)
                .bind(f.rollout_pct)
                .bind(&f.targeting_cohort_id)
                .bind(&vjson)
                .bind(f.updated_at_ms)
                .execute(p)
                .await?;
            }
            MetaStore::Postgres(p) => {
                sqlx::query(
                    "INSERT INTO feature_flags (id, project_id, flag_key, description, enabled, rollout_pct, targeting_cohort_id, variants_json, updated_at_ms)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                     ON CONFLICT(id) DO UPDATE SET
                        enabled = EXCLUDED.enabled,
                        rollout_pct = EXCLUDED.rollout_pct,
                        targeting_cohort_id = EXCLUDED.targeting_cohort_id,
                        variants_json = EXCLUDED.variants_json,
                        updated_at_ms = EXCLUDED.updated_at_ms",
                )
                .bind(&f.id)
                .bind(&f.project_id)
                .bind(&f.key)
                .bind(&f.description)
                .bind(en)
                .bind(f.rollout_pct)
                .bind(&f.targeting_cohort_id)
                .bind(&vjson)
                .bind(f.updated_at_ms)
                .execute(p)
                .await?;
            }
        }
        Ok(())
    }

    pub async fn list_feature_flags(
        &self,
        project_id: &str,
    ) -> Result<Vec<FeatureFlag>, sqlx::Error> {
        type FlagRow = (
            String,
            String,
            String,
            String,
            i32,
            f64,
            Option<String>,
            String,
            i64,
        );
        let raw: Vec<FlagRow> = match self {
            MetaStore::Sqlite(p) => {
                sqlx::query_as(
                    "SELECT id, project_id, flag_key, description, enabled, rollout_pct, targeting_cohort_id, variants_json, updated_at_ms
                     FROM feature_flags WHERE project_id = ? ORDER BY flag_key ASC",
                )
                .bind(project_id)
                .fetch_all(p)
                .await?
            }
            MetaStore::Postgres(p) => {
                sqlx::query_as(
                    "SELECT id, project_id, flag_key, description, enabled, rollout_pct, targeting_cohort_id, variants_json, updated_at_ms
                     FROM feature_flags WHERE project_id = $1 ORDER BY flag_key ASC",
                )
                .bind(project_id)
                .fetch_all(p)
                .await?
            }
        };
        Ok(raw
            .into_iter()
            .map(|(id, pid, key, desc, en, pct, cid, vj, uts)| FeatureFlag {
                id,
                project_id: pid,
                key,
                description: desc,
                enabled: en != 0,
                rollout_pct: pct,
                targeting_cohort_id: cid,
                variants: serde_json::from_str(&vj).unwrap_or_default(),
                updated_at_ms: uts,
            })
            .collect())
    }

    pub async fn upsert_experiment(&self, exp: &Experiment) -> Result<(), sqlx::Error> {
        let pm = serde_json::to_string(&exp.primary_metric).unwrap_or_default();
        let gm = serde_json::to_string(&exp.guardrail_metrics).unwrap_or_default();
        match self {
            MetaStore::Sqlite(p) => {
                sqlx::query(
                    "INSERT INTO experiments (id, project_id, name, flag_id, flag_key, hypothesis, status, start_ts_ms, end_ts_ms, primary_metric_json, guardrail_metrics_json, created_at_ms)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                     ON CONFLICT(id) DO UPDATE SET
                        name = excluded.name,
                        status = excluded.status,
                        hypothesis = excluded.hypothesis,
                        end_ts_ms = excluded.end_ts_ms,
                        primary_metric_json = excluded.primary_metric_json,
                        guardrail_metrics_json = excluded.guardrail_metrics_json",
                )
                .bind(&exp.id)
                .bind(&exp.project_id)
                .bind(&exp.name)
                .bind(&exp.flag_id)
                .bind(&exp.flag_key)
                .bind(&exp.hypothesis)
                .bind(&exp.status)
                .bind(exp.start_ts_ms)
                .bind(exp.end_ts_ms)
                .bind(&pm)
                .bind(&gm)
                .bind(exp.created_at_ms)
                .execute(p)
                .await?;
            }
            MetaStore::Postgres(p) => {
                sqlx::query(
                    "INSERT INTO experiments (id, project_id, name, flag_id, flag_key, hypothesis, status, start_ts_ms, end_ts_ms, primary_metric_json, guardrail_metrics_json, created_at_ms)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                     ON CONFLICT(id) DO UPDATE SET
                        name = EXCLUDED.name,
                        status = EXCLUDED.status,
                        hypothesis = EXCLUDED.hypothesis,
                        end_ts_ms = EXCLUDED.end_ts_ms,
                        primary_metric_json = EXCLUDED.primary_metric_json,
                        guardrail_metrics_json = EXCLUDED.guardrail_metrics_json",
                )
                .bind(&exp.id)
                .bind(&exp.project_id)
                .bind(&exp.name)
                .bind(&exp.flag_id)
                .bind(&exp.flag_key)
                .bind(&exp.hypothesis)
                .bind(&exp.status)
                .bind(exp.start_ts_ms)
                .bind(exp.end_ts_ms)
                .bind(&pm)
                .bind(&gm)
                .bind(exp.created_at_ms)
                .execute(p)
                .await?;
            }
        }
        Ok(())
    }

    pub async fn list_experiments(&self, project_id: &str) -> Result<Vec<Experiment>, sqlx::Error> {
        type ExpRow = (
            String,
            String,
            String,
            String,
            String,
            String,
            String,
            i64,
            i64,
            String,
            String,
            i64,
        );
        let raw: Vec<ExpRow> = match self {
            MetaStore::Sqlite(p) => {
                sqlx::query_as(
                    "SELECT id, project_id, name, flag_id, flag_key, hypothesis, status, start_ts_ms, end_ts_ms, primary_metric_json, guardrail_metrics_json, created_at_ms
                     FROM experiments WHERE project_id = ? ORDER BY created_at_ms DESC",
                )
                .bind(project_id)
                .fetch_all(p)
                .await?
            }
            MetaStore::Postgres(p) => {
                sqlx::query_as(
                    "SELECT id, project_id, name, flag_id, flag_key, hypothesis, status, start_ts_ms, end_ts_ms, primary_metric_json, guardrail_metrics_json, created_at_ms
                     FROM experiments WHERE project_id = $1 ORDER BY created_at_ms DESC",
                )
                .bind(project_id)
                .fetch_all(p)
                .await?
            }
        };
        let mut out = Vec::with_capacity(raw.len());
        for (id, pid, name, fid, fkey, hyp, status, sts, ets, pmj, gmj, cts) in raw {
            if let Ok(pm) = serde_json::from_str(&pmj) {
                out.push(Experiment {
                    id,
                    project_id: pid,
                    name,
                    flag_id: fid,
                    flag_key: fkey,
                    hypothesis: hyp,
                    status,
                    start_ts_ms: sts,
                    end_ts_ms: ets,
                    primary_metric: pm,
                    guardrail_metrics: serde_json::from_str(&gmj).unwrap_or_default(),
                    created_at_ms: cts,
                });
            }
        }
        Ok(out)
    }

    pub async fn get_experiment_by_id(
        &self,
        exp_id: &str,
    ) -> Result<Option<Experiment>, sqlx::Error> {
        type ExpRow = (
            String,
            String,
            String,
            String,
            String,
            String,
            String,
            i64,
            i64,
            String,
            String,
            i64,
        );
        let raw: Option<ExpRow> = match self {
            MetaStore::Sqlite(p) => {
                sqlx::query_as(
                    "SELECT id, project_id, name, flag_id, flag_key, hypothesis, status, start_ts_ms, end_ts_ms, primary_metric_json, guardrail_metrics_json, created_at_ms
                     FROM experiments WHERE id = ?",
                )
                .bind(exp_id)
                .fetch_optional(p)
                .await?
            }
            MetaStore::Postgres(p) => {
                sqlx::query_as(
                    "SELECT id, project_id, name, flag_id, flag_key, hypothesis, status, start_ts_ms, end_ts_ms, primary_metric_json, guardrail_metrics_json, created_at_ms
                     FROM experiments WHERE id = $1",
                )
                .bind(exp_id)
                .fetch_optional(p)
                .await?
            }
        };
        Ok(raw.and_then(
            |(id, pid, name, fid, fkey, hyp, status, sts, ets, pmj, gmj, cts)| {
                let pm = serde_json::from_str(&pmj).ok()?;
                Some(Experiment {
                    id,
                    project_id: pid,
                    name,
                    flag_id: fid,
                    flag_key: fkey,
                    hypothesis: hyp,
                    status,
                    start_ts_ms: sts,
                    end_ts_ms: ets,
                    primary_metric: pm,
                    guardrail_metrics: serde_json::from_str(&gmj).unwrap_or_default(),
                    created_at_ms: cts,
                })
            },
        ))
    }

    // ------------------- Saved Reports & Dashboards -------------------

    pub async fn upsert_saved_report(&self, rep: &SavedReport) -> Result<(), sqlx::Error> {
        let qj = serde_json::to_string(&rep.query_json).unwrap_or_default();
        match self {
            MetaStore::Sqlite(p) => {
                sqlx::query(
                    "INSERT INTO saved_reports (id, project_id, name, report_kind, query_json, created_at_ms)
                     VALUES (?, ?, ?, ?, ?, ?)
                     ON CONFLICT(id) DO UPDATE SET name = excluded.name, query_json = excluded.query_json",
                )
                .bind(&rep.id)
                .bind(&rep.project_id)
                .bind(&rep.name)
                .bind(&rep.report_kind)
                .bind(&qj)
                .bind(rep.created_at_ms)
                .execute(p)
                .await?;
            }
            MetaStore::Postgres(p) => {
                sqlx::query(
                    "INSERT INTO saved_reports (id, project_id, name, report_kind, query_json, created_at_ms)
                     VALUES ($1, $2, $3, $4, $5, $6)
                     ON CONFLICT(id) DO UPDATE SET name = EXCLUDED.name, query_json = EXCLUDED.query_json",
                )
                .bind(&rep.id)
                .bind(&rep.project_id)
                .bind(&rep.name)
                .bind(&rep.report_kind)
                .bind(&qj)
                .bind(rep.created_at_ms)
                .execute(p)
                .await?;
            }
        }
        Ok(())
    }

    pub async fn list_saved_reports(
        &self,
        project_id: &str,
    ) -> Result<Vec<SavedReport>, sqlx::Error> {
        let raw: Vec<(String, String, String, String, String, i64)> = match self {
            MetaStore::Sqlite(p) => {
                sqlx::query_as(
                    "SELECT id, project_id, name, report_kind, query_json, created_at_ms
                     FROM saved_reports WHERE project_id = ? ORDER BY created_at_ms DESC",
                )
                .bind(project_id)
                .fetch_all(p)
                .await?
            }
            MetaStore::Postgres(p) => {
                sqlx::query_as(
                    "SELECT id, project_id, name, report_kind, query_json, created_at_ms
                     FROM saved_reports WHERE project_id = $1 ORDER BY created_at_ms DESC",
                )
                .bind(project_id)
                .fetch_all(p)
                .await?
            }
        };
        Ok(raw
            .into_iter()
            .map(|(id, pid, name, kind, qj, ts)| SavedReport {
                id,
                project_id: pid,
                name,
                report_kind: kind,
                query_json: serde_json::from_str(&qj).unwrap_or(serde_json::Value::Null),
                created_at_ms: ts,
            })
            .collect())
    }

    pub async fn upsert_dashboard(&self, d: &Dashboard) -> Result<(), sqlx::Error> {
        let rj = serde_json::to_string(&d.report_ids).unwrap_or_default();
        match self {
            MetaStore::Sqlite(p) => {
                sqlx::query(
                    "INSERT INTO dashboards (id, project_id, name, report_ids_json, created_at_ms)
                     VALUES (?, ?, ?, ?, ?)
                     ON CONFLICT(id) DO UPDATE SET name = excluded.name, report_ids_json = excluded.report_ids_json",
                )
                .bind(&d.id)
                .bind(&d.project_id)
                .bind(&d.name)
                .bind(&rj)
                .bind(d.created_at_ms)
                .execute(p)
                .await?;
            }
            MetaStore::Postgres(p) => {
                sqlx::query(
                    "INSERT INTO dashboards (id, project_id, name, report_ids_json, created_at_ms)
                     VALUES ($1, $2, $3, $4, $5)
                     ON CONFLICT(id) DO UPDATE SET name = EXCLUDED.name, report_ids_json = EXCLUDED.report_ids_json",
                )
                .bind(&d.id)
                .bind(&d.project_id)
                .bind(&d.name)
                .bind(&rj)
                .bind(d.created_at_ms)
                .execute(p)
                .await?;
            }
        }
        Ok(())
    }

    pub async fn list_dashboards(&self, project_id: &str) -> Result<Vec<Dashboard>, sqlx::Error> {
        let raw: Vec<(String, String, String, String, i64)> = match self {
            MetaStore::Sqlite(p) => {
                sqlx::query_as(
                    "SELECT id, project_id, name, report_ids_json, created_at_ms
                     FROM dashboards WHERE project_id = ? ORDER BY created_at_ms DESC",
                )
                .bind(project_id)
                .fetch_all(p)
                .await?
            }
            MetaStore::Postgres(p) => {
                sqlx::query_as(
                    "SELECT id, project_id, name, report_ids_json, created_at_ms
                     FROM dashboards WHERE project_id = $1 ORDER BY created_at_ms DESC",
                )
                .bind(project_id)
                .fetch_all(p)
                .await?
            }
        };
        Ok(raw
            .into_iter()
            .map(|(id, pid, name, rj, ts)| Dashboard {
                id,
                project_id: pid,
                name,
                report_ids: serde_json::from_str(&rj).unwrap_or_default(),
                created_at_ms: ts,
            })
            .collect())
    }
}
