use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use std::str::FromStr;

use pulse_exec::{CustomEventDef, CustomPropertyDef, LookupTableDef};
use serde_json::Value;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions};
use sqlx::Row;
use thiserror::Error;

use crate::models::DashboardCard;
use crate::models::{
    AlertRule, Annotation, ApiKeyRecord, ByokCredentialRecord, CohortRecord, Dashboard,
    DeletionAuditRecord, EnterpriseAuditRecord, Experiment, FeatureFlag, GroupProfileRecord,
    LexiconEvent, LexiconProperty, LookupTableSummary, Organization, Project, Role, SavedReport,
    SystemKeyringRecord, User, UserProfileRecord,
};

/// A row of the active-sessions list; `id` is a prefix of the token hash, never the token.
#[derive(Debug, Clone, serde::Serialize)]
pub struct SessionInfo {
    pub id: String,
    pub user_agent: Option<String>,
    pub created_at_ms: i64,
    pub last_seen_ms: i64,
    pub expires_at_ms: i64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct OrgInvite {
    pub id: String,
    #[serde(skip)]
    pub org_id: String,
    pub email: String,
    pub role: String,
    pub invited_by: String,
    pub created_at_ms: i64,
    pub expires_at_ms: i64,
}

/// What the auth middleware needs about a live session.
#[derive(Debug, Clone)]
pub struct SessionUser {
    pub user_id: String,
    pub org_id: String,
    pub role: Role,
    pub last_seen_ms: i64,
}

/// Unknown role strings read as viewer, so a bad row never grants more than read access.
fn role_of(s: &str) -> Role {
    Role::parse(s).unwrap_or(Role::Viewer)
}

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
pub(crate) fn pg_rewrite(sql: &str) -> String {
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

macro_rules! profile_from_row {
    ($r:ident) => {{
        let pstr: String = $r.get("props_json");
        let props: BTreeMap<String, Value> = serde_json::from_str(&pstr).unwrap_or_default();
        UserProfileRecord {
            project_id: $r.get("project_id"),
            distinct_id: $r.get("distinct_id"),
            canonical_uid: $r.get::<i64, _>("canonical_uid").max(0) as u32,
            props,
            total_revenue: $r.get("total_revenue"),
            updated_at_ms: $r.get("updated_at_ms"),
        }
    }};
}

/// Escapes `LIKE` wildcards so user search text matches literally (paired with `ESCAPE '\\'`).
pub(crate) fn escape_like(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if matches!(c, '%' | '_' | '\\') {
            out.push('\\');
        }
        out.push(c);
    }
    out
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

fn saved_report_from_row<R>(r: &R) -> SavedReport
where
    R: Row,
    for<'a> &'a str: sqlx::ColumnIndex<R>,
    String: for<'a> sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    i64: for<'a> sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
{
    let qstr: String = r.get("query_json");
    SavedReport {
        id: r.get("id"),
        project_id: r.get("project_id"),
        name: r.get("name"),
        report_kind: r.get("report_kind"),
        query_json: serde_json::from_str(&qstr).unwrap_or(serde_json::Value::Null),
        created_at_ms: r.get("created_at_ms"),
        updated_at_ms: r.get("updated_at_ms"),
    }
}

/// Rows written before `0003` have no `cards_json`; each legacy report id becomes a half-width card.
fn dashboard_from_row<R>(r: &R) -> Dashboard
where
    R: Row,
    for<'a> &'a str: sqlx::ColumnIndex<R>,
    String: for<'a> sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    Option<String>: for<'a> sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    i64: for<'a> sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
{
    let cards = match r.get::<Option<String>, _>("cards_json") {
        Some(cjson) => serde_json::from_str(&cjson).unwrap_or_default(),
        None => {
            let rstr: String = r.get("report_ids_json");
            serde_json::from_str::<Vec<String>>(&rstr)
                .unwrap_or_default()
                .into_iter()
                .enumerate()
                .map(|(i, report_id)| DashboardCard {
                    id: format!("card_legacy_{i}"),
                    kind: "report".to_string(),
                    report_id: Some(report_id),
                    text: None,
                    width: 1,
                })
                .collect()
        }
    };
    Dashboard {
        id: r.get("id"),
        project_id: r.get("project_id"),
        name: r.get("name"),
        description: r.get("description"),
        cards,
        created_by: r.get("created_by"),
        created_at_ms: r.get("created_at_ms"),
        updated_at_ms: r.get("updated_at_ms"),
    }
}

impl MetaStore {
    /// Returns `true` when this `MetaStore` is backed by SQLite rather than PostgreSQL.
    pub fn is_sqlite(&self) -> bool {
        matches!(self, Self::Sqlite(_))
    }

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

    /// Inserts a brand-new project; NEVER overwrites an existing project.
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
                label = excluded.label
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

    /// Revokes a key for good. Re-inserting the same key later (a restart with it still
    /// configured) does not bring it back, because the upsert above never clears `revoked`.
    pub async fn revoke_api_key(&self, project_id: &str, id: &str) -> Result<bool, MetaError> {
        let affected = db_exec!(
            self,
            "UPDATE api_keys SET revoked = 1 WHERE project_id = ? AND id = ?",
            project_id,
            id,
        );
        Ok(affected > 0)
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

    // ------------------- Saved Reports & Dashboards -------------------

    pub async fn upsert_saved_report(&self, rep: &SavedReport) -> Result<(), MetaError> {
        let qjson = serde_json::to_string(&rep.query_json).unwrap_or_else(|_| "{}".into());
        let affected = db_exec!(
            self,
            "INSERT INTO saved_reports (id, project_id, name, report_kind, query_json, created_at_ms, updated_at_ms)
             VALUES (?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                report_kind = excluded.report_kind,
                query_json = excluded.query_json,
                updated_at_ms = excluded.updated_at_ms
             WHERE saved_reports.project_id = excluded.project_id",
            &rep.id,
            &rep.project_id,
            &rep.name,
            &rep.report_kind,
            &qjson,
            rep.created_at_ms,
            rep.updated_at_ms,
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
            "SELECT id, project_id, name, report_kind, query_json, created_at_ms,
                    COALESCE(updated_at_ms, created_at_ms) AS updated_at_ms
             FROM saved_reports WHERE project_id = ? ORDER BY created_at_ms DESC",
            |r| { saved_report_from_row(&r) },
            project_id,
        );
        Ok(rows)
    }

    pub async fn get_saved_report(
        &self,
        project_id: &str,
        id: &str,
    ) -> Result<Option<SavedReport>, MetaError> {
        let row = db_map_opt!(
            self,
            "SELECT id, project_id, name, report_kind, query_json, created_at_ms,
                    COALESCE(updated_at_ms, created_at_ms) AS updated_at_ms
             FROM saved_reports WHERE project_id = ? AND id = ?",
            |r| { saved_report_from_row(&r) },
            project_id,
            id,
        );
        Ok(row)
    }

    /// Returns `false` when no report with this id exists in the project.
    pub async fn insert_annotation(&self, a: &Annotation) -> Result<(), MetaError> {
        db_exec!(
            self,
            "INSERT INTO annotations (id, project_id, ts_ms, text, created_by, created_at_ms)
             VALUES (?, ?, ?, ?, ?, ?)",
            a.id.as_str(),
            a.project_id.as_str(),
            a.ts_ms,
            a.text.as_str(),
            a.created_by.as_deref(),
            a.created_at_ms,
        );
        Ok(())
    }

    /// Annotations with `start_ms <= ts_ms < end_ms`, oldest first.
    pub async fn list_annotations(
        &self,
        project_id: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Annotation>, MetaError> {
        let rows = db_map_all!(
            self,
            "SELECT id, project_id, ts_ms, text, created_by, created_at_ms FROM annotations
             WHERE project_id = ? AND ts_ms >= ? AND ts_ms < ? ORDER BY ts_ms ASC LIMIT 500",
            |r| {
                Annotation {
                    id: r.get("id"),
                    project_id: r.get("project_id"),
                    ts_ms: r.get("ts_ms"),
                    text: r.get("text"),
                    created_by: r.get("created_by"),
                    created_at_ms: r.get("created_at_ms"),
                }
            },
            project_id,
            start_ms,
            end_ms,
        );
        Ok(rows)
    }

    pub async fn delete_annotation(&self, project_id: &str, id: &str) -> Result<bool, MetaError> {
        let affected = db_exec!(
            self,
            "DELETE FROM annotations WHERE project_id = ? AND id = ?",
            project_id,
            id,
        );
        Ok(affected > 0)
    }

    pub async fn delete_cohort(&self, project_id: &str, id: &str) -> Result<bool, MetaError> {
        let affected = db_exec!(
            self,
            "DELETE FROM cohorts WHERE project_id = ? AND id = ?",
            project_id,
            id,
        );
        Ok(affected > 0)
    }

    pub async fn delete_saved_report(&self, project_id: &str, id: &str) -> Result<bool, MetaError> {
        let affected = db_exec!(
            self,
            "DELETE FROM saved_reports WHERE project_id = ? AND id = ?",
            project_id,
            id,
        );
        Ok(affected > 0)
    }

    /// Writes `cards_json` plus the derived legacy `report_ids_json`. `created_by` and
    /// `created_at_ms` are kept from the original row on update.
    pub async fn upsert_dashboard(&self, d: &Dashboard) -> Result<(), MetaError> {
        let report_ids: Vec<&str> = d
            .cards
            .iter()
            .filter_map(|c| c.report_id.as_deref())
            .collect();
        let rjson = serde_json::to_string(&report_ids).unwrap_or_else(|_| "[]".into());
        let cjson = serde_json::to_string(&d.cards).unwrap_or_else(|_| "[]".into());
        let affected = db_exec!(
            self,
            "INSERT INTO dashboards (id, project_id, name, report_ids_json, created_at_ms,
                                     description, cards_json, created_by, updated_at_ms)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                report_ids_json = excluded.report_ids_json,
                description = excluded.description,
                cards_json = excluded.cards_json,
                updated_at_ms = excluded.updated_at_ms
             WHERE dashboards.project_id = excluded.project_id",
            &d.id,
            &d.project_id,
            &d.name,
            &rjson,
            d.created_at_ms,
            &d.description,
            &cjson,
            &d.created_by,
            d.updated_at_ms,
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
            "SELECT id, project_id, name, report_ids_json, created_at_ms, description, cards_json,
                    created_by, COALESCE(updated_at_ms, created_at_ms) AS updated_at_ms
             FROM dashboards WHERE project_id = ?
             ORDER BY COALESCE(updated_at_ms, created_at_ms) DESC, id ASC",
            |r| { dashboard_from_row(&r) },
            project_id,
        );
        Ok(rows)
    }

    pub async fn get_dashboard(
        &self,
        project_id: &str,
        id: &str,
    ) -> Result<Option<Dashboard>, MetaError> {
        let row = db_map_opt!(
            self,
            "SELECT id, project_id, name, report_ids_json, created_at_ms, description, cards_json,
                    created_by, COALESCE(updated_at_ms, created_at_ms) AS updated_at_ms
             FROM dashboards WHERE project_id = ? AND id = ?",
            |r| { dashboard_from_row(&r) },
            project_id,
            id,
        );
        Ok(row)
    }

    /// Returns `false` when no dashboard with this id exists in the project.
    pub async fn delete_dashboard(&self, project_id: &str, id: &str) -> Result<bool, MetaError> {
        let affected = db_exec!(
            self,
            "DELETE FROM dashboards WHERE project_id = ? AND id = ?",
            project_id,
            id,
        );
        Ok(affected > 0)
    }

    // ------------------- Users & Sessions -------------------

    /// Accounts with an organisation; sign-ups still waiting to verify their email don't count.
    /// One round trip to the database, for readiness checks.
    pub async fn ping(&self) -> Result<(), MetaError> {
        let _ = db_map_opt!(self, "SELECT 1 AS n", |r| { r.get::<i64, _>("n") });
        Ok(())
    }

    pub async fn count_users(&self) -> Result<i64, MetaError> {
        let n = db_map_opt!(
            self,
            "SELECT COUNT(*) AS n FROM users WHERE org_id <> 'org_pending'",
            |r| { r.get::<i64, _>("n") }
        );
        Ok(n.unwrap_or(0))
    }

    pub async fn count_orgs(&self) -> Result<i64, MetaError> {
        let n = db_map_opt!(self, "SELECT COUNT(*) AS n FROM orgs", |r| {
            r.get::<i64, _>("n")
        });
        Ok(n.unwrap_or(0))
    }

    /// Moves an account into an organisation with a role (verifying, or accepting an invite).
    /// Moves a user into the default org: owner if it has nobody yet, else member. Decided in one
    /// statement so concurrent first sign-ups don't both become owner.
    // ponytail: atomic on SQLite (one writer); on Postgres two transactions committing in the same
    // instant can still both see an empty org. Add a unique partial index on owners if that matters.
    pub async fn join_default_org(&self, user_id: &str) -> Result<Role, MetaError> {
        let _ = db_exec!(
            self,
            "UPDATE users SET org_id = 'org_default',
                role = CASE WHEN EXISTS (SELECT 1 FROM users u2 WHERE u2.org_id = 'org_default' AND u2.id <> ?)
                            THEN 'member' ELSE 'owner' END
             WHERE id = ?",
            user_id,
            user_id,
        );
        let role = db_map_opt!(
            self,
            "SELECT role FROM users WHERE id = ?",
            |r| { role_of(&r.get::<String, _>("role")) },
            user_id,
        );
        Ok(role.unwrap_or(Role::Member))
    }

    pub async fn set_user_org(
        &self,
        user_id: &str,
        org_id: &str,
        role: Role,
    ) -> Result<(), MetaError> {
        let _ = db_exec!(
            self,
            "UPDATE users SET org_id = ?, role = ? WHERE id = ?",
            org_id,
            role.as_str(),
            user_id,
        );
        Ok(())
    }

    pub async fn insert_email_verification(
        &self,
        token_hash: &str,
        user_id: &str,
        org_name: Option<&str>,
        expires_at_ms: i64,
    ) -> Result<(), MetaError> {
        let _ = db_exec!(
            self,
            "INSERT INTO email_verifications (token_hash, user_id, org_name, expires_at_ms) VALUES (?, ?, ?, ?)",
            token_hash,
            user_id,
            org_name,
            expires_at_ms,
        );
        Ok(())
    }

    /// Uses a verification token once; returns its user id and the organisation name to create.
    pub async fn take_email_verification(
        &self,
        token_hash: &str,
        now_ms: i64,
    ) -> Result<Option<(String, Option<String>)>, MetaError> {
        let row = db_map_opt!(
            self,
            "SELECT user_id, org_name FROM email_verifications
             WHERE token_hash = ? AND used_at_ms IS NULL AND expires_at_ms > ?",
            |r| {
                (
                    r.get::<String, _>("user_id"),
                    r.get::<Option<String>, _>("org_name"),
                )
            },
            token_hash,
            now_ms,
        );
        let Some(row) = row else { return Ok(None) };
        let n = db_exec!(
            self,
            "UPDATE email_verifications SET used_at_ms = ? WHERE token_hash = ? AND used_at_ms IS NULL",
            now_ms,
            token_hash,
        );
        Ok((n > 0).then_some(row))
    }

    /// Inserts a new user; `Conflict` when the email (or Google subject) is already taken.
    pub async fn insert_user(&self, u: &User) -> Result<(), MetaError> {
        let affected = db_exec!(
            self,
            "INSERT INTO users (id, email, name, password_hash, google_sub, created_at_ms, org_id, role)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT DO NOTHING",
            &u.id,
            &u.email,
            &u.name,
            &u.password_hash,
            &u.google_sub,
            u.created_at_ms,
            &u.org_id,
            u.role.as_str(),
        );
        if affected == 0 {
            return Err(MetaError::Conflict(
                "an account with this email already exists".to_string(),
            ));
        }
        Ok(())
    }

    /// `column` is always a hard-coded identifier from this module, never user input.
    async fn get_user_by(
        &self,
        column: &'static str,
        value: &str,
    ) -> Result<Option<User>, MetaError> {
        let sql = format!(
            "SELECT id, email, name, password_hash, google_sub, created_at_ms, org_id, role FROM users WHERE {column} = ?"
        );
        let user = db_map_opt!(
            self,
            &sql,
            |r| {
                User {
                    id: r.get("id"),
                    email: r.get("email"),
                    name: r.get("name"),
                    password_hash: r.get("password_hash"),
                    google_sub: r.get("google_sub"),
                    created_at_ms: r.get("created_at_ms"),
                    org_id: r.get("org_id"),
                    role: role_of(&r.get::<String, _>("role")),
                }
            },
            value,
        );
        Ok(user)
    }

    pub async fn get_user_by_id(&self, id: &str) -> Result<Option<User>, MetaError> {
        self.get_user_by("id", id).await
    }

    pub async fn get_user_by_email(&self, email: &str) -> Result<Option<User>, MetaError> {
        self.get_user_by("email", email).await
    }

    pub async fn get_user_by_google_sub(&self, sub: &str) -> Result<Option<User>, MetaError> {
        self.get_user_by("google_sub", sub).await
    }

    pub async fn set_user_google_sub(&self, user_id: &str, sub: &str) -> Result<(), MetaError> {
        let _ = db_exec!(
            self,
            "UPDATE users SET google_sub = ? WHERE id = ?",
            sub,
            user_id,
        );
        Ok(())
    }

    pub async fn insert_session(
        &self,
        token_hash: &str,
        user_id: &str,
        created_at_ms: i64,
        expires_at_ms: i64,
        user_agent: Option<&str>,
    ) -> Result<(), MetaError> {
        let _ = db_exec!(
            self,
            "INSERT INTO user_sessions (token_hash, user_id, created_at_ms, expires_at_ms, user_agent, last_seen_ms)
             VALUES (?, ?, ?, ?, ?, ?)",
            token_hash,
            user_id,
            created_at_ms,
            expires_at_ms,
            user_agent,
            created_at_ms,
        );
        Ok(())
    }

    /// The signed-in user's id, organisation and role for a live (unexpired) session whose user still exists.
    pub async fn get_session_user(
        &self,
        token_hash: &str,
        now_ms: i64,
    ) -> Result<Option<SessionUser>, MetaError> {
        let row = db_map_opt!(
            self,
            "SELECT s.user_id AS user_id, u.org_id AS org_id, u.role AS role, s.last_seen_ms AS last_seen_ms
             FROM user_sessions s JOIN users u ON u.id = s.user_id
             WHERE s.token_hash = ? AND s.expires_at_ms > ?",
            |r| {
                SessionUser {
                    user_id: r.get("user_id"),
                    org_id: r.get("org_id"),
                    role: role_of(&r.get::<String, _>("role")),
                    last_seen_ms: r.get("last_seen_ms"),
                }
            },
            token_hash,
            now_ms,
        );
        Ok(row)
    }

    pub async fn touch_session(&self, token_hash: &str, now_ms: i64) -> Result<(), MetaError> {
        let _ = db_exec!(
            self,
            "UPDATE user_sessions SET last_seen_ms = ? WHERE token_hash = ?",
            now_ms,
            token_hash,
        );
        Ok(())
    }

    pub async fn list_sessions(
        &self,
        user_id: &str,
        now_ms: i64,
    ) -> Result<Vec<(String, SessionInfo)>, MetaError> {
        let rows = db_map_all!(
            self,
            "SELECT token_hash, user_agent, created_at_ms, last_seen_ms, expires_at_ms FROM user_sessions
             WHERE user_id = ? AND expires_at_ms > ? ORDER BY last_seen_ms DESC",
            |r| {
                let hash: String = r.get("token_hash");
                let info = SessionInfo {
                    id: hash[..16].to_string(),
                    user_agent: r.get("user_agent"),
                    created_at_ms: r.get("created_at_ms"),
                    last_seen_ms: r.get("last_seen_ms"),
                    expires_at_ms: r.get("expires_at_ms"),
                };
                (hash, info)
            },
            user_id,
            now_ms,
        );
        Ok(rows)
    }

    /// Deletes `user_id`'s sessions: the one whose hash starts with `id_prefix`, or, with `None`,
    /// all of them except `keep_hash` (if given).
    pub async fn delete_user_sessions(
        &self,
        user_id: &str,
        id_prefix: Option<&str>,
        keep_hash: Option<&str>,
    ) -> Result<u64, MetaError> {
        let n = match id_prefix {
            Some(prefix) => db_exec!(
                self,
                "DELETE FROM user_sessions WHERE user_id = ? AND substr(token_hash, 1, 16) = ?",
                user_id,
                prefix,
            ),
            None => db_exec!(
                self,
                "DELETE FROM user_sessions WHERE user_id = ? AND token_hash <> ?",
                user_id,
                keep_hash.unwrap_or(""),
            ),
        };
        Ok(n)
    }

    pub async fn update_user_name(
        &self,
        user_id: &str,
        name: Option<&str>,
    ) -> Result<(), MetaError> {
        let _ = db_exec!(
            self,
            "UPDATE users SET name = ? WHERE id = ?",
            name,
            user_id
        );
        Ok(())
    }

    pub async fn update_user_password(&self, user_id: &str, phc: &str) -> Result<(), MetaError> {
        let _ = db_exec!(
            self,
            "UPDATE users SET password_hash = ? WHERE id = ?",
            phc,
            user_id,
        );
        Ok(())
    }

    pub async fn update_user_role(
        &self,
        org_id: &str,
        user_id: &str,
        role: Role,
    ) -> Result<bool, MetaError> {
        let n = db_exec!(
            self,
            "UPDATE users SET role = ? WHERE id = ? AND org_id = ?",
            role.as_str(),
            user_id,
            org_id,
        );
        Ok(n > 0)
    }

    /// Removes an account from its organisation. A user has exactly one organisation, so this
    /// deletes the account and its sessions.
    pub async fn delete_user(&self, org_id: &str, user_id: &str) -> Result<bool, MetaError> {
        let n = db_exec!(
            self,
            "DELETE FROM users WHERE id = ? AND org_id = ?",
            user_id,
            org_id,
        );
        if n > 0 {
            let _ = db_exec!(self, "DELETE FROM user_sessions WHERE user_id = ?", user_id);
            let _ = db_exec!(
                self,
                "DELETE FROM password_resets WHERE user_id = ?",
                user_id
            );
            let _ = db_exec!(
                self,
                "DELETE FROM email_verifications WHERE user_id = ?",
                user_id
            );
        }
        Ok(n > 0)
    }

    pub async fn list_org_users(&self, org_id: &str) -> Result<Vec<User>, MetaError> {
        let rows = db_map_all!(
            self,
            "SELECT id, email, name, password_hash, google_sub, created_at_ms, org_id, role FROM users
             WHERE org_id = ? ORDER BY created_at_ms",
            |r| {
                User {
                    id: r.get("id"),
                    email: r.get("email"),
                    name: r.get("name"),
                    password_hash: r.get("password_hash"),
                    google_sub: r.get("google_sub"),
                    created_at_ms: r.get("created_at_ms"),
                    org_id: r.get("org_id"),
                    role: role_of(&r.get::<String, _>("role")),
                }
            },
            org_id,
        );
        Ok(rows)
    }

    pub async fn rename_project(&self, id: &str, name: &str) -> Result<(), MetaError> {
        let _ = db_exec!(self, "UPDATE projects SET name = ? WHERE id = ?", name, id);
        Ok(())
    }

    pub async fn set_project_timezone(&self, id: &str, tz: &str) -> Result<(), MetaError> {
        let _ = db_exec!(
            self,
            "UPDATE projects SET timezone = ? WHERE id = ?",
            tz,
            id
        );
        Ok(())
    }

    pub async fn rename_org(&self, id: &str, name: &str) -> Result<(), MetaError> {
        let _ = db_exec!(self, "UPDATE orgs SET name = ? WHERE id = ?", name, id);
        Ok(())
    }

    /// Stores an invite, replacing any pending one for the same email in the same organisation.
    pub async fn insert_invite(&self, inv: &OrgInvite, token_hash: &str) -> Result<(), MetaError> {
        let _ = db_exec!(
            self,
            "DELETE FROM org_invites WHERE org_id = ? AND email = ? AND accepted_at_ms IS NULL",
            &inv.org_id,
            &inv.email,
        );
        let _ = db_exec!(
            self,
            "INSERT INTO org_invites (id, token_hash, org_id, email, role, invited_by, created_at_ms, expires_at_ms)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            &inv.id,
            token_hash,
            &inv.org_id,
            &inv.email,
            &inv.role,
            &inv.invited_by,
            inv.created_at_ms,
            inv.expires_at_ms,
        );
        Ok(())
    }

    /// Pending (unaccepted, unexpired) invites of an organisation, newest first.
    pub async fn list_invites(
        &self,
        org_id: &str,
        now_ms: i64,
    ) -> Result<Vec<OrgInvite>, MetaError> {
        let rows = db_map_all!(
            self,
            "SELECT id, org_id, email, role, invited_by, created_at_ms, expires_at_ms FROM org_invites
             WHERE org_id = ? AND accepted_at_ms IS NULL AND expires_at_ms > ? ORDER BY created_at_ms DESC",
            |r| {
                OrgInvite {
                    id: r.get("id"),
                    org_id: r.get("org_id"),
                    email: r.get("email"),
                    role: r.get("role"),
                    invited_by: r.get("invited_by"),
                    created_at_ms: r.get("created_at_ms"),
                    expires_at_ms: r.get("expires_at_ms"),
                }
            },
            org_id,
            now_ms,
        );
        Ok(rows)
    }

    /// A pending invite by its token hash.
    pub async fn get_invite(
        &self,
        token_hash: &str,
        now_ms: i64,
    ) -> Result<Option<OrgInvite>, MetaError> {
        let row = db_map_opt!(
            self,
            "SELECT id, org_id, email, role, invited_by, created_at_ms, expires_at_ms FROM org_invites
             WHERE token_hash = ? AND accepted_at_ms IS NULL AND expires_at_ms > ?",
            |r| {
                OrgInvite {
                    id: r.get("id"),
                    org_id: r.get("org_id"),
                    email: r.get("email"),
                    role: r.get("role"),
                    invited_by: r.get("invited_by"),
                    created_at_ms: r.get("created_at_ms"),
                    expires_at_ms: r.get("expires_at_ms"),
                }
            },
            token_hash,
            now_ms,
        );
        Ok(row)
    }

    /// Marks an invite used. `false` when someone else used it first.
    pub async fn accept_invite(&self, id: &str, now_ms: i64) -> Result<bool, MetaError> {
        let n = db_exec!(
            self,
            "UPDATE org_invites SET accepted_at_ms = ? WHERE id = ? AND accepted_at_ms IS NULL",
            now_ms,
            id,
        );
        Ok(n > 0)
    }

    pub async fn delete_invite(&self, org_id: &str, id: &str) -> Result<bool, MetaError> {
        let n = db_exec!(
            self,
            "DELETE FROM org_invites WHERE id = ? AND org_id = ? AND accepted_at_ms IS NULL",
            id,
            org_id,
        );
        Ok(n > 0)
    }

    pub async fn insert_password_reset(
        &self,
        token_hash: &str,
        user_id: &str,
        expires_at_ms: i64,
    ) -> Result<(), MetaError> {
        let _ = db_exec!(
            self,
            "INSERT INTO password_resets (token_hash, user_id, expires_at_ms) VALUES (?, ?, ?)",
            token_hash,
            user_id,
            expires_at_ms,
        );
        Ok(())
    }

    /// Uses a reset token once; returns its user id when it was live.
    pub async fn delete_password_resets(&self, user_id: &str) -> Result<(), MetaError> {
        let _ = db_exec!(
            self,
            "DELETE FROM password_resets WHERE user_id = ?",
            user_id
        );
        Ok(())
    }

    pub async fn take_password_reset(
        &self,
        token_hash: &str,
        now_ms: i64,
    ) -> Result<Option<String>, MetaError> {
        let uid = db_map_opt!(
            self,
            "SELECT user_id FROM password_resets WHERE token_hash = ? AND used_at_ms IS NULL AND expires_at_ms > ?",
            |r| { r.get::<String, _>("user_id") },
            token_hash,
            now_ms,
        );
        let Some(uid) = uid else { return Ok(None) };
        let n = db_exec!(
            self,
            "UPDATE password_resets SET used_at_ms = ? WHERE token_hash = ? AND used_at_ms IS NULL",
            now_ms,
            token_hash,
        );
        Ok((n > 0).then_some(uid))
    }

    pub async fn insert_org(
        &self,
        id: &str,
        name: &str,
        created_at_ms: i64,
    ) -> Result<(), MetaError> {
        let _ = db_exec!(
            self,
            "INSERT INTO orgs (id, name, created_at_ms) VALUES (?, ?, ?)",
            id,
            name,
            created_at_ms,
        );
        Ok(())
    }

    pub async fn get_org_name(&self, id: &str) -> Result<Option<String>, MetaError> {
        let name = db_map_opt!(
            self,
            "SELECT name FROM orgs WHERE id = ?",
            |r| { r.get::<String, _>("name") },
            id,
        );
        Ok(name)
    }

    pub async fn delete_session(&self, token_hash: &str) -> Result<(), MetaError> {
        let _ = db_exec!(
            self,
            "DELETE FROM user_sessions WHERE token_hash = ?",
            token_hash
        );
        Ok(())
    }

    // ------------------- User Profiles, Groups, DPDP Erasure & Alerts (`Gaps 1, 4, 8, 14`) -------------------

    pub async fn upsert_user_profile(&self, prof: &UserProfileRecord) -> Result<(), MetaError> {
        let pjson = serde_json::to_string(&prof.props).unwrap_or_else(|_| "{}".into());
        let _ = db_exec!(
            self,
            "INSERT INTO user_profiles (project_id, distinct_id, canonical_uid, props_json, total_revenue, updated_at_ms)
             VALUES (?, ?, ?, ?, ?, ?)
             ON CONFLICT(project_id, distinct_id) DO UPDATE SET
                canonical_uid = excluded.canonical_uid,
                props_json = excluded.props_json,
                total_revenue = excluded.total_revenue,
                updated_at_ms = excluded.updated_at_ms",
            &prof.project_id,
            &prof.distinct_id,
            prof.canonical_uid as i64,
            pjson,
            prof.total_revenue,
            prof.updated_at_ms,
        );
        Ok(())
    }

    pub async fn get_user_profile(
        &self,
        project_id: &str,
        distinct_id: &str,
    ) -> Result<Option<UserProfileRecord>, MetaError> {
        let rec = db_map_opt!(
            self,
            "SELECT project_id, distinct_id, canonical_uid, props_json, total_revenue, updated_at_ms
             FROM user_profiles WHERE project_id = ? AND distinct_id = ?",
            |r| {
                let pstr: String = r.get("props_json");
                let props: BTreeMap<String, Value> =
                    serde_json::from_str(&pstr).unwrap_or_default();
                UserProfileRecord {
                    project_id: r.get("project_id"),
                    distinct_id: r.get("distinct_id"),
                    canonical_uid: r.get::<i64, _>("canonical_uid").max(0) as u32,
                    props,
                    total_revenue: r.get("total_revenue"),
                    updated_at_ms: r.get("updated_at_ms"),
                }
            },
            project_id,
            distinct_id,
        );
        Ok(rec)
    }

    /// Newest-first page of a project's profiles, optionally filtered by a case-insensitive
    /// substring of the distinct id or any property value. Search runs in the database so it
    /// covers every profile, not just a loaded window.
    pub async fn list_user_profiles(
        &self,
        project_id: &str,
        search: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<UserProfileRecord>, MetaError> {
        let lim = limit.clamp(1, 1000) as i64;
        let off = offset.min(i64::MAX as usize) as i64;
        let pattern = match search.map(str::trim).filter(|s| !s.is_empty()) {
            Some(s) => format!("%{}%", escape_like(&s.to_lowercase())),
            None => "%".to_string(),
        };
        let rows = db_map_all!(
            self,
            "SELECT project_id, distinct_id, canonical_uid, props_json, total_revenue, updated_at_ms
             FROM user_profiles
             WHERE project_id = ? AND (LOWER(distinct_id) LIKE ? ESCAPE '\\' OR LOWER(props_json) LIKE ? ESCAPE '\\')
             ORDER BY updated_at_ms DESC, distinct_id ASC LIMIT ? OFFSET ?",
            |r| { profile_from_row!(r) },
            project_id,
            &pattern,
            &pattern,
            lim,
            off,
        );
        Ok(rows)
    }

    /// Every profile updated at or after `since_ms`, oldest first, so applying them in order
    /// leaves the newest value of each property. `since_ms = 0` loads the whole project.
    pub async fn list_user_profiles_updated_since(
        &self,
        project_id: &str,
        since_ms: i64,
    ) -> Result<Vec<UserProfileRecord>, MetaError> {
        let rows = db_map_all!(
            self,
            "SELECT project_id, distinct_id, canonical_uid, props_json, total_revenue, updated_at_ms
             FROM user_profiles WHERE project_id = ? AND updated_at_ms >= ?
             ORDER BY updated_at_ms ASC, distinct_id ASC",
            |r| { profile_from_row!(r) },
            project_id,
            since_ms,
        );
        Ok(rows)
    }

    /// All rows (anonymous and identified ids) that belong to one canonical user, oldest first.
    pub async fn list_user_profiles_for_canonical(
        &self,
        project_id: &str,
        canonical_uid: u32,
    ) -> Result<Vec<UserProfileRecord>, MetaError> {
        let rows = db_map_all!(
            self,
            "SELECT project_id, distinct_id, canonical_uid, props_json, total_revenue, updated_at_ms
             FROM user_profiles WHERE project_id = ? AND canonical_uid = ?
             ORDER BY updated_at_ms ASC, distinct_id ASC",
            |r| { profile_from_row!(r) },
            project_id,
            canonical_uid as i64,
        );
        Ok(rows)
    }

    /// Points the given distinct ids' rows at `canonical_uid` after an identity merge.
    pub async fn rekey_user_profiles(
        &self,
        project_id: &str,
        distinct_ids: &[&str],
        canonical_uid: u32,
    ) -> Result<u64, MetaError> {
        let mut n = 0u64;
        for did in distinct_ids {
            n += db_exec!(
                self,
                "UPDATE user_profiles SET canonical_uid = ? WHERE project_id = ? AND distinct_id = ?",
                canonical_uid as i64,
                project_id,
                *did,
            );
        }
        Ok(n)
    }

    /// `(profiles, paying profiles, total revenue)` over every profile in the project.
    pub async fn user_revenue_summary(
        &self,
        project_id: &str,
    ) -> Result<(u64, u64, f64), MetaError> {
        let row = db_map_opt!(
            self,
            "SELECT COUNT(*) AS n,
                    COALESCE(SUM(CASE WHEN total_revenue > 0 THEN 1 ELSE 0 END), 0) AS paying,
                    COALESCE(SUM(CASE WHEN total_revenue > 0 THEN total_revenue ELSE 0 END), 0.0) AS total
             FROM user_profiles WHERE project_id = ?",
            |r| {
                (
                    r.get::<i64, _>("n").max(0) as u64,
                    r.get::<i64, _>("paying").max(0) as u64,
                    r.get::<f64, _>("total"),
                )
            },
            project_id,
        );
        Ok(row.unwrap_or((0, 0, 0.0)))
    }

    pub async fn delete_user_profiles_for_ids(
        &self,
        project_id: &str,
        distinct_ids: &[String],
    ) -> Result<u64, MetaError> {
        let mut removed = 0u64;
        for did in distinct_ids {
            removed += db_exec!(
                self,
                "DELETE FROM user_profiles WHERE project_id = ? AND distinct_id = ?",
                project_id,
                did,
            );
        }
        Ok(removed)
    }

    pub async fn upsert_group_profile(&self, grp: &GroupProfileRecord) -> Result<(), MetaError> {
        let pjson = serde_json::to_string(&grp.props).unwrap_or_else(|_| "{}".into());
        let _ = db_exec!(
            self,
            "INSERT INTO group_profiles (project_id, group_type, group_id, props_json, updated_at_ms)
             VALUES (?, ?, ?, ?, ?)
             ON CONFLICT(project_id, group_type, group_id) DO UPDATE SET
                props_json = excluded.props_json,
                updated_at_ms = excluded.updated_at_ms",
            &grp.project_id,
            &grp.group_type,
            &grp.group_id,
            pjson,
            grp.updated_at_ms,
        );
        Ok(())
    }

    pub async fn get_group_profile(
        &self,
        project_id: &str,
        group_type: &str,
        group_id: &str,
    ) -> Result<Option<GroupProfileRecord>, MetaError> {
        let rec = db_map_opt!(
            self,
            "SELECT project_id, group_type, group_id, props_json, updated_at_ms
             FROM group_profiles WHERE project_id = ? AND group_type = ? AND group_id = ?",
            |r| {
                let pstr: String = r.get("props_json");
                let props: BTreeMap<String, Value> =
                    serde_json::from_str(&pstr).unwrap_or_default();
                GroupProfileRecord {
                    project_id: r.get("project_id"),
                    group_type: r.get("group_type"),
                    group_id: r.get("group_id"),
                    props,
                    updated_at_ms: r.get("updated_at_ms"),
                }
            },
            project_id,
            group_type,
            group_id,
        );
        Ok(rec)
    }

    pub async fn insert_deletion_audit(&self, rec: &DeletionAuditRecord) -> Result<(), MetaError> {
        let _ = db_exec!(
            self,
            "INSERT INTO deletion_audit_log
             (id, project_id, distinct_id_masked, canonical_uid, events_purged, replays_purged, requested_by, deleted_at_ms)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            &rec.id,
            &rec.project_id,
            &rec.distinct_id_masked,
            rec.canonical_uid as i64,
            rec.events_purged as i64,
            rec.replays_purged as i64,
            &rec.requested_by,
            rec.deleted_at_ms,
        );
        Ok(())
    }

    pub async fn list_deletion_audit(
        &self,
        project_id: &str,
    ) -> Result<Vec<DeletionAuditRecord>, MetaError> {
        let rows = db_map_all!(
            self,
            "SELECT id, project_id, distinct_id_masked, canonical_uid, events_purged, replays_purged, requested_by, deleted_at_ms
             FROM deletion_audit_log WHERE project_id = ? ORDER BY deleted_at_ms DESC LIMIT 200",
            |r| {
                DeletionAuditRecord {
                    id: r.get("id"),
                    project_id: r.get("project_id"),
                    distinct_id_masked: r.get("distinct_id_masked"),
                    canonical_uid: r.get::<i64, _>("canonical_uid").max(0) as u32,
                    events_purged: r.get::<i64, _>("events_purged").max(0) as u64,
                    replays_purged: r.get::<i64, _>("replays_purged").max(0) as u64,
                    requested_by: r.get("requested_by"),
                    deleted_at_ms: r.get("deleted_at_ms"),
                }
            },
            project_id,
        );
        Ok(rows)
    }

    pub async fn insert_erased_uids(
        &self,
        project_id: &str,
        uids: &[u32],
        erased_at_ms: i64,
    ) -> Result<(), MetaError> {
        for &uid in uids {
            db_exec!(
                self,
                "INSERT INTO erased_uids (project_id, uid, erased_at_ms) VALUES (?, ?, ?)
                 ON CONFLICT(project_id, uid) DO UPDATE SET erased_at_ms = excluded.erased_at_ms",
                project_id,
                uid as i64,
                erased_at_ms,
            );
        }
        Ok(())
    }

    /// `(raw uid, erased at ms)` for every erased id in the project.
    pub async fn list_erased_uids(&self, project_id: &str) -> Result<Vec<(u32, i64)>, MetaError> {
        let rows = db_map_all!(
            self,
            "SELECT uid, erased_at_ms FROM erased_uids WHERE project_id = ?",
            |r| {
                (
                    r.get::<i64, _>("uid").max(0) as u32,
                    r.get::<i64, _>("erased_at_ms"),
                )
            },
            project_id,
        );
        Ok(rows)
    }

    /// Deletes erasure tombstones at or above `high_water_mark` for `project_id` after restore,
    /// leaving `deletion_audit_log` intact.
    pub async fn delete_erased_uids_from_hwm(
        &self,
        project_id: &str,
        high_water_mark: u64,
    ) -> Result<u64, MetaError> {
        let Ok(hwm) = i64::try_from(high_water_mark) else {
            return Ok(0);
        };
        Ok(db_exec!(
            self,
            "DELETE FROM erased_uids WHERE project_id = ? AND uid >= ?",
            project_id,
            hwm,
        ))
    }

    /// Records event ids taken in by an import and returns the ones not seen before. Claiming
    /// first makes two concurrent imports of the same file add each event once.
    pub async fn claim_import_uuids(
        &self,
        project_id: &str,
        uuids: &[String],
        now_ms: i64,
    ) -> Result<HashSet<String>, MetaError> {
        let mut fresh = HashSet::new();
        for chunk in uuids.chunks(300) {
            let mut sql = String::from(
                "INSERT INTO import_seen (project_id, event_uuid, seen_at_ms) VALUES ",
            );
            for i in 0..chunk.len() {
                sql.push_str(if i == 0 { "(?, ?, ?)" } else { ", (?, ?, ?)" });
            }
            sql.push_str(" ON CONFLICT(project_id, event_uuid) DO NOTHING RETURNING event_uuid");
            let got: Vec<String> = match self {
                MetaStore::Sqlite(pool) => {
                    let mut q = sqlx::query(&sql);
                    for u in chunk {
                        q = q.bind(project_id).bind(u).bind(now_ms);
                    }
                    q.fetch_all(pool)
                        .await?
                        .into_iter()
                        .map(|r| r.get::<String, _>("event_uuid"))
                        .collect()
                }
                MetaStore::Postgres(pool) => {
                    let pg_q = pg_rewrite(&sql);
                    let mut q = sqlx::query(&pg_q);
                    for u in chunk {
                        q = q.bind(project_id).bind(u).bind(now_ms);
                    }
                    q.fetch_all(pool)
                        .await?
                        .into_iter()
                        .map(|r| r.get::<String, _>("event_uuid"))
                        .collect()
                }
            };
            fresh.extend(got);
        }
        Ok(fresh)
    }

    /// Gives back claims for an import that failed, so a retry is not counted as a duplicate.
    pub async fn release_import_uuids(
        &self,
        project_id: &str,
        uuids: &[String],
    ) -> Result<(), MetaError> {
        for u in uuids {
            db_exec!(
                self,
                "DELETE FROM import_seen WHERE project_id = ? AND event_uuid = ?",
                project_id,
                u.as_str(),
            );
        }
        Ok(())
    }

    /// Forgets import ids older than `before_ms`.
    pub async fn prune_import_uuids(&self, before_ms: i64) -> Result<u64, MetaError> {
        Ok(db_exec!(
            self,
            "DELETE FROM import_seen WHERE seen_at_ms < ?",
            before_ms
        ))
    }

    pub async fn upsert_alert_rule(&self, alert: &AlertRule) -> Result<(), MetaError> {
        let _ = db_exec!(
            self,
            "INSERT INTO alert_rules (id, project_id, name, event_name, operator, threshold, webhook_url, last_triggered_ms, created_at_ms)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                event_name = excluded.event_name,
                operator = excluded.operator,
                threshold = excluded.threshold,
                webhook_url = excluded.webhook_url,
                last_triggered_ms = excluded.last_triggered_ms",
            &alert.id,
            &alert.project_id,
            &alert.name,
            &alert.event_name,
            &alert.operator,
            alert.threshold,
            alert.webhook_url.as_deref(),
            alert.last_triggered_ms,
            alert.created_at_ms,
        );
        Ok(())
    }

    /// Claims an alert trigger: sets `last_triggered_ms` only if it still holds the value the
    /// caller read. Returns false when another node claimed it first.
    pub async fn claim_alert_trigger(
        &self,
        alert_id: &str,
        prev_triggered_ms: Option<i64>,
        now_ms: i64,
    ) -> Result<bool, MetaError> {
        let affected = db_exec!(
            self,
            "UPDATE alert_rules SET last_triggered_ms = ?
             WHERE id = ? AND COALESCE(last_triggered_ms, 0) = ?",
            now_ms,
            alert_id,
            prev_triggered_ms.unwrap_or(0),
        );
        Ok(affected == 1)
    }

    pub async fn list_alert_rules(&self, project_id: &str) -> Result<Vec<AlertRule>, MetaError> {
        let rows = db_map_all!(
            self,
            "SELECT id, project_id, name, event_name, operator, threshold, webhook_url, last_triggered_ms, created_at_ms
             FROM alert_rules WHERE project_id = ? ORDER BY created_at_ms DESC",
            |r| {
                AlertRule {
                    id: r.get("id"),
                    project_id: r.get("project_id"),
                    name: r.get("name"),
                    event_name: r.get("event_name"),
                    operator: r.get("operator"),
                    threshold: r.get("threshold"),
                    webhook_url: r.get("webhook_url"),
                    last_triggered_ms: r.get("last_triggered_ms"),
                    created_at_ms: r.get("created_at_ms"),
                }
            },
            project_id,
        );
        Ok(rows)
    }

    pub async fn delete_alert_rule(
        &self,
        project_id: &str,
        alert_id: &str,
    ) -> Result<u64, MetaError> {
        let affected = db_exec!(
            self,
            "DELETE FROM alert_rules WHERE project_id = ? AND id = ?",
            project_id,
            alert_id,
        );
        Ok(affected)
    }

    // ------------------- Provider credential vault -------------------

    /// Resolves a persistent cluster KEK from `PULSE_VAULT_KEK` or `<db_dir>/.pulse_cluster.kek`,
    /// completely independent of `PULSE_ADMIN_TOKEN`. Returns `Ok(None)` only for pure in-memory stores.
    pub async fn resolve_or_create_cluster_kek(&self) -> Result<Option<[u8; 32]>, String> {
        if let Ok(raw) = std::env::var("PULSE_VAULT_KEK") {
            let trimmed = raw.trim();
            if !trimmed.is_empty() {
                let k = pulse_storage::parse_byok_master_key(trimmed).map_err(|e| e.to_string())?;
                return Ok(Some(k));
            }
        }

        let dir_opt: Option<std::path::PathBuf> = match self {
            MetaStore::Sqlite(pool) => {
                let rows = sqlx::query("PRAGMA database_list")
                    .fetch_all(pool)
                    .await
                    .map_err(|e| e.to_string())?;
                rows.into_iter()
                    .find(|r| r.get::<String, _>("name") == "main")
                    .and_then(|r| {
                        let file: String = r.get("file");
                        if file.is_empty() || file == ":memory:" {
                            None
                        } else {
                            std::path::Path::new(&file)
                                .parent()
                                .map(|p| p.to_path_buf())
                        }
                    })
            }
            // Postgres usually means several nodes; a per-node KEK file would differ between them,
            // so Postgres deployments must share one PULSE_VAULT_KEK.
            MetaStore::Postgres(_) => None,
        };

        let Some(dir) = dir_opt else {
            return Ok(None);
        };
        std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
        let kek_path = dir.join(".pulse_cluster.kek");
        if kek_path.exists() {
            let content = std::fs::read_to_string(&kek_path)
                .map_err(|e| format!("failed to read {}: {e}", kek_path.display()))?;
            let bytes = hex::decode(content.trim())
                .map_err(|e| format!("corrupted cluster KEK file {}: {e}", kek_path.display()))?;
            if bytes.len() != 32 {
                return Err(format!(
                    "cluster KEK file {} must contain 32 hex bytes",
                    kek_path.display()
                ));
            }
            let mut key = [0u8; 32];
            key.copy_from_slice(&bytes);
            return Ok(Some(key));
        }

        // A missing KEK file with sealed rows means the key was lost or the file wasn't restored.
        // Minting a new one would silently orphan every sealed secret, so stop instead.
        if self.has_sealed_rows().await.map_err(|e| e.to_string())? {
            return Err(format!(
                "cluster KEK file {} is missing but sealed secrets exist; restore it or set PULSE_VAULT_KEK",
                kek_path.display()
            ));
        }
        let mut new_key = [0u8; 32];
        ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut new_key)
            .map_err(|_| "CSPRNG failure generating cluster KEK".to_string())?;
        let hex_str = hex::encode(new_key);

        #[cfg(unix)]
        {
            use std::io::Write;
            use std::os::unix::fs::OpenOptionsExt;
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&kek_path)
                .map_err(|e| format!("failed creating {}: {e}", kek_path.display()))?;
            f.write_all(hex_str.as_bytes())
                .map_err(|e| format!("failed writing {}: {e}", kek_path.display()))?;
        }
        #[cfg(not(unix))]
        {
            std::fs::write(&kek_path, hex_str.as_bytes())
                .map_err(|e| format!("failed writing {}: {e}", kek_path.display()))?;
        }

        Ok(Some(new_key))
    }

    async fn has_sealed_rows(&self) -> Result<bool, MetaError> {
        let n = db_map_all!(
            self,
            "SELECT (SELECT COUNT(*) FROM byok_credentials) + (SELECT COUNT(*) FROM system_keyring) AS n",
            |r| { r.get::<i64, _>("n") }
        );
        Ok(n.first().copied().unwrap_or(0) > 0)
    }

    pub async fn list_system_keyring(&self) -> Result<Vec<SystemKeyringRecord>, MetaError> {
        let rows = db_map_all!(
            self,
            "SELECT key_id, salt_hex, sealed_key_hex, is_active, created_at_ms
             FROM system_keyring ORDER BY created_at_ms ASC",
            |r| {
                let active_i: i64 = r.get("is_active");
                SystemKeyringRecord {
                    key_id: r.get("key_id"),
                    salt_hex: r.get("salt_hex"),
                    sealed_key_hex: r.get("sealed_key_hex"),
                    is_active: active_i != 0,
                    created_at_ms: r.get("created_at_ms"),
                }
            },
        );
        Ok(rows)
    }

    pub async fn upsert_system_keyring_key(
        &self,
        rec: &SystemKeyringRecord,
    ) -> Result<(), MetaError> {
        if rec.is_active {
            let _ = db_exec!(self, "UPDATE system_keyring SET is_active = 0");
        }
        let active_val: i64 = if rec.is_active { 1 } else { 0 };
        let _ = db_exec!(
            self,
            "INSERT INTO system_keyring (key_id, salt_hex, sealed_key_hex, is_active, created_at_ms)
             VALUES (?, ?, ?, ?, ?)
             ON CONFLICT(key_id) DO UPDATE SET
                salt_hex = excluded.salt_hex,
                sealed_key_hex = excluded.sealed_key_hex,
                is_active = excluded.is_active",
            &rec.key_id,
            &rec.salt_hex,
            &rec.sealed_key_hex,
            active_val,
            rec.created_at_ms,
        );
        Ok(())
    }

    pub async fn deactivate_all_keyring_writes(&self) -> Result<(), MetaError> {
        let _ = db_exec!(self, "UPDATE system_keyring SET is_active = 0");
        Ok(())
    }

    pub async fn insert_enterprise_audit(
        &self,
        rec: &EnterpriseAuditRecord,
    ) -> Result<(), MetaError> {
        let djson = serde_json::to_string(&rec.details_json).unwrap_or_else(|_| "{}".into());
        let _ = db_exec!(
            self,
            "INSERT INTO enterprise_audit_log (id, actor, action, resource, details_json, created_at_ms)
             VALUES (?, ?, ?, ?, ?, ?)",
            &rec.id,
            &rec.actor,
            &rec.action,
            &rec.resource,
            djson,
            rec.created_at_ms,
        );
        Ok(())
    }

    pub async fn list_enterprise_audit(
        &self,
        limit: i64,
    ) -> Result<Vec<EnterpriseAuditRecord>, MetaError> {
        let rows = db_map_all!(
            self,
            "SELECT id, actor, action, resource, details_json, created_at_ms
             FROM enterprise_audit_log ORDER BY created_at_ms DESC LIMIT ?",
            |r| {
                let dstr: String = r.get("details_json");
                let details_json =
                    serde_json::from_str(&dstr).unwrap_or_else(|_| serde_json::json!({}));
                EnterpriseAuditRecord {
                    id: r.get("id"),
                    actor: r.get("actor"),
                    action: r.get("action"),
                    resource: r.get("resource"),
                    details_json,
                    created_at_ms: r.get("created_at_ms"),
                }
            },
            limit.clamp(1, 500),
        );
        Ok(rows)
    }

    pub async fn list_byok_credentials(
        &self,
        project_id: &str,
    ) -> Result<Vec<ByokCredentialRecord>, MetaError> {
        let rows = db_map_all!(
            self,
            "SELECT id, project_id, category, provider, label, key_prefix_masked, sealed_secret_hex, config_json, status, last_verified_at_ms, updated_at_ms
             FROM byok_credentials WHERE project_id = ? ORDER BY category ASC",
            |r| {
                let cstr: String = r.get("config_json");
                let config_json = serde_json::from_str(&cstr).unwrap_or_else(|_| serde_json::json!({}));
                ByokCredentialRecord {
                    id: r.get("id"),
                    project_id: r.get("project_id"),
                    category: r.get("category"),
                    provider: r.get("provider"),
                    label: r.get("label"),
                    key_prefix_masked: r.get("key_prefix_masked"),
                    sealed_secret_hex: r.get("sealed_secret_hex"),
                    config_json,
                    status: r.get("status"),
                    last_verified_at_ms: r.get("last_verified_at_ms"),
                    updated_at_ms: r.get("updated_at_ms"),
                }
            },
            project_id,
        );
        Ok(rows)
    }

    pub async fn get_byok_credential(
        &self,
        project_id: &str,
        category: &str,
    ) -> Result<Option<ByokCredentialRecord>, MetaError> {
        let rec = db_map_opt!(
            self,
            "SELECT id, project_id, category, provider, label, key_prefix_masked, sealed_secret_hex, config_json, status, last_verified_at_ms, updated_at_ms
             FROM byok_credentials WHERE project_id = ? AND category = ?",
            |r| {
                let cstr: String = r.get("config_json");
                let config_json = serde_json::from_str(&cstr).unwrap_or_else(|_| serde_json::json!({}));
                ByokCredentialRecord {
                    id: r.get("id"),
                    project_id: r.get("project_id"),
                    category: r.get("category"),
                    provider: r.get("provider"),
                    label: r.get("label"),
                    key_prefix_masked: r.get("key_prefix_masked"),
                    sealed_secret_hex: r.get("sealed_secret_hex"),
                    config_json,
                    status: r.get("status"),
                    last_verified_at_ms: r.get("last_verified_at_ms"),
                    updated_at_ms: r.get("updated_at_ms"),
                }
            },
            project_id,
            category,
        );
        Ok(rec)
    }

    pub async fn upsert_byok_credential(
        &self,
        rec: &ByokCredentialRecord,
    ) -> Result<(), MetaError> {
        let cstr = serde_json::to_string(&rec.config_json).unwrap_or_else(|_| "{}".into());
        let _ = db_exec!(
            self,
            "INSERT INTO byok_credentials (id, project_id, category, provider, label, key_prefix_masked, sealed_secret_hex, config_json, status, last_verified_at_ms, updated_at_ms)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(project_id, category) DO UPDATE SET
                id = excluded.id,
                provider = excluded.provider,
                label = excluded.label,
                key_prefix_masked = excluded.key_prefix_masked,
                sealed_secret_hex = excluded.sealed_secret_hex,
                config_json = excluded.config_json,
                status = excluded.status,
                last_verified_at_ms = excluded.last_verified_at_ms,
                updated_at_ms = excluded.updated_at_ms",
            &rec.id,
            &rec.project_id,
            &rec.category,
            &rec.provider,
            &rec.label,
            &rec.key_prefix_masked,
            &rec.sealed_secret_hex,
            cstr,
            &rec.status,
            rec.last_verified_at_ms,
            rec.updated_at_ms,
        );
        Ok(())
    }

    pub async fn touch_byok_verified(
        &self,
        project_id: &str,
        category: &str,
        status: &str,
        ts_ms: i64,
    ) -> Result<(), MetaError> {
        let _ = db_exec!(
            self,
            "UPDATE byok_credentials SET status = ?, last_verified_at_ms = ?, updated_at_ms = ?
             WHERE project_id = ? AND category = ?",
            status,
            ts_ms,
            ts_ms,
            project_id,
            category,
        );
        Ok(())
    }

    pub async fn delete_byok_credential(
        &self,
        project_id: &str,
        category: &str,
    ) -> Result<u64, MetaError> {
        let affected = db_exec!(
            self,
            "DELETE FROM byok_credentials WHERE project_id = ? AND category = ?",
            project_id,
            category,
        );
        Ok(affected)
    }
}

impl MetaStore {
    /// Takes or renews `node_id`'s writer lease on a project. `Some(generation)` when the node
    /// holds it afterwards; the generation grows by one each time the holder changes.
    pub async fn acquire_writer_lease(
        &self,
        project_id: &str,
        node_id: &str,
        now_ms: i64,
        expires_at_ms: i64,
    ) -> Result<Option<u64>, MetaError> {
        // A single statement, so two nodes racing for an expired lease cannot both win.
        db_exec!(
            self,
            "INSERT INTO writer_leases (project_id, node_id, generation, expires_at_ms)
             VALUES (?, ?, 1, ?)
             ON CONFLICT(project_id) DO UPDATE SET
                generation = CASE WHEN writer_leases.node_id = excluded.node_id
                    THEN writer_leases.generation ELSE writer_leases.generation + 1 END,
                node_id = excluded.node_id,
                expires_at_ms = excluded.expires_at_ms
             WHERE writer_leases.node_id = excluded.node_id OR writer_leases.expires_at_ms < ?",
            project_id,
            node_id,
            expires_at_ms,
            now_ms,
        );
        Ok(db_map_opt!(
            self,
            "SELECT generation FROM writer_leases WHERE project_id = ? AND node_id = ?",
            |r| { r.get::<i64, _>("generation") as u64 },
            project_id,
            node_id,
        ))
    }

    /// Expires every lease `node_id` holds so another node can take over at once.
    pub async fn release_writer_leases(&self, node_id: &str) -> Result<u64, MetaError> {
        Ok(db_exec!(
            self,
            "UPDATE writer_leases SET expires_at_ms = 0 WHERE node_id = ?",
            node_id,
        ))
    }
}

/// Writer leases kept in the metadata database, shared by every node of a deployment.
pub struct DbWriterLeases {
    meta: MetaStore,
    node_id: String,
    ttl: std::time::Duration,
}

impl DbWriterLeases {
    /// `PULSE_WRITER_LEASE_TTL_SECS` (default 15, minimum 3) sets how long a dead writer blocks
    /// a takeover. Node clocks must agree to within a third of it.
    pub fn from_env(meta: MetaStore) -> Self {
        let ttl_secs = std::env::var("PULSE_WRITER_LEASE_TTL_SECS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(15)
            .max(3);
        Self {
            meta,
            // Per process, never reused: a restarted node is a new holder and waits out its
            // old lease unless it released it on a clean shutdown.
            node_id: uuid::Uuid::new_v4().simple().to_string(),
            ttl: std::time::Duration::from_secs(ttl_secs),
        }
    }

    pub async fn release_all(&self) -> Result<u64, MetaError> {
        self.meta.release_writer_leases(&self.node_id).await
    }
}

#[async_trait::async_trait]
impl pulse_storage::WriterLeases for DbWriterLeases {
    async fn acquire(&self, project_id: &str) -> std::result::Result<Option<u64>, String> {
        let now_ms = chrono::Utc::now().timestamp_millis();
        self.meta
            .acquire_writer_lease(
                project_id,
                &self.node_id,
                now_ms,
                now_ms + self.ttl.as_millis() as i64,
            )
            .await
            .map_err(|e| e.to_string())
    }

    fn ttl(&self) -> std::time::Duration {
        self.ttl
    }
}

#[cfg(test)]
mod writer_lease_tests {
    use super::*;

    #[tokio::test]
    async fn lease_has_one_holder_and_generation_grows_on_each_change_of_holder() {
        let dir = tempfile::tempdir().unwrap();
        let meta = MetaStore::connect_auto(dir.path()).await.unwrap();
        let acquire = |node: &'static str, now_ms: i64| {
            let meta = meta.clone();
            async move {
                meta.acquire_writer_lease("p", node, now_ms, now_ms + 1_000)
                    .await
                    .unwrap()
            }
        };
        assert_eq!(acquire("a", 0).await, Some(1));
        assert_eq!(acquire("b", 500).await, None, "unexpired lease was taken");
        assert_eq!(
            acquire("a", 900).await,
            Some(1),
            "renewal changed generation"
        );
        assert_eq!(acquire("b", 1_500).await, None, "renewal did not extend");
        assert_eq!(acquire("b", 2_000).await, Some(2));
        assert_eq!(acquire("a", 2_100).await, None);
        // Renewing after its own expiry, with no other holder in between, keeps the generation.
        assert_eq!(acquire("b", 9_000).await, Some(2));
        meta.release_writer_leases("b").await.unwrap();
        assert_eq!(acquire("a", 9_001).await, Some(3));
        // Leases are per project.
        assert_eq!(
            meta.acquire_writer_lease("q", "b", 9_001, 10_000)
                .await
                .unwrap(),
            Some(1)
        );
    }

    #[tokio::test]
    async fn is_sqlite_delete_erased_uids_from_hwm_and_pg_rewrite() {
        assert_eq!(
            pg_rewrite("DELETE FROM erased_uids WHERE project_id = ? AND uid >= ?"),
            "DELETE FROM erased_uids WHERE project_id = $1 AND uid >= $2"
        );

        let dir = tempfile::tempdir().unwrap();
        let meta = MetaStore::connect_auto(dir.path()).await.unwrap();
        assert!(meta.is_sqlite());

        meta.insert_erased_uids("proj_a", &[0, 2, 5, 6], 10_000)
            .await
            .unwrap();
        meta.insert_erased_uids("proj_b", &[0, 5, 9], 20_000)
            .await
            .unwrap();
        meta.insert_deletion_audit(&DeletionAuditRecord {
            id: "del_1".into(),
            project_id: "proj_a".into(),
            distinct_id_masked: "u***".into(),
            canonical_uid: 5,
            events_purged: 10,
            replays_purged: 1,
            requested_by: "admin".into(),
            deleted_at_ms: 10_000,
        })
        .await
        .unwrap();

        let deleted = meta.delete_erased_uids_from_hwm("proj_a", 5).await.unwrap();
        assert_eq!(deleted, 2);

        let mut remaining_a = meta.list_erased_uids("proj_a").await.unwrap();
        remaining_a.sort_by_key(|(u, _)| *u);
        assert_eq!(remaining_a, vec![(0, 10_000), (2, 10_000)]);

        let mut remaining_b = meta.list_erased_uids("proj_b").await.unwrap();
        remaining_b.sort_by_key(|(u, _)| *u);
        assert_eq!(remaining_b, vec![(0, 20_000), (5, 20_000), (9, 20_000)]);

        let audit = meta.list_deletion_audit("proj_a").await.unwrap();
        assert_eq!(audit.len(), 1);
        assert_eq!(audit[0].id, "del_1");
    }
}
