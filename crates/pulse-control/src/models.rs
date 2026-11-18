use std::collections::BTreeMap;

use pulse_exec::{BehavioralCohortDef, CustomEventDef, CustomPropertyDef};
use rand::RngCore;
use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM};
use ring::rand::{SecureRandom, SystemRandom};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Organization {
    pub id: String,
    pub name: String,
    pub slug: String,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Project {
    pub id: String,
    pub org_id: String,
    pub name: String,
    pub timezone: String,
    pub created_at_ms: i64,
}

/// Dashboard user account. Deliberately not `Serialize` so `password_hash` can never leak into a response.
#[derive(Debug, Clone)]
pub struct User {
    pub id: String,
    /// Always stored lowercased.
    pub email: String,
    pub name: Option<String>,
    /// Argon2id PHC string; `None` for Google-only accounts.
    pub password_hash: Option<String>,
    pub google_sub: Option<String>,
    pub created_at_ms: i64,
    /// The organisation whose projects this account can see.
    pub org_id: String,
    /// The account's role in `org_id`.
    pub role: Role,
}

/// A user's role in their organisation. Ordered: each role can do everything the ones before it can.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Role {
    /// Reads reports, dashboards and runs queries.
    Viewer,
    /// Also creates and edits reports, dashboards, cohorts, flags, experiments and alerts.
    Member,
    /// Also manages API keys, members, invites, BYOK and data import/export.
    Admin,
    /// Also renames the organisation and manages other owners.
    Owner,
}

impl Role {
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Viewer => "viewer",
            Role::Member => "member",
            Role::Admin => "admin",
            Role::Owner => "owner",
        }
    }

    pub fn parse(s: &str) -> Option<Role> {
        Some(match s {
            "viewer" => Role::Viewer,
            "member" => Role::Member,
            "admin" => Role::Admin,
            "owner" => Role::Owner,
            _ => return None,
        })
    }

    /// Whether this role may grant `other`, or change or remove an account that holds it:
    /// owners manage anyone, everyone else only roles below their own.
    pub fn outranks(self, other: Role) -> bool {
        self == Role::Owner || other < self
    }
}

pub const KEY_ROLE_INGEST_AND_FLAGS: &str = "ingest_and_flags";
pub const KEY_ROLE_ADMIN: &str = "admin";

/// Computes a lowercase hex SHA-256 digest of a high-entropy API key or bearer token (`P0-02`).
pub fn sha256_hex(secret: &str) -> String {
    let digest = Sha256::digest(secret.as_bytes());
    hex::encode(digest)
}

/// Computes raw 32-byte SHA-256 digest for constant-time comparison (`P0-01`, `P0-02`).
pub fn sha256_bytes(secret: &str) -> [u8; 32] {
    Sha256::digest(secret.as_bytes()).into()
}

/// Builds a short non-sensitive display prefix (e.g. `"pk_live_a1b2…"`).
pub fn mask_key_prefix(secret: &str) -> String {
    let take = secret.len().min(12);
    format!("{}…", &secret[..take])
}

/// Generates a cryptographically random 192-bit `pk_live_...` or `sk_live_...` key,
/// returning `(plaintext_key, key_hash, key_prefix)`.
pub fn generate_random_api_key(role: &str) -> (String, String, String) {
    let mut raw = [0u8; 24];
    rand::rngs::OsRng.fill_bytes(&mut raw);
    let prefix = if role == KEY_ROLE_ADMIN {
        "sk_live_"
    } else {
        "pk_live_"
    };
    let plaintext = format!("{prefix}{}", hex::encode(raw));
    let key_hash = sha256_hex(&plaintext);
    let key_prefix = mask_key_prefix(&plaintext);
    (plaintext, key_hash, key_prefix)
}

/// Generates a cryptographically random 192-bit cluster root admin token (`sk_root_...`).
pub fn generate_root_admin_token() -> String {
    let mut raw = [0u8; 24];
    rand::rngs::OsRng.fill_bytes(&mut raw);
    format!("sk_root_{}", hex::encode(raw))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiKeyRecord {
    pub id: String,
    #[serde(skip_serializing)]
    pub key_hash: String,
    pub key_prefix: String,
    pub project_id: String,
    pub label: String,
    pub role: String,
    pub created_at_ms: i64,
    pub revoked: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreatedApiKeyResponse {
    pub record: ApiKeyRecord,
    /// Returned ONCE at creation time; never stored or returned again (`P0-02`).
    pub plaintext_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LexiconEvent {
    pub project_id: String,
    pub name: String,
    pub display_name: String,
    pub description: String,
    /// `"active"` | `"hidden"` | `"dropped"`
    pub status: String,
    pub owner: String,
    pub volume_24h: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LexiconProperty {
    pub project_id: String,
    pub name: String,
    pub prop_scope: String,
    pub inferred_types: String,
    /// `"active"` | `"dropped"`
    pub status: String,
    pub pii_masked: bool,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CohortRecord {
    pub id: String,
    pub project_id: String,
    pub name: String,
    pub description: String,
    pub definition: BehavioralCohortDef,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SavedReport {
    pub id: String,
    pub project_id: String,
    pub name: String,
    pub report_kind: String,
    pub query_json: serde_json::Value,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

/// A dated note on the project's timeline, drawn on report charts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Annotation {
    pub id: String,
    pub project_id: String,
    pub ts_ms: i64,
    pub text: String,
    pub created_by: Option<String>,
    pub created_at_ms: i64,
}

pub const REPORT_KINDS: [&str; 4] = ["funnel", "insights", "retention", "flows"];

/// One tile on a dashboard: a saved report or a markdown/text note.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardCard {
    /// Server-generated when empty on write.
    #[serde(default)]
    pub id: String,
    /// `"report"` or `"text"`.
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub report_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    /// `1` = half row, `2` = full row.
    #[serde(default = "default_card_width")]
    pub width: i64,
}

fn default_card_width() -> i64 {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dashboard {
    pub id: String,
    pub project_id: String,
    pub name: String,
    pub description: Option<String>,
    pub cards: Vec<DashboardCard>,
    /// User email, `"admin token"`, or `None` for rows created before ownership was recorded.
    pub created_by: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VariantAllocation {
    pub key: String,
    /// Basis points out of 10,000 (e.g. `5000` = 50%).
    pub weight_bps: u32,
    #[serde(default)]
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureFlag {
    pub id: String,
    pub project_id: String,
    pub key: String,
    pub description: String,
    pub enabled: bool,
    /// Rollout percentage `0.0 ..= 100.0`.
    pub rollout_pct: f64,
    pub targeting_cohort_id: Option<String>,
    pub variants: Vec<VariantAllocation>,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExperimentMetricSpec {
    pub name: String,
    pub event_name: String,
    /// `"conversion"` (binary user conversion) or `"continuous"` (numeric property average/sum per user).
    #[serde(default = "default_conversion_kind")]
    pub metric_type: String,
    #[serde(default)]
    pub numeric_prop: Option<String>,
    #[serde(default = "default_window_ms")]
    pub conversion_window_ms: i64,
}

fn default_conversion_kind() -> String {
    "conversion".to_string()
}

fn default_window_ms() -> i64 {
    7 * 86_400_000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Experiment {
    pub id: String,
    pub project_id: String,
    pub name: String,
    pub flag_id: String,
    pub flag_key: String,
    pub hypothesis: String,
    /// `"running"` | `"paused"` | `"concluded"`
    pub status: String,
    pub start_ts_ms: i64,
    pub end_ts_ms: i64,
    pub primary_metric: ExperimentMetricSpec,
    #[serde(default)]
    pub guardrail_metrics: Vec<ExperimentMetricSpec>,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VariantLiftRow {
    pub variant_key: String,
    pub is_control: bool,
    pub sample_size: u64,
    pub conversions: u64,
    pub conversion_rate: f64,
    pub mean_value: f64,
    pub variance: f64,
    pub relative_lift_pct: f64,
    pub ci95_low_pct: f64,
    pub ci95_high_pct: f64,
    pub p_value: f64,
    pub bayesian_prob_to_beat_control: f64,
    pub statistically_significant: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GuardrailResult {
    pub metric_name: String,
    pub event_name: String,
    pub variants: Vec<VariantLiftRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExperimentResults {
    pub experiment_id: String,
    pub experiment_name: String,
    pub flag_key: String,
    pub status: String,
    pub hypothesis: String,
    pub metric_name: String,
    pub exec_time_us: u64,
    #[serde(default)]
    pub srm_p_value: f64,
    #[serde(default)]
    pub sample_ratio_mismatch: bool,
    #[serde(default = "default_stats_note")]
    pub statistical_methodology_note: String,
    pub variants: Vec<VariantLiftRow>,
    pub guardrails: Vec<GuardrailResult>,
}

fn default_stats_note() -> String {
    "Fixed-horizon two-sided Z-test / Welch t-test (not peeking-corrected; Holm-Bonferroni adjusted across treatment variants)".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeFlagRule {
    pub key: String,
    pub enabled: bool,
    pub rollout_bps: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub targeting_cohort_id: Option<String>,
    /// Materialized sorted external `distinct_id` allowlist when `targeting_cohort_id` is set (`P2-02`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cohort_members: Option<Vec<String>>,
    pub variants: Vec<VariantAllocation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeFlagBundle {
    pub project_id: String,
    pub etag: String,
    pub generated_at_ms: i64,
    pub flags: Vec<EdgeFlagRule>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LookupTableSummary {
    pub id: String,
    pub project_id: String,
    pub name: String,
    pub join_prop: String,
    pub row_count: usize,
    pub sample_columns: Vec<String>,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomSchemaBundle {
    pub custom_events: Vec<CustomEventDef>,
    pub custom_properties: Vec<CustomPropertyDef>,
    pub lookup_tables: Vec<LookupTableSummary>,
    /// User profile property keys, queryable as `user.<key>` in filters and breakdowns.
    #[serde(default)]
    pub user_properties: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserProfileRecord {
    pub project_id: String,
    pub distinct_id: String,
    pub canonical_uid: u32,
    pub props: BTreeMap<String, Value>,
    pub total_revenue: f64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupProfileRecord {
    pub project_id: String,
    pub group_type: String,
    pub group_id: String,
    pub props: BTreeMap<String, Value>,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeletionAuditRecord {
    pub id: String,
    pub project_id: String,
    pub distinct_id_masked: String,
    pub canonical_uid: u32,
    pub events_purged: u64,
    pub replays_purged: u64,
    pub requested_by: String,
    pub deleted_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertRule {
    pub id: String,
    pub project_id: String,
    pub name: String,
    pub event_name: String,
    pub operator: String,
    pub threshold: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub webhook_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_triggered_ms: Option<i64>,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ProfileOpOutcome {
    pub modified: bool,
    pub delete_requested: bool,
}

fn ensure_array_mut<'a>(props: &'a mut BTreeMap<String, Value>, key: &str) -> &'a mut Vec<Value> {
    let entry = props
        .entry(key.to_string())
        .or_insert_with(|| Value::Array(Vec::new()));
    if !entry.is_array() {
        let prev = entry.clone();
        *entry = Value::Array(if prev.is_null() { vec![] } else { vec![prev] });
    }
    entry.as_array_mut().expect("guaranteed array")
}

fn items_from_val(val: &Value) -> Vec<Value> {
    match val {
        Value::Array(arr) => arr.clone(),
        other => vec![other.clone()],
    }
}

/// Applies user profile operators in deterministic arrival order (`Gap 1`).
pub fn apply_profile_ops(
    props: &mut BTreeMap<String, Value>,
    total_revenue: &mut f64,
    payload: &Value,
) -> ProfileOpOutcome {
    let mut outcome = ProfileOpOutcome::default();

    let Some(obj) = payload.as_object() else {
        return outcome;
    };

    const META_KEYS: &[&str] = &[
        "distinct_id",
        "$distinct_id",
        "user_id",
        "$user_id",
        "anon_id",
        "anonymous_id",
        "group_type",
        "$group_key",
        "group_id",
        "$group_id",
        "routing_seed",
        "$token",
        "$ip",
        "$time",
        "project_id",
    ];

    let mut handled_any_op = false;

    let mut apply_op = |op_name: &str, op_val: &Value| {
        handled_any_op = true;
        match op_name {
            "$clearAll" | "clear_all" => {
                if op_val.as_bool().unwrap_or(true) {
                    props.clear();
                    *total_revenue = 0.0;
                    outcome.modified = true;
                }
            }
            "$delete_user" | "delete_user" => {
                if !matches!(op_val, Value::Bool(false) | Value::Null) {
                    outcome.delete_requested = true;
                }
            }
            "$set" | "set" => {
                if let Some(m) = op_val.as_object() {
                    for (k, v) in m {
                        props.insert(k.clone(), v.clone());
                        outcome.modified = true;
                    }
                }
            }
            "$setOnce" | "$set_once" | "set_once" | "setOnce" => {
                if let Some(m) = op_val.as_object() {
                    for (k, v) in m {
                        if !props.contains_key(k) {
                            props.insert(k.clone(), v.clone());
                            outcome.modified = true;
                        }
                    }
                }
            }
            "$unset" | "unset" => match op_val {
                Value::Array(keys) => {
                    for k in keys.iter().filter_map(|v| v.as_str()) {
                        if props.remove(k).is_some() {
                            outcome.modified = true;
                        }
                    }
                }
                Value::Object(m) => {
                    for k in m.keys() {
                        if props.remove(k).is_some() {
                            outcome.modified = true;
                        }
                    }
                }
                Value::String(k) => {
                    if props.remove(k).is_some() {
                        outcome.modified = true;
                    }
                }
                _ => {}
            },
            "$add" | "increment" => {
                if let Some(m) = op_val.as_object() {
                    for (k, v) in m {
                        if let Some(delta) = v.as_f64() {
                            let cur = props.get(k).and_then(|c| c.as_f64()).unwrap_or(0.0);
                            let next = cur + delta;
                            if next.fract() == 0.0
                                && v.is_i64()
                                && props.get(k).is_none_or(|c| c.is_i64())
                                && next >= (i64::MIN as f64)
                                && next <= (i64::MAX as f64)
                            {
                                props.insert(k.clone(), Value::from(next as i64));
                            } else {
                                props.insert(k.clone(), Value::from(next));
                            }
                            outcome.modified = true;
                        }
                    }
                }
            }
            "$append" | "append" => {
                if let Some(m) = op_val.as_object() {
                    for (k, v) in m {
                        let arr = ensure_array_mut(props, k);
                        arr.push(v.clone());
                        outcome.modified = true;
                    }
                }
            }
            "$prepend" | "prepend" => {
                if let Some(m) = op_val.as_object() {
                    for (k, v) in m {
                        let items = items_from_val(v);
                        let arr = ensure_array_mut(props, k);
                        for (idx, it) in items.into_iter().enumerate() {
                            arr.insert(idx, it);
                        }
                        outcome.modified = true;
                    }
                }
            }
            "$union" | "union" => {
                if let Some(m) = op_val.as_object() {
                    for (k, v) in m {
                        let items = items_from_val(v);
                        let arr = ensure_array_mut(props, k);
                        for it in items {
                            if !arr.contains(&it) {
                                arr.push(it);
                                outcome.modified = true;
                            }
                        }
                    }
                }
            }
            "$preInsert" | "preInsert" => {
                if let Some(m) = op_val.as_object() {
                    for (k, v) in m {
                        let items = items_from_val(v);
                        let arr = ensure_array_mut(props, k);
                        for it in items.into_iter().rev() {
                            if !arr.contains(&it) {
                                arr.insert(0, it);
                                outcome.modified = true;
                            }
                        }
                    }
                }
            }
            "$postInsert" | "postInsert" => {
                if let Some(m) = op_val.as_object() {
                    for (k, v) in m {
                        let items = items_from_val(v);
                        let arr = ensure_array_mut(props, k);
                        for it in items {
                            if !arr.contains(&it) {
                                arr.push(it);
                                outcome.modified = true;
                            }
                        }
                    }
                }
            }
            "$remove" | "remove" => {
                if let Some(m) = op_val.as_object() {
                    for (k, v) in m {
                        let items = items_from_val(v);
                        if let Some(Value::Array(arr)) = props.get_mut(k) {
                            let prev = arr.len();
                            arr.retain(|existing| !items.contains(existing));
                            if arr.len() != prev {
                                outcome.modified = true;
                            }
                        }
                    }
                }
            }
            "$track_charge" | "track_charge" => {
                let charges = items_from_val(op_val);
                for ch in charges {
                    if let Some(amt) = ch
                        .get("$amount")
                        .or_else(|| ch.get("amount"))
                        .and_then(|v| v.as_f64())
                    {
                        *total_revenue += amt;
                        props.insert("$total_revenue".to_string(), Value::from(*total_revenue));
                        let txns = ensure_array_mut(props, "$transactions");
                        txns.push(ch);
                        outcome.modified = true;
                    }
                }
            }
            "$clear_charges" | "clear_charges" => {
                if !matches!(op_val, Value::Bool(false)) {
                    *total_revenue = 0.0;
                    props.remove("$total_revenue");
                    props.remove("$transactions");
                    outcome.modified = true;
                }
            }
            _ => {
                handled_any_op = false;
            }
        }
    };

    const OP_ORDER: &[&str] = &[
        "$clearAll",
        "clear_all",
        "$set",
        "set",
        "$setOnce",
        "$set_once",
        "set_once",
        "setOnce",
        "$add",
        "increment",
        "$append",
        "append",
        "$prepend",
        "prepend",
        "$union",
        "union",
        "$preInsert",
        "preInsert",
        "$postInsert",
        "postInsert",
        "$remove",
        "remove",
        "$unset",
        "unset",
        "$track_charge",
        "track_charge",
        "$clear_charges",
        "clear_charges",
        "$delete_user",
        "delete_user",
    ];

    for key in OP_ORDER {
        if let Some(val) = obj.get(*key) {
            apply_op(key, val);
        }
    }

    if let Some(ops_val) = obj.get("operations").or_else(|| obj.get("ops")) {
        if let Some(arr) = ops_val.as_array() {
            for step in arr {
                let sub = apply_profile_ops(props, total_revenue, step);
                outcome.modified |= sub.modified;
                outcome.delete_requested |= sub.delete_requested;
            }
            handled_any_op = true;
        } else if ops_val.is_object() {
            let sub = apply_profile_ops(props, total_revenue, ops_val);
            outcome.modified |= sub.modified;
            outcome.delete_requested |= sub.delete_requested;
            handled_any_op = true;
        }
    }

    if let Some(up) = obj.get("user_properties").and_then(|v| v.as_object()) {
        for (k, v) in up {
            props.insert(k.clone(), v.clone());
            outcome.modified = true;
        }
        handled_any_op = true;
    }

    if !handled_any_op {
        for (k, v) in obj {
            if !META_KEYS.contains(&k.as_str()) {
                props.insert(k.clone(), v.clone());
                outcome.modified = true;
            }
        }
    }

    outcome
}

#[cfg(test)]
mod profile_op_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn gap1_all_profile_operators_apply_cleanly() {
        let mut props = BTreeMap::new();
        let mut rev = 0.0;

        let out1 = apply_profile_ops(
            &mut props,
            &mut rev,
            &json!({
                "$set": { "plan": "pro", "city": "Bengaluru" },
                "$setOnce": { "plan": "free", "initial_utm_source": "google" },
                "$add": { "logins": 3 },
                "$append": { "tags": "vip" },
                "$prepend": { "tags": "kyc_done" },
                "$union": { "tags": ["vip", "early"] },
                "$preInsert": { "tags": ["first", "vip"] },
                "$postInsert": { "tags": ["last", "first"] },
                "$remove": { "tags": "kyc_done" },
                "$track_charge": { "$amount": 499.0, "plan": "pro" }
            }),
        );
        assert!(out1.modified && !out1.delete_requested);
        assert_eq!(props["plan"], json!("pro"));
        assert_eq!(props["initial_utm_source"], json!("google"));
        assert_eq!(props["logins"], json!(3));
        assert_eq!(props["tags"], json!(["first", "vip", "early", "last"]));
        assert_eq!(rev, 499.0);

        let out2 = apply_profile_ops(
            &mut props,
            &mut rev,
            &json!({
                "$unset": ["city"],
                "$clear_charges": true,
                "$delete_user": true
            }),
        );
        assert!(out2.modified && out2.delete_requested);
        assert!(!props.contains_key("city"));
        assert_eq!(rev, 0.0);
    }
}

pub const BYOK_CATEGORIES: &[&str] = &["llm", "storage", "webhook"];
const ALLOWED_BYOK_CONFIG_KEYS: &[&str] = &[
    "model",
    "endpoint",
    "url",
    "bucket_url",
    "region",
    "prefix",
    "channel",
];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemKeyringRecord {
    pub key_id: String,
    pub salt_hex: String,
    #[serde(skip_serializing)]
    pub sealed_key_hex: String,
    pub is_active: bool,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnterpriseAuditRecord {
    pub id: String,
    pub actor: String,
    pub action: String,
    pub resource: String,
    pub details_json: Value,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone)]
pub struct ByokCredentialRecord {
    pub id: String,
    pub project_id: String,
    pub category: String,
    pub provider: String,
    pub label: String,
    pub key_prefix_masked: String,
    pub sealed_secret_hex: String,
    pub config_json: Value,
    pub status: String,
    pub last_verified_at_ms: Option<i64>,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ByokCredentialView {
    pub id: String,
    pub project_id: String,
    pub category: String,
    pub provider: String,
    pub label: String,
    pub key_prefix_masked: String,
    pub config_json: Value,
    pub status: String,
    pub last_verified_at_ms: Option<i64>,
    pub updated_at_ms: i64,
}

/// Allowlists non-secret metadata fields returned in API responses so secrets in `config_json`
/// (e.g. `access_key`) are never echoed back (`M3`).
pub fn sanitize_byok_config_json(cfg: &Value) -> Value {
    let Some(obj) = cfg.as_object() else {
        return Value::Object(serde_json::Map::new());
    };
    let mut out = serde_json::Map::new();
    for &key in ALLOWED_BYOK_CONFIG_KEYS {
        if let Some(v) = obj.get(key) {
            out.insert(key.to_string(), v.clone());
        }
    }
    Value::Object(out)
}

/// `config_json` keys that hold secrets. At rest they live only as `<key>_sealed` (vault KEK).
const SEALED_BYOK_CONFIG_KEYS: &[&str] = &["access_key", "signing_secret"];

/// What gets written to the DB: allow-listed metadata plus sealed secret fields, no plaintext secrets.
pub fn seal_byok_config_json(kek: &[u8; 32], cfg: &Value) -> Value {
    let mut out = sanitize_byok_config_json(cfg);
    if let Some(obj) = out.as_object_mut() {
        for &key in SEALED_BYOK_CONFIG_KEYS {
            if let Some(s) = cfg
                .get(key)
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
            {
                obj.insert(format!("{key}_sealed"), Value::String(seal_secret(kek, s)));
            }
        }
    }
    out
}

/// Restores sealed secret fields for use. Rows saved before sealing keep their plaintext values.
pub fn unseal_byok_config_json(kek: &[u8; 32], cfg: &Value) -> Result<Value, String> {
    let mut out = cfg.clone();
    if let Some(obj) = out.as_object_mut() {
        for &key in SEALED_BYOK_CONFIG_KEYS {
            if let Some(hex) = cfg.get(format!("{key}_sealed")).and_then(Value::as_str) {
                obj.insert(key.to_string(), Value::String(unseal_secret(kek, hex)?));
            }
        }
    }
    Ok(out)
}

impl From<&ByokCredentialRecord> for ByokCredentialView {
    fn from(r: &ByokCredentialRecord) -> Self {
        Self {
            id: r.id.clone(),
            project_id: r.project_id.clone(),
            category: r.category.clone(),
            provider: r.provider.clone(),
            label: r.label.clone(),
            key_prefix_masked: r.key_prefix_masked.clone(),
            config_json: sanitize_byok_config_json(&r.config_json),
            status: r.status.clone(),
            last_verified_at_ms: r.last_verified_at_ms,
            updated_at_ms: r.updated_at_ms,
        }
    }
}

pub fn mask_byok_secret(secret: &str) -> String {
    let trimmed = secret.trim();
    let chars: Vec<char> = trimmed.chars().collect();
    let n = chars.len();
    if n <= 6 {
        "••••".to_string()
    } else if n <= 14 {
        let head: String = chars[..2].iter().collect();
        format!("{head}••••")
    } else {
        let head: String = chars[..8].iter().collect();
        let tail: String = chars[n - 4..].iter().collect();
        format!("{head}…{tail}")
    }
}

/// Seals a customer BYOK secret or keyring entry using AES-256-GCM with a random 96-bit nonce (`M2`).
pub fn seal_secret(kek: &[u8; 32], plaintext: &str) -> String {
    let rng = SystemRandom::new();
    let mut nonce_bytes = [0u8; 12];
    rng.fill(&mut nonce_bytes).expect("CSPRNG failure");
    let unbound = UnboundKey::new(&AES_256_GCM, kek).expect("32-byte KEK");
    let key = LessSafeKey::new(unbound);
    let mut ct = plaintext.as_bytes().to_vec();
    key.seal_in_place_append_tag(
        Nonce::assume_unique_for_key(nonce_bytes),
        Aad::from(b"PULSE_BYOK_VAULT_GCM_V2"),
        &mut ct,
    )
    .expect("AES-256-GCM seal");
    let mut out = Vec::with_capacity(12 + ct.len());
    out.extend_from_slice(&nonce_bytes);
    out.extend_from_slice(&ct);
    hex::encode(out)
}

/// Verifies AES-256-GCM integrity and decrypts a sealed BYOK credential (`M2`).
pub fn unseal_secret(kek: &[u8; 32], sealed_hex: &str) -> Result<String, String> {
    let raw = hex::decode(sealed_hex.trim()).map_err(|_| "malformed sealed secret".to_string())?;
    if raw.len() < 12 + 16 {
        return Err("sealed secret payload truncated".to_string());
    }
    let mut nonce_bytes = [0u8; 12];
    nonce_bytes.copy_from_slice(&raw[..12]);
    let unbound = UnboundKey::new(&AES_256_GCM, kek).map_err(|_| "invalid KEK".to_string())?;
    let key = LessSafeKey::new(unbound);
    let mut ct = raw[12..].to_vec();
    let pt = key
        .open_in_place(
            Nonce::assume_unique_for_key(nonce_bytes),
            Aad::from(b"PULSE_BYOK_VAULT_GCM_V2"),
            &mut ct,
        )
        .map_err(|_| {
            "sealed secret authentication failed (wrong cluster KEK or corrupted DB)".to_string()
        })?;
    String::from_utf8(pt.to_vec()).map_err(|_| "sealed secret is not valid UTF-8".to_string())
}

#[cfg(test)]
mod byok_config_seal_tests {
    use super::*;

    #[test]
    fn secrets_are_sealed_at_rest_and_restored_for_use() {
        let kek = [3u8; 32];
        let cfg = serde_json::json!({"url": "s3://b", "access_key": "AKIA123", "signing_secret": "s".repeat(20), "junk": 1});
        let stored = seal_byok_config_json(&kek, &cfg);
        let text = stored.to_string();
        assert!(
            !text.contains("AKIA123") && !text.contains(&"s".repeat(20)) && !text.contains("junk")
        );
        let used = unseal_byok_config_json(&kek, &stored).unwrap();
        assert_eq!(used["access_key"], "AKIA123");
        assert_eq!(used["url"], "s3://b");
        assert!(sanitize_byok_config_json(&stored)
            .get("access_key_sealed")
            .is_none());
    }
}
