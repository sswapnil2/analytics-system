use pulse_exec::{BehavioralCohortDef, CustomEventDef, CustomPropertyDef};
use serde::{Deserialize, Serialize};

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiKeyRecord {
    pub key_secret: String,
    pub project_id: String,
    pub label: String,
    pub role: String,
    pub created_at_ms: i64,
    pub revoked: bool,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dashboard {
    pub id: String,
    pub project_id: String,
    pub name: String,
    pub report_ids: Vec<String>,
    pub created_at_ms: i64,
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
    pub variants: Vec<VariantLiftRow>,
    pub guardrails: Vec<GuardrailResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeFlagRule {
    pub key: String,
    pub enabled: bool,
    pub rollout_bps: u32,
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
}
