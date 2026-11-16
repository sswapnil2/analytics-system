use pulse_wire::PropValue;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimeRange {
    pub start_ts_ms: i64,
    pub end_ts_ms: i64,
}

impl TimeRange {
    #[inline]
    pub fn contains(&self, ts_ms: i64) -> bool {
        ts_ms >= self.start_ts_ms && ts_ms <= self.end_ts_ms
    }

    #[inline]
    pub fn duration_ms(&self) -> i64 {
        (self.end_ts_ms - self.start_ts_ms).max(1)
    }

    pub fn previous_period(&self) -> Self {
        let dur = self.duration_ms();
        Self {
            start_ts_ms: self.start_ts_ms - dur,
            end_ts_ms: self.start_ts_ms - 1,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum TimeInterval {
    Hour,
    #[default]
    Day,
    Week,
    Month,
}

impl TimeInterval {
    pub fn step_ms(self) -> i64 {
        match self {
            TimeInterval::Hour => 3_600_000,
            TimeInterval::Day => 86_400_000,
            TimeInterval::Week => 7 * 86_400_000,
            TimeInterval::Month => 30 * 86_400_000,
        }
    }

    pub fn align_floor(self, ts_ms: i64) -> i64 {
        let step = self.step_ms();
        ts_ms.div_euclid(step) * step
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", content = "val", rename_all = "snake_case")]
pub enum PropFilterOp {
    Eq(PropValue),
    Neq(PropValue),
    Gt(f64),
    Gte(f64),
    Lt(f64),
    Lte(f64),
    Contains(String),
    In(Vec<String>),
    IsSet,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PropPredicate {
    pub prop_name: String,
    pub filter: PropFilterOp,
}

impl PropPredicate {
    pub fn eq_str(prop_name: impl Into<String>, val: impl Into<String>) -> Self {
        Self {
            prop_name: prop_name.into(),
            filter: PropFilterOp::Eq(PropValue::Str(val.into())),
        }
    }

    pub fn matches(&self, candidate: Option<&PropValue>) -> bool {
        match (&self.filter, candidate) {
            (PropFilterOp::IsSet, Some(v)) => !matches!(v, PropValue::Null),
            (PropFilterOp::IsSet, None) => false,
            (PropFilterOp::Neq(target), None) => !matches!(target, PropValue::Null),
            (_, None | Some(PropValue::Null)) => false,
            (PropFilterOp::Eq(target), Some(actual)) => values_equal_coalesced(actual, target),
            (PropFilterOp::Neq(target), Some(actual)) => !values_equal_coalesced(actual, target),
            (PropFilterOp::Gt(bound), Some(actual)) => actual.as_f64().is_some_and(|v| v > *bound),
            (PropFilterOp::Gte(bound), Some(actual)) => {
                actual.as_f64().is_some_and(|v| v >= *bound)
            }
            (PropFilterOp::Lt(bound), Some(actual)) => actual.as_f64().is_some_and(|v| v < *bound),
            (PropFilterOp::Lte(bound), Some(actual)) => {
                actual.as_f64().is_some_and(|v| v <= *bound)
            }
            (PropFilterOp::Contains(sub), Some(actual)) => actual.as_str_cow().contains(sub),
            (PropFilterOp::In(options), Some(actual)) => {
                let cow = actual.as_str_cow();
                options.iter().any(|o| o == cow.as_ref())
            }
        }
    }
}

#[inline]
pub fn values_equal_coalesced(a: &PropValue, b: &PropValue) -> bool {
    if a == b {
        return true;
    }
    if let (Some(fa), Some(fb)) = (a.as_f64(), b.as_f64()) {
        return (fa - fb).abs() < 1e-9;
    }
    a.as_str_cow() == b.as_str_cow()
}

/// Recursive Behavioral Cohort AST evaluated to an exact `RoaringBitmap` of canonical `u32` user IDs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BehavioralCohortDef {
    PerformedEvent {
        event_name: String,
        min_count: u32,
        #[serde(default)]
        time_range: Option<TimeRange>,
        #[serde(default)]
        where_clauses: Vec<PropPredicate>,
    },
    NotPerformedEvent {
        event_name: String,
        #[serde(default)]
        time_range: Option<TimeRange>,
    },
    PropertyMatch {
        predicate: PropPredicate,
        #[serde(default)]
        time_range: Option<TimeRange>,
    },
    CohortRef {
        cohort_id: String,
    },
    And {
        clauses: Vec<BehavioralCohortDef>,
    },
    Or {
        clauses: Vec<BehavioralCohortDef>,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct GlobalFilter {
    #[serde(default)]
    pub predicates: Vec<PropPredicate>,
    #[serde(default)]
    pub cohort: Option<BehavioralCohortDef>,
}

// ======================= INSIGHTS =======================

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InsightsMetric {
    #[default]
    Uniques,
    Totals,
    Sessions,
    Dau,
    Wau,
    Mau,
    PropSum {
        prop: String,
    },
    PropAvg {
        prop: String,
    },
    PropMin {
        prop: String,
    },
    PropMax {
        prop: String,
    },
    PropPercentile {
        prop: String,
        quantile: f64,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InsightsSeriesSpec {
    pub label: String,
    pub event_name: String,
    #[serde(default)]
    pub metric: InsightsMetric,
    #[serde(default)]
    pub filters: Vec<PropPredicate>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InsightsQuery {
    pub time_range: TimeRange,
    #[serde(default)]
    pub interval: TimeInterval,
    pub series: Vec<InsightsSeriesSpec>,
    #[serde(default)]
    pub breakdown_by: Vec<String>,
    #[serde(default)]
    pub global_filter: GlobalFilter,
    #[serde(default)]
    pub compare_previous_period: bool,
    #[serde(default)]
    pub formula: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimePoint {
    pub bucket_ts_ms: i64,
    pub value: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeriesOutput {
    pub label: String,
    pub breakdown_key: String,
    pub overall_value: f64,
    pub points: Vec<TimePoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InsightsResult {
    pub snapshot_epoch: u64,
    pub exec_time_us: u64,
    pub buckets_ts_ms: Vec<i64>,
    pub series: Vec<SeriesOutput>,
    pub previous_series: Vec<SeriesOutput>,
    pub formula_series: Vec<SeriesOutput>,
}

// ======================= FUNNELS =======================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum FunnelOrderMode {
    StrictOrder,
    #[default]
    LooseOrder,
    AnyOrder,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FunnelStepSpec {
    pub event_name: String,
    #[serde(default)]
    pub filters: Vec<PropPredicate>,
    #[serde(default)]
    pub first_time_ever: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FunnelExclusion {
    pub event_name: String,
    /// Step index pair `(from_step, to_step)` e.g. `(1, 2)` excludes users hitting `event_name` between Step 1 and Step 2.
    pub between_steps: (usize, usize),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FunnelQuery {
    pub time_range: TimeRange,
    pub steps: Vec<FunnelStepSpec>,
    #[serde(default)]
    pub order_mode: FunnelOrderMode,
    /// Conversion window in milliseconds (default 7 days if 0).
    pub conversion_window_ms: i64,
    #[serde(default)]
    pub exclusions: Vec<FunnelExclusion>,
    #[serde(default)]
    pub hold_constant_props: Vec<String>,
    #[serde(default)]
    pub breakdown_by: Option<String>,
    #[serde(default)]
    pub global_filter: GlobalFilter,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunnelStepCount {
    pub step_index: usize,
    pub event_name: String,
    pub entered_users: u64,
    pub dropped_users: u64,
    pub conversion_from_start_pct: f64,
    pub conversion_from_prev_pct: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunnelBreakdownGroup {
    pub breakdown_value: String,
    pub steps: Vec<FunnelStepCount>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeToConvertStats {
    pub converted_count: u64,
    pub avg_ms: f64,
    pub p50_ms: f64,
    pub p90_ms: f64,
    pub p99_ms: f64,
    pub histogram_bins: Vec<(f64, f64, u64)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DroppedSessionSample {
    pub dropped_before_step: usize,
    pub session_id: String,
    pub canonical_uid: u32,
    pub distinct_id: String,
    pub drop_ts_ms: i64,
    pub replay_offset_kb: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunnelResult {
    pub snapshot_epoch: u64,
    pub exec_time_us: u64,
    pub overall_steps: Vec<FunnelStepCount>,
    pub breakdowns: Vec<FunnelBreakdownGroup>,
    pub time_to_convert: TimeToConvertStats,
    pub dropped_sample_sessions: Vec<DroppedSessionSample>,
}

// ======================= RETENTION =======================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum RetentionMode {
    #[default]
    NDay,
    Unbounded,
    FrequencyStickiness,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RetentionQuery {
    pub time_range: TimeRange,
    pub start_event: String,
    pub return_event: String,
    #[serde(default)]
    pub mode: RetentionMode,
    #[serde(default)]
    pub unit: TimeInterval,
    pub num_periods: usize,
    #[serde(default)]
    pub global_filter: GlobalFilter,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionCohortRow {
    pub cohort_start_ts_ms: i64,
    pub cohort_label: String,
    pub cohort_size: u64,
    /// For `NDay` / `Unbounded`: active count in period `0..=num_periods`.
    /// For `FrequencyStickiness`: count of users active on `>= (k+1)` distinct intervals in window.
    pub retained_counts: Vec<u64>,
    pub retained_pcts: Vec<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionResult {
    pub snapshot_epoch: u64,
    pub exec_time_us: u64,
    pub mode: RetentionMode,
    pub cohorts: Vec<RetentionCohortRow>,
}

// ======================= FLOWS =======================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum FlowDirection {
    #[default]
    Forward,
    Reverse,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FlowsQuery {
    pub time_range: TimeRange,
    pub anchor_event: String,
    #[serde(default)]
    pub direction: FlowDirection,
    pub steps: usize,
    #[serde(default)]
    pub top_n_per_step: Option<usize>,
    #[serde(default)]
    pub global_filter: GlobalFilter,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowNode {
    pub id: String,
    pub step_offset: i32,
    pub event_name: String,
    pub user_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowLink {
    pub source: String,
    pub target: String,
    pub user_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowsResult {
    pub snapshot_epoch: u64,
    pub exec_time_us: u64,
    pub nodes: Vec<FlowNode>,
    pub links: Vec<FlowLink>,
}

// ======================= SESSIONS EXPLORER =======================

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SessionSearchQuery {
    pub time_range: Option<TimeRange>,
    pub event_name: Option<String>,
    #[serde(default)]
    pub only_rage_tap: bool,
    #[serde(default)]
    pub only_dead_tap: bool,
    #[serde(default)]
    pub only_errors: bool,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionSummaryItem {
    pub session_id: String,
    pub canonical_uid: u32,
    pub distinct_id: String,
    pub start_ts_ms: i64,
    pub end_ts_ms: i64,
    pub duration_ms: i64,
    pub event_count: u32,
    pub has_rage_tap: bool,
    pub has_dead_tap: bool,
    pub has_error: bool,
    pub events: Vec<(i64, String, u32)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionSearchResult {
    pub snapshot_epoch: u64,
    pub exec_time_us: u64,
    pub total_sessions_inspected: usize,
    pub sessions_pruned_by_bloom: usize,
    pub sessions: Vec<SessionSummaryItem>,
}
