//! Pulse Analytics — AI Agent Observability & Evaluation Core (`pulse-agent`).
//!
//! Provides:
//! - Canonical execution data model (`AgentTrace`, `AgentSpan`, `AgentEvaluation`,
//!   `AgentEvalRollup`, `SessionSummary`, `AgentErasureTombstone`, `AgentDataset`,
//!   `AgentExperiment`, `AgentAlertRule`, `AgentPrivacyPolicy`).
//! - `PLST` v1 columnar wire codec with bounded Zstd decompression and `xxh3_64` blob dedup.
//! - Server-side payload governance, PII redaction, and hard operational limits.
//! - Multi-provider token & integer `cost_nanos` calculation (OpenAI, Anthropic, Gemini + overrides).
//! - OpenTelemetry OTLP/JSON and OTLP/Protobuf (`gen_ai.*` + OpenInference) normalizer.
//! - Modular multi-level evaluation framework (session, trace, span deterministic + LLM-as-judge).
//! - Statistical percentile, loop detection, failure attribution, and regression comparison helpers.

pub mod evaluators;
pub mod model;
pub mod otlp;
pub mod pricing;
pub mod privacy;
pub mod protocol;
pub mod stats;
pub mod store;

pub use evaluators::*;
pub use model::*;
pub use otlp::*;
pub use pricing::*;
pub use privacy::*;
pub use protocol::*;
pub use stats::*;
pub use store::{AgentTraceStore, AgentTraceStoreConfig, StoreError};

/// Hard operational limits to guarantee bounded memory and predictable query latency.
pub const MAX_SPANS_PER_TRACE: usize = 512;
pub const MAX_SPAN_DEPTH: usize = 32;
pub const MAX_LINKED_SPANS_PER_SPAN: usize = 64;
pub const MAX_PAYLOAD_BYTES: usize = 64 * 1024; // 64 KiB per input/output payload field
pub const MAX_ATTRIBUTES_PER_SPAN: usize = 64;
pub const MAX_EVENTS_PER_SPAN: usize = 64;
pub const MAX_TAGS_PER_TRACE: usize = 32;
pub const MAX_TRACE_LIST_LIMIT: usize = 100;
pub const DEFAULT_RETENTION_DAYS: i64 = 30;
pub const MIN_SIGNIFICANCE_SAMPLES: usize = 20;
pub const NANOS_PER_USD: i64 = 1_000_000_000;
