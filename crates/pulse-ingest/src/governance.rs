use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use pulse_wire::{mask_input_value, PropValue, WireEvent};

/// Cached Lexicon enforcement rules for a single project.
#[derive(Debug, Clone, Default)]
pub struct ProjectLexiconRules {
    pub dropped_events: HashSet<String>,
    pub dropped_properties: HashSet<String>,
    pub pii_masked_properties: HashSet<String>,
}

impl ProjectLexiconRules {
    /// Applies Lexicon governance to `ev`. Returns `false` if the entire event is dropped.
    pub fn apply(&self, ev: &mut WireEvent) -> bool {
        if self.dropped_events.contains(&ev.event_name) {
            return false;
        }
        if !self.dropped_properties.is_empty() {
            ev.props
                .retain(|(k, _)| !self.dropped_properties.contains(k));
        }
        if !self.pii_masked_properties.is_empty() {
            for (k, v) in &mut ev.props {
                if self.pii_masked_properties.contains(k) {
                    let masked = match &*v {
                        PropValue::Str(s) => mask_input_value(s),
                        other => mask_input_value(&other.as_str_cow()),
                    };
                    *v = PropValue::Str(masked);
                }
            }
        }
        true
    }
}

#[derive(Debug, Clone)]
pub struct DiscoveredEventSchema {
    pub project_id: String,
    pub event_name: String,
    pub count: u64,
    pub props: Vec<(String, &'static str)>,
}

#[async_trait]
pub trait GovernanceProvider: Send + Sync {
    /// Resolves `x-pulse-key` header to a valid `project_id`, or `None` if unauthorized.
    async fn authenticate_api_key(&self, api_key: &str) -> Option<String>;

    /// Fetches cached Lexicon enforcement rules for `project_id`.
    fn lexicon_rules(&self, project_id: &str) -> Arc<ProjectLexiconRules>;

    /// Asynchronously records newly observed event/property schema items.
    fn record_discovery(&self, items: Vec<DiscoveredEventSchema>);

    /// Applies user profile operations from `/v1/engage` or `/v1/identify` (`Gap 1`).
    async fn apply_engage(
        &self,
        _project_id: &str,
        _distinct_id: &str,
        canonical_uid: u32,
        _ops: &serde_json::Value,
    ) -> crate::Result<u32> {
        Ok(canonical_uid)
    }

    /// Called after `anon_id` is merged into `user_id` so profile stores can follow the merge.
    async fn on_identify(
        &self,
        _project_id: &str,
        _anon_id: &str,
        _user_id: &str,
        canonical_uid: u32,
    ) -> crate::Result<u32> {
        Ok(canonical_uid)
    }

    /// Applies Group Analytics profile operations from `/v1/groups` (`Gap 8`).
    async fn apply_group_engage(
        &self,
        _project_id: &str,
        _group_type: &str,
        _group_id: &str,
        _ops: &serde_json::Value,
    ) -> crate::Result<()> {
        Ok(())
    }
}

/// Fast in-memory governance provider for standalone tests and zero-latency caching.
#[derive(Default)]
pub struct InMemoryGovernance {
    api_keys: DashMap<String, String>,
    rules: DashMap<String, Arc<ProjectLexiconRules>>,
    discovered_events: DashMap<(String, String), u64>,
}

impl InMemoryGovernance {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn register_api_key(&self, api_key: impl Into<String>, project_id: impl Into<String>) {
        self.api_keys.insert(api_key.into(), project_id.into());
    }

    pub fn set_rules(&self, project_id: impl Into<String>, rules: ProjectLexiconRules) {
        self.rules.insert(project_id.into(), Arc::new(rules));
    }

    pub fn discovered_count(&self, project_id: &str, event_name: &str) -> u64 {
        self.discovered_events
            .get(&(project_id.to_string(), event_name.to_string()))
            .map(|r| *r)
            .unwrap_or(0)
    }
}

#[async_trait]
impl GovernanceProvider for InMemoryGovernance {
    async fn authenticate_api_key(&self, api_key: &str) -> Option<String> {
        self.api_keys.get(api_key).map(|v| v.clone())
    }

    fn lexicon_rules(&self, project_id: &str) -> Arc<ProjectLexiconRules> {
        self.rules
            .get(project_id)
            .map(|r| r.clone())
            .unwrap_or_else(|| Arc::new(ProjectLexiconRules::default()))
    }

    fn record_discovery(&self, items: Vec<DiscoveredEventSchema>) {
        for item in items {
            *self
                .discovered_events
                .entry((item.project_id, item.event_name))
                .or_insert(0) += item.count;
        }
    }
}
