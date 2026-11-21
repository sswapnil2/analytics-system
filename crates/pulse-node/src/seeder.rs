use std::sync::Arc;
use std::time::Instant;

use chrono::Utc;
use pulse_control::{
    AlertRule, AnalysisConfig, CohortRecord, ControlPlaneService, Dashboard, DashboardCard,
    Experiment, ExperimentMetricSpec, FeatureFlag, LexiconProperty, MetricDenominator, SavedReport,
    VariantAllocation,
};
use pulse_exec::{
    BehavioralCohortDef, CustomEventDef, CustomPropertyDef, LookupTableDef, TimeInterval,
};
use pulse_ingest::IngestService;
use pulse_wire::{
    InteractionKind, NodeKind, PropValue, ReplayChunk, WireBatch, WireEvent, WireNode, WireframeOp,
};
use tracing::info;

#[derive(Debug, Clone)]
pub struct SeedSummary {
    pub unique_events: usize,
    pub duplicate_retries: usize,
    pub users_seeded: usize,
    pub replay_sessions: usize,
    pub elapsed_ms: u128,
}

#[inline]
fn seed_hash(salt: &str, u: usize) -> u64 {
    let mut buf = [0u8; 32];
    let sb = salt.as_bytes();
    let copy = sb.len().min(24);
    buf[..copy].copy_from_slice(&sb[..copy]);
    buf[24..32].copy_from_slice(&(u as u64).to_le_bytes());
    xxhash_rust::xxh3::xxh3_64(&buf)
}

fn pick_cohort_day(u: usize) -> i64 {
    // Smooth two-week acquisition trend with realistic weekend softenings on days 5-6
    const WEIGHTS: [u64; 14] = [48, 52, 60, 63, 66, 54, 51, 70, 74, 82, 86, 89, 101, 104];
    let slot = seed_hash("cohort_day", u) % 1000;
    let mut acc = 0u64;
    for (idx, w) in WEIGHTS.iter().enumerate() {
        acc += *w;
        if slot < acc {
            return idx as i64;
        }
    }
    13
}

fn pick_city(u: usize) -> &'static str {
    const CITIES: [(&str, u64); 6] = [
        ("Bengaluru", 290),
        ("Mumbai", 220),
        ("Delhi_NCR", 180),
        ("Hyderabad", 130),
        ("Pune", 100),
        ("Chennai", 80),
    ];
    let slot = seed_hash("city_dist", u) % 1000;
    let mut acc = 0u64;
    for (name, w) in CITIES {
        acc += w;
        if slot < acc {
            return name;
        }
    }
    "Bengaluru"
}

/// Generates the deterministic 100,000+ event Indian Consumer Fintech & Quick-Commerce dataset
/// across 8,000 users over 14 days on first boot.
pub async fn seed_indian_fintech_dataset(
    ctrl: &Arc<ControlPlaneService>,
    ingest: &Arc<IngestService>,
    project_id: &str,
) -> anyhow::Result<SeedSummary> {
    let t0 = Instant::now();
    let now_ms = Utc::now().timestamp_millis();
    let today_floor = TimeInterval::Day.align_floor(now_ms);
    let start_day_ms = today_floor - 13 * 86_400_000;

    // Name the demo for what it is; a first owner who names their project at signup overrides this.
    ctrl.meta()
        .rename_project(project_id, "Demo: consumer lending app")
        .await?;

    // 1. Seed Control Plane Governance, Lookup Table, Custom Schema, Cohort, Flag & Experiment
    ctrl.meta()
        .upsert_lexicon_property(&LexiconProperty {
            project_id: project_id.to_string(),
            name: "pan_masked".to_string(),
            prop_scope: "event".to_string(),
            inferred_types: "string".to_string(),
            status: "active".to_string(),
            pii_masked: true,
            updated_at_ms: now_ms,
        })
        .await?;
    ctrl.refresh_project_lexicon_cache(project_id)
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

    let lookup_csv = b"merchant_id,category,margin_pct,tier\n\
m_zepto,Quick_Grocery,18.5,Tier_1\n\
m_blinkit,Quick_Grocery,17.8,Tier_1\n\
m_swiggy,Food_Delivery,22.0,Tier_1\n\
m_zomato,Food_Delivery,21.5,Tier_1\n\
m_bb_daily,Dairy_Subscription,14.2,Tier_2\n\
m_croma,Electronics_EMI,9.5,Prime\n";
    let lt_def =
        LookupTableDef::from_csv_bytes("merchant_category_lookup", "merchant_id", lookup_csv)
            .map_err(|e| anyhow::anyhow!(e))?;
    ctrl.meta()
        .upsert_lookup_table("lt_merchant_category_lookup", project_id, &lt_def, now_ms)
        .await?;

    ctrl.meta()
        .upsert_custom_event(
            "ce_checkout_any",
            project_id,
            &CustomEventDef {
                name: "Checkout_Any".to_string(),
                source_events: vec![
                    "First_Disbursal_Success".to_string(),
                    "Quick_Commerce_Order_Paid".to_string(),
                ],
                filter: None,
            },
            now_ms,
        )
        .await?;

    ctrl.meta()
        .upsert_custom_property(
            "cp_ticket_band",
            project_id,
            &CustomPropertyDef {
                name: "ticket_band".to_string(),
                sql_expr: "CASE WHEN amount > 5000 THEN 'Prime_Credit' ELSE 'Micro_Ticket' END"
                    .to_string(),
                referenced_props: vec!["amount".to_string()],
            },
            now_ms,
        )
        .await?;

    ctrl.meta()
        .upsert_cohort(&CohortRecord {
            id: "coh_upi_mandate_power".to_string(),
            project_id: project_id.to_string(),
            name: "Approved UPI Autopay Users".to_string(),
            description: "Users who completed UPI_Autopay_Mandate_Approved at least once in 14d"
                .to_string(),
            definition: BehavioralCohortDef::PerformedEvent {
                event_name: "UPI_Autopay_Mandate_Approved".to_string(),
                min_count: 1,
                time_range: None,
                where_clauses: Vec::new(),
            },
            created_at_ms: now_ms,
        })
        .await?;

    ctrl.refresh_project_virtual_catalog(project_id)
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

    let flag = FeatureFlag {
        id: "flg_one_tap_upi_mandate".to_string(),
        project_id: project_id.to_string(),
        key: "one_tap_upi_mandate".to_string(),
        description: "Bottom-sheet 1-Tap UPI Autopay Mandate vs legacy multi-step VPA redirect"
            .to_string(),
        enabled: true,
        rollout_pct: 50.0,
        targeting_cohort_id: None,
        holdout: None,
        layer: None,
        variants: vec![
            VariantAllocation {
                key: "control".to_string(),
                weight_bps: 5000,
                payload: serde_json::json!({"flow": "vpa_redirect"}),
            },
            VariantAllocation {
                key: "one_tap_sheet".to_string(),
                weight_bps: 5000,
                payload: serde_json::json!({"flow": "intent_bottom_sheet"}),
            },
        ],
        updated_at_ms: now_ms,
    };
    ctrl.meta().upsert_feature_flag(&flag).await?;

    let exp = Experiment {
        id: "exp_one_tap_upi_mandate".to_string(),
        project_id: project_id.to_string(),
        name: "1-Tap UPI Autopay Mandate Sheet".to_string(),
        flag_id: flag.id.clone(),
        flag_key: flag.key.clone(),
        hypothesis: "Native intent bottom-sheet reduces NPCI VPA entry drop-off and lifts UPI_Autopay_Mandate_Approved by >12% without increasing NSDL/Gateway timeouts.".to_string(),
        status: "running".to_string(),
        start_ts_ms: start_day_ms,
        end_ts_ms: now_ms + 7 * 86_400_000,
        primary_metric: ExperimentMetricSpec {
            name: "UPI Mandate Approval Rate".to_string(),
            event_name: "UPI_Autopay_Mandate_Approved".to_string(),
            metric_type: "conversion".to_string(),
            numeric_prop: None,
            filters: Vec::new(),
            denominator: None,
            conversion_window_ms: 7 * 86_400_000,
            direction: "decrease_is_bad".to_string(),
            auto_stop: false,
        },
        guardrail_metrics: vec![
            ExperimentMetricSpec {
                name: "NSDL Gateway 504 Timeout Rate".to_string(),
                event_name: "NSDL_Gateway_504_Timeout".to_string(),
                metric_type: "conversion".to_string(),
                numeric_prop: None,
                filters: Vec::new(),
                denominator: None,
                conversion_window_ms: 7 * 86_400_000,
                direction: "increase_is_bad".to_string(),
                auto_stop: true,
            },
            ExperimentMetricSpec {
                name: "Mandate amount per user".to_string(),
                event_name: "UPI_Autopay_Mandate_Approved".to_string(),
                metric_type: "sum".to_string(),
                numeric_prop: Some("amount".to_string()),
                filters: Vec::new(),
                denominator: None,
                conversion_window_ms: 7 * 86_400_000,
                direction: "decrease_is_bad".to_string(),
                auto_stop: false,
            },
        ],
        secondary_metrics: vec![
            ExperimentMetricSpec {
                name: "First disbursal".to_string(),
                event_name: "First_Disbursal_Success".to_string(),
                metric_type: "conversion".to_string(),
                numeric_prop: None,
                filters: Vec::new(),
                denominator: None,
                conversion_window_ms: 7 * 86_400_000,
                direction: "decrease_is_bad".to_string(),
                auto_stop: false,
            },
            ExperimentMetricSpec {
                name: "Mandate to disbursal".to_string(),
                event_name: "First_Disbursal_Success".to_string(),
                metric_type: "step_through".to_string(),
                numeric_prop: None,
                filters: Vec::new(),
                denominator: Some(MetricDenominator {
                    event_name: "UPI_Autopay_Mandate_Approved".to_string(),
                    numeric_prop: None,
                }),
                conversion_window_ms: 7 * 86_400_000,
                direction: "decrease_is_bad".to_string(),
                auto_stop: false,
            },
        ],
        // Demo users are exposed on their first day, so CUPED has no pre-period to use here.
        analysis: AnalysisConfig {
            sequential: true,
            cuped: true,
            // The hypothesis promises >12%, so the verdict checks the range against it.
            target_lift_pct: Some(12.0),
            ..AnalysisConfig::default()
        },
        created_at_ms: now_ms,
        created_by: "Pulse demo".to_string(),
        problem: "Users drop off at the UPI Autopay mandate step, where they type their VPA by hand.".to_string(),
        owner: "Pulse demo".to_string(),
        notify: true,
        stop_reason: None,
        kind: "ab".to_string(),
        bandit: None,
        decision: None,
    };
    ctrl.meta().upsert_experiment(&exp).await?;

    // 2. Seed Multi-Frame Frustrated Wireframe Replay Sessions across ALL funnel drop-off steps
    let total_users = 8_000usize;
    let replay_session_count = 32usize;
    let mut replay_offsets_by_sess: std::collections::HashMap<String, u32> =
        std::collections::HashMap::new();
    let mut kyc_timeout_sessions: std::collections::HashSet<String> =
        std::collections::HashSet::new();

    for r_idx in 0..replay_session_count {
        let u = 200 + r_idx * 17;
        let sess_id = format!("sess_kyc_timeout_{u:05}");
        kyc_timeout_sessions.insert(sess_id.clone());
        let base_t = start_day_ms + pick_cohort_day(u) * 86_400_000 + 38_000_000;

        let chunk1 = ReplayChunk {
            project_id: project_id.to_string(),
            session_id: sess_id.clone(),
            chunk_start_ts_ms: base_t,
            ops: vec![
                WireframeOp::FullSnapshot {
                    ts_offset_ms: 0,
                    width: 390,
                    height: 844,
                    nodes: vec![
                        WireNode {
                            node_id: 1,
                            parent_id: 0,
                            kind: NodeKind::Screen,
                            x: 0,
                            y: 0,
                            w: 390,
                            h: 844,
                            bg_rgba: 0x090D_16FF,
                            masked_label: "Pulse Credit — Instant PAN KYC".to_string(),
                        },
                        WireNode {
                            node_id: 2,
                            parent_id: 1,
                            kind: NodeKind::Container,
                            x: 18,
                            y: 72,
                            w: 354,
                            h: 92,
                            bg_rgba: 0x1118_27FF,
                            masked_label: "Pre-Approved Credit Limit ₹75,000 • 0% Foreclosure"
                                .to_string(),
                        },
                        WireNode {
                            node_id: 3,
                            parent_id: 1,
                            kind: NodeKind::Text,
                            x: 24,
                            y: 188,
                            w: 280,
                            h: 24,
                            bg_rgba: 0x0000_0000,
                            masked_label: "Enter 10-character Permanent Account Number".to_string(),
                        },
                        WireNode {
                            node_id: 4,
                            parent_id: 1,
                            kind: NodeKind::MaskedInput,
                            x: 24,
                            y: 220,
                            w: 342,
                            h: 52,
                            bg_rgba: 0x1E29_3BFF,
                            masked_label: "████".to_string(),
                        },
                        WireNode {
                            node_id: 5,
                            parent_id: 1,
                            kind: NodeKind::MaskedInput,
                            x: 24,
                            y: 286,
                            w: 342,
                            h: 52,
                            bg_rgba: 0x1E29_3BFF,
                            masked_label: "████".to_string(),
                        },
                        WireNode {
                            node_id: 6,
                            parent_id: 1,
                            kind: NodeKind::Button,
                            x: 24,
                            y: 362,
                            w: 342,
                            h: 54,
                            bg_rgba: 0x3B82_F6FF,
                            masked_label: "Verify PAN".to_string(),
                        },
                    ],
                },
                WireframeOp::PointerInteraction {
                    ts_offset_ms: 900,
                    x: 195,
                    y: 246,
                    kind: InteractionKind::Tap,
                },
                WireframeOp::PointerInteraction {
                    ts_offset_ms: 2_100,
                    x: 195,
                    y: 389,
                    kind: InteractionKind::Tap,
                },
                WireframeOp::NetworkBreadcrumb {
                    ts_offset_ms: 7_150,
                    method: "POST".to_string(),
                    path: "/api/v2/kyc/nsdl-pan-verify".to_string(),
                    status_code: 504,
                    duration_ms: 5_042,
                },
            ],
        };
        let _ = ctrl
            .qengine()
            .storage()
            .append_replay_chunk(&chunk1)
            .await?;

        let chunk2 = ReplayChunk {
            project_id: project_id.to_string(),
            session_id: sess_id.clone(),
            chunk_start_ts_ms: base_t + 7_500,
            ops: vec![
                WireframeOp::NodeUpsert {
                    ts_offset_ms: 0,
                    node: WireNode {
                        node_id: 7,
                        parent_id: 1,
                        kind: NodeKind::ErrorBanner,
                        x: 24,
                        y: 434,
                        w: 342,
                        h: 56,
                        bg_rgba: 0xDC26_26FF,
                        masked_label:
                            "NSDL_Gateway_504_Timeout: Upstream KYC verification timed out"
                                .to_string(),
                    },
                },
                WireframeOp::PointerInteraction {
                    ts_offset_ms: 600,
                    x: 198,
                    y: 388,
                    kind: InteractionKind::RageTap { count: 4 },
                },
                WireframeOp::PointerInteraction {
                    ts_offset_ms: 1_350,
                    x: 192,
                    y: 460,
                    kind: InteractionKind::DeadTap,
                },
                WireframeOp::NetworkBreadcrumb {
                    ts_offset_ms: 6_420,
                    method: "POST".to_string(),
                    path: "/api/v2/kyc/nsdl-pan-verify".to_string(),
                    status_code: 504,
                    duration_ms: 5_011,
                },
            ],
        };
        let r2 = ctrl
            .qengine()
            .storage()
            .append_replay_chunk(&chunk2)
            .await?;
        replay_offsets_by_sess.insert(sess_id, r2.replay_offset_kb);
    }

    // Seed recordings for Step 0->1 (Calculator/PAN Gate), Step 2->3 (UPI Autopay Sheet), and Step 3->4 (Disbursal e-Sign)
    let extra_step_replays: &[(usize, &str, &str, &str)] = &[
        (
            6005,
            "Instant Line Offer — Verify PAN",
            "Camera & CKYC permission prompt stalled",
            "/api/v2/kyc/ckyc-consent",
        ),
        (
            6022,
            "Instant Line Offer — Verify PAN",
            "Camera & CKYC permission prompt stalled",
            "/api/v2/kyc/ckyc-consent",
        ),
        (
            6039,
            "Instant Line Offer — Verify PAN",
            "Camera & CKYC permission prompt stalled",
            "/api/v2/kyc/ckyc-consent",
        ),
        (
            6056,
            "Instant Line Offer — Verify PAN",
            "Camera & CKYC permission prompt stalled",
            "/api/v2/kyc/ckyc-consent",
        ),
        (
            300,
            "UPI Autopay — Confirm Mandate",
            "NPCI VPA intent app switch returned 502",
            "/api/v2/upi/mandate-create",
        ),
        (
            304,
            "UPI Autopay — Confirm Mandate",
            "NPCI VPA intent app switch returned 502",
            "/api/v2/upi/mandate-create",
        ),
        (
            306,
            "UPI Autopay — Confirm Mandate",
            "NPCI VPA intent app switch returned 502",
            "/api/v2/upi/mandate-create",
        ),
        (
            308,
            "UPI Autopay — Confirm Mandate",
            "NPCI VPA intent app switch returned 502",
            "/api/v2/upi/mandate-create",
        ),
        (
            164,
            "Sanction Letter — Final e-Sign",
            "IMPS penny-drop verification timed out",
            "/api/v2/disbursal/penny-drop",
        ),
        (
            166,
            "Sanction Letter — Final e-Sign",
            "IMPS penny-drop verification timed out",
            "/api/v2/disbursal/penny-drop",
        ),
        (
            168,
            "Sanction Letter — Final e-Sign",
            "IMPS penny-drop verification timed out",
            "/api/v2/disbursal/penny-drop",
        ),
        (
            170,
            "Sanction Letter — Final e-Sign",
            "IMPS penny-drop verification timed out",
            "/api/v2/disbursal/penny-drop",
        ),
    ];
    for &(u, screen_title, err_msg, api_path) in extra_step_replays {
        let sess_id = format!("sess_onboard_{u:05}");
        let base_t = start_day_ms + pick_cohort_day(u) * 86_400_000 + 38_000_000;
        let chunk = ReplayChunk {
            project_id: project_id.to_string(),
            session_id: sess_id.clone(),
            chunk_start_ts_ms: base_t,
            ops: vec![
                WireframeOp::FullSnapshot {
                    ts_offset_ms: 0,
                    width: 390,
                    height: 844,
                    nodes: vec![
                        WireNode {
                            node_id: 1,
                            parent_id: 0,
                            kind: NodeKind::Screen,
                            x: 0,
                            y: 0,
                            w: 390,
                            h: 844,
                            bg_rgba: 0x090D_16FF,
                            masked_label: screen_title.to_string(),
                        },
                        WireNode {
                            node_id: 2,
                            parent_id: 1,
                            kind: NodeKind::MaskedInput,
                            x: 24,
                            y: 200,
                            w: 342,
                            h: 52,
                            bg_rgba: 0x1E29_3BFF,
                            masked_label: "████".to_string(),
                        },
                        WireNode {
                            node_id: 3,
                            parent_id: 1,
                            kind: NodeKind::Button,
                            x: 24,
                            y: 340,
                            w: 342,
                            h: 52,
                            bg_rgba: 0x3B82_F6FF,
                            masked_label: "Continue".to_string(),
                        },
                        WireNode {
                            node_id: 4,
                            parent_id: 1,
                            kind: NodeKind::ErrorBanner,
                            x: 24,
                            y: 420,
                            w: 342,
                            h: 56,
                            bg_rgba: 0xDC26_26FF,
                            masked_label: err_msg.to_string(),
                        },
                    ],
                },
                WireframeOp::PointerInteraction {
                    ts_offset_ms: 1_200,
                    x: 195,
                    y: 366,
                    kind: InteractionKind::Tap,
                },
                WireframeOp::PointerInteraction {
                    ts_offset_ms: 2_800,
                    x: 196,
                    y: 368,
                    kind: InteractionKind::DeadTap,
                },
                WireframeOp::NetworkBreadcrumb {
                    ts_offset_ms: 4_100,
                    method: "POST".to_string(),
                    path: api_path.to_string(),
                    status_code: 502,
                    duration_ms: 3_420,
                },
            ],
        };
        let res = ctrl.qengine().storage().append_replay_chunk(&chunk).await?;
        replay_offsets_by_sess.insert(sess_id, res.replay_offset_kb);
    }

    // 3. Generate 100,000+ Analytical Events + Retroactive Aliases across 8,000 Users over 14 Days
    let networks = ["Jio_5G", "Airtel_5G", "Jio_4G", "Airtel_4G", "WiFi_Fiber"];
    let merchants = [
        "m_zepto",
        "m_blinkit",
        "m_swiggy",
        "m_zomato",
        "m_bb_daily",
        "m_croma",
    ];

    let id_graph = ctrl
        .qengine()
        .storage()
        .identity_router()
        .get_or_open(project_id)
        .await?;
    let num_buckets = ctrl.qengine().storage().config().num_buckets;

    let mut all_events: Vec<WireEvent> = Vec::with_capacity(115_000);
    let mut seq = 1u64;

    for u in 0..total_users {
        let cohort_day = pick_cohort_day(u);
        let time_of_day_ms = ((seed_hash("tod", u) % 61_200) + 18_000) as i64 * 1_000; // active 05:00..22:00 UTC/IST spread
        let user_day0_ts = start_day_ms + cohort_day * 86_400_000 + time_of_day_ms;

        let anon_id = format!("anon_in_{u:05}");
        let phone_id = format!("+919845{:06}", 100_000 + u);
        let primary_seed = format!("install_primary_{u:05}");
        // 5% of users (400 users) are cross-device (`iOS` phone + `Web`/`Android` second device with distinct routing_seed!)
        let is_cross_device = u < 400;
        let secondary_seed = if is_cross_device {
            format!("install_secondary_ipad_web_{u:05}")
        } else {
            primary_seed.clone()
        };

        let city = pick_city(u);
        let net_slot = seed_hash("network", u) % 100;
        let net = if net_slot < 36 {
            networks[0]
        } else if net_slot < 64 {
            networks[1]
        } else if net_slot < 80 {
            networks[2]
        } else if net_slot < 91 {
            networks[3]
        } else {
            networks[4]
        };
        let platform_primary = if seed_hash("platform", u) % 100 < 26 {
            "iOS"
        } else {
            "Android"
        };
        let pay_slot = seed_hash("payment", u) % 100;
        let payment_method = if pay_slot < 20 {
            "card"
        } else if pay_slot < 31 {
            "netbanking"
        } else {
            "upi"
        };
        let merchant = merchants[(seed_hash("merchant", u) % (merchants.len() as u64)) as usize];
        let base_amount = 650 + (seed_hash("amount", u) % 18_500) as i64;

        let is_step2_3_replay = [300usize, 304, 306, 308].contains(&u);
        let is_step3_4_replay = [164usize, 166, 168, 170].contains(&u);

        let sess_onboard =
            if u >= 200 && (u - 200) % 17 == 0 && (u - 200) / 17 < replay_session_count {
                format!("sess_kyc_timeout_{u:05}")
            } else {
                format!("sess_onboard_{u:05}")
            };
        let replay_kb = replay_offsets_by_sess
            .get(&sess_onboard)
            .copied()
            .unwrap_or(0);

        let base_props = |amt: i64, extra: Option<(&str, PropValue)>| -> Vec<(String, PropValue)> {
            let mut p = vec![
                ("city".to_string(), PropValue::Str(city.to_string())),
                (
                    "platform".to_string(),
                    PropValue::Str(platform_primary.to_string()),
                ),
                ("network".to_string(), PropValue::Str(net.to_string())),
                (
                    "payment_method".to_string(),
                    PropValue::Str(payment_method.to_string()),
                ),
                (
                    "merchant_id".to_string(),
                    PropValue::Str(merchant.to_string()),
                ),
                // Exercise polymorphic `amount` across Android/iOS safely!
                (
                    "amount".to_string(),
                    if u % 11 == 0 {
                        PropValue::Float(amt as f64 + 0.50)
                    } else {
                        PropValue::Int(amt)
                    },
                ),
            ];
            if let Some((ek, ev)) = extra {
                p.push((ek.to_string(), ev));
            }
            p
        };

        // --- Anonymous Device Exploration (`anon_id`) ---
        let t_browse = 5_000 + (seed_hash("t_browse", u) % 12_000) as i64;
        let t_calc = t_browse + 7_000 + (seed_hash("t_calc", u) % 18_000) as i64;
        let t_otp = t_calc + 10_000 + (seed_hash("t_otp", u) % 24_000) as i64;
        let t_pan = t_otp + 15_000 + (seed_hash("t_pan", u) % 45_000) as i64;
        let t_nsdl = t_pan + 4_500 + (seed_hash("t_nsdl", u) % 7_500) as i64;
        let tail_vkyc = if seed_hash("vkyc_tail", u) % 100 < 15 {
            240_000 + (seed_hash("vkyc_tail_ms", u) % 600_000) as i64
        } else {
            0
        };
        let t_vkyc = t_pan + 28_000 + (seed_hash("t_vkyc", u) % 150_000) as i64 + tail_vkyc;
        let t_exp = t_vkyc + 8_000 + (seed_hash("t_exp", u) % 25_000) as i64;
        let tail_mandate = if seed_hash("mandate_tail", u) % 100 < 18 {
            180_000 + (seed_hash("mandate_tail_ms", u) % 540_000) as i64
        } else {
            0
        };
        let t_mandate = t_exp + 14_000 + (seed_hash("t_mandate", u) % 95_000) as i64 + tail_mandate;
        let tail_disb = if seed_hash("disb_tail", u) % 100 < 12 {
            220_000 + (seed_hash("disb_tail_ms", u) % 720_000) as i64
        } else {
            0
        };
        let t_disbursal =
            t_mandate + 18_000 + (seed_hash("t_disb", u) % 140_000) as i64 + tail_disb;

        for (dt, ename, r_kb) in [
            (0i64, "App_Open", 0),
            (t_browse, "Browse_Credit_Card", 0),
            (
                t_calc,
                "View_Loan_Calculator",
                if u >= 6_000 { replay_kb } else { 0 },
            ),
        ] {
            seq += 1;
            all_events.push(WireEvent {
                event_uuid: WireEvent::deterministic_uuid(
                    &anon_id,
                    &sess_onboard,
                    ename,
                    user_day0_ts + dt,
                    seq,
                ),
                routing_seed: primary_seed.clone(),
                distinct_id: anon_id.clone(),
                session_id: sess_onboard.clone(),
                event_name: ename.to_string(),
                client_event_ts_ms: user_day0_ts + dt,
                client_sent_ts_ms: now_ms,
                props: base_props(base_amount, None),
                replay_offset_kb: r_kb,
            });
        }

        // --- Mid-Session Login (`OTP_Verified` + Retroactive `alias(anon_id -> phone_id)`) ---
        // 85% of users verify OTP and log in (`u < 6800`)
        if u < 6_800 {
            let b_anon = pulse_storage::route_bucket(&primary_seed, num_buckets);
            let b_user = pulse_storage::route_bucket(&secondary_seed, num_buckets);
            id_graph.alias(&anon_id, &phone_id, b_anon, b_user)?;

            seq += 1;
            all_events.push(WireEvent {
                event_uuid: WireEvent::deterministic_uuid(
                    &phone_id,
                    &sess_onboard,
                    "OTP_Verified",
                    user_day0_ts + t_otp,
                    seq,
                ),
                routing_seed: primary_seed.clone(),
                distinct_id: phone_id.clone(),
                session_id: sess_onboard.clone(),
                event_name: "OTP_Verified".to_string(),
                client_event_ts_ms: user_day0_ts + t_otp,
                client_sent_ts_ms: now_ms,
                props: base_props(base_amount, None),
                replay_offset_kb: 0,
            });
        }

        let active_did = if u < 6_800 { &phone_id } else { &anon_id };

        // --- Funnel Step 1: `KYC_PAN_Submitted` (6,000 users = 75% of 8,000) ---
        if u < 6_000 {
            seq += 1;
            all_events.push(WireEvent {
                event_uuid: WireEvent::deterministic_uuid(
                    active_did,
                    &sess_onboard,
                    "KYC_PAN_Submitted",
                    user_day0_ts + t_pan,
                    seq,
                ),
                routing_seed: primary_seed.clone(),
                distinct_id: active_did.clone(),
                session_id: sess_onboard.clone(),
                event_name: "KYC_PAN_Submitted".to_string(),
                client_event_ts_ms: user_day0_ts + t_pan,
                client_sent_ts_ms: now_ms,
                props: base_props(
                    base_amount,
                    Some(("pan_masked", PropValue::Str("████".into()))),
                ),
                replay_offset_kb: replay_kb,
            });

            // Frustration cohort (~380 users, including all 32 KYC-timeout replay sessions) hits `NSDL_Gateway_504_Timeout`!
            let hit_nsdl_timeout = kyc_timeout_sessions.contains(&sess_onboard)
                || (seed_hash("nsdl_504", u) % 16 == 5);
            if hit_nsdl_timeout {
                seq += 1;
                all_events.push(WireEvent {
                    event_uuid: WireEvent::deterministic_uuid(
                        active_did,
                        &sess_onboard,
                        "NSDL_Gateway_504_Timeout",
                        user_day0_ts + t_nsdl,
                        seq,
                    ),
                    routing_seed: primary_seed.clone(),
                    distinct_id: active_did.clone(),
                    session_id: sess_onboard.clone(),
                    event_name: "NSDL_Gateway_504_Timeout".to_string(),
                    client_event_ts_ms: user_day0_ts + t_nsdl,
                    client_sent_ts_ms: now_ms,
                    props: base_props(base_amount, Some(("status_code", PropValue::Int(504)))),
                    replay_offset_kb: replay_kb,
                });
            }

            // --- Funnel Step 2: `Video_KYC_Completed` & A/B Experiment Exposure ---
            if u < 5_000 && !kyc_timeout_sessions.contains(&sess_onboard) {
                seq += 1;
                all_events.push(WireEvent {
                    event_uuid: WireEvent::deterministic_uuid(
                        active_did,
                        &sess_onboard,
                        "Video_KYC_Completed",
                        user_day0_ts + t_vkyc,
                        seq,
                    ),
                    routing_seed: secondary_seed.clone(),
                    distinct_id: active_did.clone(),
                    session_id: sess_onboard.clone(),
                    event_name: "Video_KYC_Completed".to_string(),
                    client_event_ts_ms: user_day0_ts + t_vkyc,
                    client_sent_ts_ms: now_ms,
                    props: base_props(base_amount, None),
                    replay_offset_kb: if is_step2_3_replay { replay_kb } else { 0 },
                });

                // Natural hash-based ~50/50 split on `exp_one_tap_upi_mandate`:
                //   control       ~48.0% conversion
                //   one_tap_sheet ~55.0% conversion (~+14.5% relative lift)
                let is_treatment =
                    pulse_wire::flag_variant_slot("one_tap_upi_mandate", active_did, 10_000)
                        >= 5_000;
                let variant = if is_treatment {
                    "one_tap_sheet"
                } else {
                    "control"
                };

                seq += 1;
                all_events.push(WireEvent {
                    event_uuid: WireEvent::deterministic_uuid(
                        active_did,
                        &sess_onboard,
                        "$experiment_started",
                        user_day0_ts + t_exp,
                        seq,
                    ),
                    routing_seed: secondary_seed.clone(),
                    distinct_id: active_did.clone(),
                    session_id: sess_onboard.clone(),
                    event_name: "$experiment_started".to_string(),
                    client_event_ts_ms: user_day0_ts + t_exp,
                    client_sent_ts_ms: now_ms,
                    props: vec![
                        (
                            "flag_key".to_string(),
                            PropValue::Str("one_tap_upi_mandate".to_string()),
                        ),
                        ("variant".to_string(), PropValue::Str(variant.to_string())),
                        ("city".to_string(), PropValue::Str(city.to_string())),
                    ],
                    replay_offset_kb: 0,
                });

                let conv_slot = (seed_hash("upi_mandate_conv", u) % 10_000) as u32;
                let converts_mandate = if is_step2_3_replay {
                    false
                } else if is_step3_4_replay {
                    true
                } else if is_treatment {
                    conv_slot < 5_350
                } else {
                    conv_slot < 4_800
                };

                if converts_mandate {
                    seq += 1;
                    all_events.push(WireEvent {
                        event_uuid: WireEvent::deterministic_uuid(
                            active_did,
                            &sess_onboard,
                            "UPI_Autopay_Mandate_Approved",
                            user_day0_ts + t_mandate,
                            seq,
                        ),
                        routing_seed: secondary_seed.clone(),
                        distinct_id: active_did.clone(),
                        session_id: sess_onboard.clone(),
                        event_name: "UPI_Autopay_Mandate_Approved".to_string(),
                        client_event_ts_ms: user_day0_ts + t_mandate,
                        client_sent_ts_ms: now_ms,
                        props: base_props(
                            base_amount,
                            Some(("$exp_one_tap_upi_mandate", PropValue::Str(variant.into()))),
                        ),
                        replay_offset_kb: if is_step3_4_replay { replay_kb } else { 0 },
                    });

                    // --- Funnel Step 4: `First_Disbursal_Success` (~82% of Mandate-Approved users) ---
                    if !is_step3_4_replay && (seed_hash("disbursal_conv", u) % 100) < 82 {
                        seq += 1;
                        all_events.push(WireEvent {
                            event_uuid: WireEvent::deterministic_uuid(
                                active_did,
                                &sess_onboard,
                                "First_Disbursal_Success",
                                user_day0_ts + t_disbursal,
                                seq,
                            ),
                            routing_seed: secondary_seed.clone(),
                            distinct_id: active_did.clone(),
                            session_id: sess_onboard.clone(),
                            event_name: "First_Disbursal_Success".to_string(),
                            client_event_ts_ms: user_day0_ts + t_disbursal,
                            client_sent_ts_ms: now_ms,
                            props: base_props(base_amount, None),
                            replay_offset_kb: 0,
                        });
                    }
                }
            }
        }

        // --- Multi-Day Retention & Quick-Commerce Repeat Sessions across Days +1 .. (13 - cohort_day) ---
        let remaining_days = (13 - cohort_day).max(0);
        for d_off in 1..=remaining_days {
            // Smooth decaying retention curve (~62% D1 -> ~47% D3 -> ~35% D7 -> ~28% D13) yielding 105,000+ unique events
            let retention_threshold = match d_off {
                1 => 62,
                2 => 54,
                3 => 47,
                4..=6 => 40,
                7..=10 => 35,
                _ => 28,
            };
            if (seed_hash("ret_day", u * 32 + d_off as usize) % 100) < retention_threshold {
                let ret_ts = user_day0_ts
                    + d_off * 86_400_000
                    + (seed_hash("ret_tod", u * 32 + d_off as usize) % 7_200) as i64 * 1_000;
                let ret_sess = format!("sess_ret_{u:05}_d{d_off}");

                for (dt, ename) in [
                    (0i64, "App_Open"),
                    (12_000, "Quick_Grocery_Browse"),
                    (28_000, "Quick_Commerce_Order_Paid"),
                ] {
                    if ename == "Quick_Commerce_Order_Paid"
                        && (seed_hash("ret_order", u * 32 + d_off as usize) % 100) < 34
                    {
                        continue;
                    }
                    seq += 1;
                    all_events.push(WireEvent {
                        event_uuid: WireEvent::deterministic_uuid(
                            active_did,
                            &ret_sess,
                            ename,
                            ret_ts + dt,
                            seq,
                        ),
                        routing_seed: primary_seed.clone(),
                        distinct_id: active_did.clone(),
                        session_id: ret_sess.clone(),
                        event_name: ename.to_string(),
                        client_event_ts_ms: ret_ts + dt,
                        client_sent_ts_ms: now_ms,
                        props: base_props(
                            320 + (seed_hash("ret_amt", u * 32 + d_off as usize) % 1800) as i64,
                            None,
                        ),
                        replay_offset_kb: 0,
                    });
                }
            }
        }
    }

    id_graph.flush().await?;

    // The synthetic timeline can run past "now" for the newest cohort (a few hours). Real clients never
    // send future events, so slide the whole timeline back; gaps between a user's events stay the same.
    let overrun = all_events
        .iter()
        .map(|e| e.client_event_ts_ms)
        .max()
        .unwrap_or(now_ms)
        - now_ms;
    if overrun > 0 {
        for e in &mut all_events {
            e.client_event_ts_ms -= overrun;
        }
    }
    let unique_events = all_events.len();
    // Separate 10% simulated metro/flight delayed offline events (sent in later waves to exercise out-of-order L1 compaction!)
    let mut primary_stream = Vec::with_capacity(unique_events);
    let mut delayed_offline_stream = Vec::with_capacity(unique_events / 9);
    for (idx, ev) in all_events.into_iter().enumerate() {
        if idx % 10 == 7 {
            delayed_offline_stream.push(ev);
        } else {
            primary_stream.push(ev);
        }
    }

    let mut duplicate_retries = 0usize;
    for chunk in primary_stream.chunks(20_000) {
        let mut wave = chunk.to_vec();
        // Inject 15% simulated flaky Jio/Airtel 4G duplicate retries
        let dup_count = (chunk.len() * 15) / 100;
        wave.extend_from_slice(&chunk[..dup_count]);
        duplicate_retries += dup_count;

        ingest
            .ingest_for_project(project_id, WireBatch::new(wave), now_ms)
            .await?;
    }

    // Flush primary stream to immutable Vortex L1 segments
    ctrl.qengine().storage().flush_all_projects().await?;

    // Now ingest the 10% delayed 3-hour-late offline batches in chunks & run Compactor!
    for chunk in delayed_offline_stream.chunks(5_000) {
        let mut wave = chunk.to_vec();
        let dup_count = (chunk.len() * 15) / 100;
        wave.extend_from_slice(&chunk[..dup_count]);
        duplicate_retries += dup_count;

        ingest
            .ingest_for_project(project_id, WireBatch::new(wave), now_ms)
            .await?;
    }
    ctrl.qengine().storage().flush_all_projects().await?;
    ctrl.qengine()
        .storage()
        .compact_all_with_min_segments(2)
        .await?;
    let _ = ctrl.qengine().warm_project_cache(project_id).await;

    // Seed representative User Profiles + default Alert Rule for User Profile queries (`Gaps 1, 5, 14`)
    let mut profile_indices: Vec<usize> = (0..260usize).collect();
    profile_indices.extend([300, 304, 306, 308, 6005, 6022, 6039, 6056]);
    profile_indices.sort_unstable();
    profile_indices.dedup();
    let utm_sources = [
        "google_ads",
        "meta_ig",
        "organic",
        "referral_upi",
        "paytm_partner",
    ];
    let utm_campaigns = [
        "diwali_zero_fee",
        "upi_autopay_launch",
        "instant_credit_q4",
        "organic",
    ];
    let pstore = ctrl
        .qengine()
        .storage()
        .profile_router()
        .get_or_open(project_id)
        .await?;
    let mut seeded_profiles = Vec::with_capacity(profile_indices.len());
    for u in profile_indices {
        let active_did = if u < 6_800 {
            format!("+919845{:06}", 100_000 + u)
        } else {
            format!("anon_in_{u:05}")
        };
        let canon_uid = id_graph
            .lookup_canonical(&active_did)
            .or_else(|| id_graph.lookup_raw(&active_did))
            .unwrap_or(0);
        let city = pick_city(u);
        let platform = if seed_hash("platform", u) % 100 < 26 {
            "iOS"
        } else {
            "Android"
        };
        let plan_slot = seed_hash("plan", u) % 100;
        let plan = if plan_slot < 24 {
            "credit_pro"
        } else if plan_slot < 62 {
            "upi_plus"
        } else {
            "basic"
        };
        let kyc_status = if u >= 6_000 {
            "pending"
        } else if u >= 200 && (u - 200) % 17 == 0 && (u - 200) / 17 < replay_session_count {
            "nsdl_timeout"
        } else {
            "verified"
        };
        let utm_src = utm_sources[(seed_hash("utm_s", u) % utm_sources.len() as u64) as usize];
        let utm_cmp = if utm_src == "organic" {
            "organic"
        } else {
            utm_campaigns[(seed_hash("utm_c", u) % (utm_campaigns.len() as u64 - 1)) as usize]
        };
        let total_rev = if kyc_status == "verified" && (seed_hash("prof_rev", u) % 100) < 44 {
            ((seed_hash("prof_rev_amt", u) % 18) as f64 + 1.0) * 500.0
        } else {
            0.0
        };
        let mut props = std::collections::BTreeMap::new();
        props.insert("city".to_string(), serde_json::json!(city));
        props.insert("platform".to_string(), serde_json::json!(platform));
        props.insert("plan".to_string(), serde_json::json!(plan));
        props.insert("kyc_status".to_string(), serde_json::json!(kyc_status));
        props.insert("utm_source".to_string(), serde_json::json!(utm_src));
        props.insert("utm_campaign".to_string(), serde_json::json!(utm_cmp));
        props.insert("total_revenue".to_string(), serde_json::json!(total_rev));

        seeded_profiles.push(pulse_storage::StoredUserProfile {
            project_id: project_id.to_string(),
            distinct_id: active_did,
            canonical_uid: canon_uid,
            props,
            total_revenue: total_rev,
            updated_at_ms: now_ms - (pick_cohort_day(u) * 86_400_000),
        });
    }
    pstore.upsert_batch(&seeded_profiles).await?;

    ctrl.meta()
        .upsert_alert_rule(&AlertRule {
            id: "alr_nsdl_timeout_spike".to_string(),
            project_id: project_id.to_string(),
            name: "NSDL KYC 504 Timeout Spike".to_string(),
            event_name: "NSDL_Gateway_504_Timeout".to_string(),
            operator: "gt".to_string(),
            threshold: 25.0,
            webhook_url: None,
            last_triggered_ms: None,
            created_at_ms: now_ms,
        })
        .await?;

    seed_starter_dashboard(ctrl, project_id, now_ms).await?;

    ctrl.refresh_project_virtual_catalog(project_id)
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

    let summary = SeedSummary {
        unique_events,
        duplicate_retries,
        users_seeded: total_users,
        replay_sessions: replay_session_count,
        elapsed_ms: t0.elapsed().as_millis(),
    };

    info!(
        unique_events = summary.unique_events,
        duplicate_retries = summary.duplicate_retries,
        users = summary.users_seeded,
        replay_sessions = summary.replay_sessions,
        elapsed_ms = summary.elapsed_ms,
        "Pulse Analytics synthetic Indian Fintech & Quick-Commerce dataset seeded"
    );

    Ok(summary)
}

/// The dashboard a new demo user lands on: the onboarding funnel, daily active users and
/// day-N retention, each a saved report that opens in its own page.
async fn seed_starter_dashboard(
    ctrl: &Arc<ControlPlaneService>,
    project_id: &str,
    now_ms: i64,
) -> anyhow::Result<()> {
    let no_filter = serde_json::json!({"predicates": [], "cohort": null});
    let funnel_steps: Vec<serde_json::Value> = [
        "App_Open",
        "KYC_PAN_Submitted",
        "Video_KYC_Completed",
        "UPI_Autopay_Mandate_Approved",
        "First_Disbursal_Success",
    ]
    .iter()
    .map(|e| serde_json::json!({"event_name": e, "filters": [], "first_time_ever": false}))
    .collect();
    let reports = [
        (
            "rep_demo_onboarding_funnel",
            "Onboarding funnel",
            "funnel",
            serde_json::json!({
                "steps": funnel_steps,
                "order_mode": "loose_order",
                "conversion_window_ms": 7 * 86_400_000i64,
                "exclusions": [],
                "hold_constant_props": [],
                "breakdown_by": null,
                "global_filter": no_filter,
            }),
        ),
        (
            "rep_demo_daily_active",
            "Daily active users",
            "insights",
            serde_json::json!({
                "interval": "day",
                "series": [{"label": "A", "event_name": "App_Open", "metric": {"type": "uniques"}, "filters": []}],
                "breakdown_by": [],
                "compare_previous_period": false,
                "formula": null,
                "global_filter": no_filter,
            }),
        ),
        (
            "rep_demo_retention",
            "Day-N retention",
            "retention",
            serde_json::json!({
                "start_event": "App_Open",
                "return_event": "$any_event",
                "mode": "n_day",
                "unit": "day",
                "num_periods": 7,
                "global_filter": no_filter,
            }),
        ),
    ];
    let mut cards = vec![DashboardCard {
        id: "card_demo_note".to_string(),
        kind: "text".to_string(),
        report_id: None,
        text: Some(
            "Demo data: a lending app's onboarding over the last 14 days. Open any card to explore it, or build your own report from the sidebar.".to_string(),
        ),
        width: 2,
    }];
    for (id, name, kind, query) in reports {
        ctrl.meta()
            .upsert_saved_report(&SavedReport {
                id: id.to_string(),
                project_id: project_id.to_string(),
                name: name.to_string(),
                report_kind: kind.to_string(),
                query_json: query,
                created_at_ms: now_ms,
                updated_at_ms: now_ms,
            })
            .await?;
        cards.push(DashboardCard {
            id: format!("card_{id}"),
            kind: "report".to_string(),
            report_id: Some(id.to_string()),
            text: None,
            width: if kind == "funnel" { 2 } else { 1 },
        });
    }
    ctrl.meta()
        .upsert_dashboard(&Dashboard {
            id: "dash_demo_overview".to_string(),
            project_id: project_id.to_string(),
            name: "Overview".to_string(),
            description: Some("Starter dashboard for the demo project".to_string()),
            cards,
            created_by: None,
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
        })
        .await?;
    Ok(())
}
