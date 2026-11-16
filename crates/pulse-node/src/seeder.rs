use std::sync::Arc;
use std::time::Instant;

use chrono::Utc;
use pulse_control::{
    CohortRecord, ControlPlaneService, Experiment, ExperimentMetricSpec, FeatureFlag,
    LexiconProperty, VariantAllocation,
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
        rollout_pct: 100.0,
        targeting_cohort_id: None,
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
            conversion_window_ms: 7 * 86_400_000,
        },
        guardrail_metrics: vec![ExperimentMetricSpec {
            name: "NSDL Gateway 504 Timeout Rate".to_string(),
            event_name: "NSDL_Gateway_504_Timeout".to_string(),
            metric_type: "conversion".to_string(),
            numeric_prop: None,
            conversion_window_ms: 7 * 86_400_000,
        }],
        created_at_ms: now_ms,
    };
    ctrl.meta().upsert_experiment(&exp).await?;

    // 2. Seed Multi-Frame Frustrated Wireframe Replay Sessions (with PAN: ██████████ + 4x Rage Taps!)
    let total_users = 8_000usize;
    let replay_session_count = 32usize;
    let mut replay_offsets_by_sess: std::collections::HashMap<String, u32> =
        std::collections::HashMap::new();

    for r_idx in 0..replay_session_count {
        let u = 200 + r_idx * 17;
        let sess_id = format!("sess_kyc_timeout_{u:05}");
        let base_t = start_day_ms + ((u % 14) as i64) * 86_400_000 + 38_000_000;

        let chunk1 = ReplayChunk {
            project_id: project_id.to_string(),
            session_id: sess_id.clone(),
            chunk_start_ts_ms: base_t,
            ops: vec![
                WireframeOp::FullSnapshot {
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
                            masked_label: "PAN: ██████████".to_string(),
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
                            masked_label: "Mobile: ██████████".to_string(),
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
                WireframeOp::NodeUpsert(WireNode {
                    node_id: 7,
                    parent_id: 1,
                    kind: NodeKind::ErrorBanner,
                    x: 24,
                    y: 434,
                    w: 342,
                    h: 56,
                    bg_rgba: 0xDC26_26FF,
                    masked_label: "NSDL_Gateway_504_Timeout: Upstream KYC verification timed out"
                        .to_string(),
                }),
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

    // 3. Generate 100,000+ Analytical Events + Retroactive Aliases across 8,000 Users over 14 Days
    let cities = [
        "Bengaluru",
        "Mumbai",
        "Delhi_NCR",
        "Hyderabad",
        "Pune",
        "Chennai",
    ];
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
        let cohort_day = (u % 14) as i64;
        let user_day0_ts =
            start_day_ms + cohort_day * 86_400_000 + ((u * 97) % 50_000) as i64 * 600;

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

        let city = cities[u % cities.len()];
        let net = networks[(u * 3) % networks.len()];
        let platform_primary = if u % 4 == 0 { "iOS" } else { "Android" };
        let payment_method = if u % 5 == 0 {
            "card"
        } else if u % 9 == 0 {
            "netbanking"
        } else {
            "upi"
        };
        let merchant = merchants[(u * 7) % merchants.len()];
        let base_amount = 750 + ((u * 137) % 18_500) as i64;

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
        for (dt, ename) in [
            (0i64, "App_Open"),
            (8_000, "Browse_Credit_Card"),
            (19_000, "View_Loan_Calculator"),
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
                replay_offset_kb: 0,
            });
        }

        // --- Mid-Session Login (`OTP_Verified` + Retroactive `alias(anon_id -> phone_id)`) ---
        // 85% of users verify OTP and log in (`u < 6800`)
        if u < 6_800 {
            let b_anon = pulse_storage::route_bucket(&primary_seed, num_buckets);
            let b_user = pulse_storage::route_bucket(&secondary_seed, num_buckets);
            id_graph.alias(&anon_id, &phone_id, b_anon, b_user);

            seq += 1;
            all_events.push(WireEvent {
                event_uuid: WireEvent::deterministic_uuid(
                    &phone_id,
                    &sess_onboard,
                    "OTP_Verified",
                    user_day0_ts + 32_000,
                    seq,
                ),
                routing_seed: primary_seed.clone(),
                distinct_id: phone_id.clone(),
                session_id: sess_onboard.clone(),
                event_name: "OTP_Verified".to_string(),
                client_event_ts_ms: user_day0_ts + 32_000,
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
                    user_day0_ts + 55_000,
                    seq,
                ),
                routing_seed: primary_seed.clone(),
                distinct_id: active_did.clone(),
                session_id: sess_onboard.clone(),
                event_name: "KYC_PAN_Submitted".to_string(),
                client_event_ts_ms: user_day0_ts + 55_000,
                client_sent_ts_ms: now_ms,
                props: base_props(
                    base_amount,
                    Some(("pan_masked", PropValue::Str("PAN: ██████████".into()))),
                ),
                replay_offset_kb: replay_kb,
            });

            // Frustration cohort (~380 users, including all 32 recorded replay sessions) hits `NSDL_Gateway_504_Timeout`!
            let hit_nsdl_timeout =
                replay_offsets_by_sess.contains_key(&sess_onboard) || (u % 16 == 5);
            if hit_nsdl_timeout {
                seq += 1;
                all_events.push(WireEvent {
                    event_uuid: WireEvent::deterministic_uuid(
                        active_did,
                        &sess_onboard,
                        "NSDL_Gateway_504_Timeout",
                        user_day0_ts + 63_000,
                        seq,
                    ),
                    routing_seed: primary_seed.clone(),
                    distinct_id: active_did.clone(),
                    session_id: sess_onboard.clone(),
                    event_name: "NSDL_Gateway_504_Timeout".to_string(),
                    client_event_ts_ms: user_day0_ts + 63_000,
                    client_sent_ts_ms: now_ms,
                    props: base_props(base_amount, Some(("status_code", PropValue::Int(504)))),
                    replay_offset_kb: replay_kb,
                });
            }

            // --- Funnel Step 2: `Video_KYC_Completed` & A/B Experiment Exposure (`5,000` users: 2,500 control vs 2,500 one_tap_sheet) ---
            if u < 5_000 && !replay_offsets_by_sess.contains_key(&sess_onboard) {
                seq += 1;
                all_events.push(WireEvent {
                    event_uuid: WireEvent::deterministic_uuid(
                        active_did,
                        &sess_onboard,
                        "Video_KYC_Completed",
                        user_day0_ts + 120_000,
                        seq,
                    ),
                    routing_seed: secondary_seed.clone(),
                    distinct_id: active_did.clone(),
                    session_id: sess_onboard.clone(),
                    event_name: "Video_KYC_Completed".to_string(),
                    client_event_ts_ms: user_day0_ts + 120_000,
                    client_sent_ts_ms: now_ms,
                    props: base_props(base_amount, None),
                    replay_offset_kb: 0,
                });

                // Deterministic 50/50 split on `exp_one_tap_upi_mandate`:
                //   even `u` -> `control` (48.0% conversion)
                //   odd  `u` -> `one_tap_sheet` (55.0% conversion -> +14.58% relative lift!)
                let is_treatment = u % 2 == 1;
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
                        user_day0_ts + 145_000,
                        seq,
                    ),
                    routing_seed: secondary_seed.clone(),
                    distinct_id: active_did.clone(),
                    session_id: sess_onboard.clone(),
                    event_name: "$experiment_started".to_string(),
                    client_event_ts_ms: user_day0_ts + 145_000,
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

                let slot_pct = (u / 2) % 200; // 0..199
                let converts_mandate = if is_treatment {
                    slot_pct < 110 // 110 / 200 = 55.0%
                } else {
                    slot_pct < 96 // 96 / 200 = 48.0% -> +14.58% lift!
                };

                if converts_mandate {
                    seq += 1;
                    all_events.push(WireEvent {
                        event_uuid: WireEvent::deterministic_uuid(
                            active_did,
                            &sess_onboard,
                            "UPI_Autopay_Mandate_Approved",
                            user_day0_ts + 185_000,
                            seq,
                        ),
                        routing_seed: secondary_seed.clone(),
                        distinct_id: active_did.clone(),
                        session_id: sess_onboard.clone(),
                        event_name: "UPI_Autopay_Mandate_Approved".to_string(),
                        client_event_ts_ms: user_day0_ts + 185_000,
                        client_sent_ts_ms: now_ms,
                        props: base_props(
                            base_amount,
                            Some(("$exp_one_tap_upi_mandate", PropValue::Str(variant.into()))),
                        ),
                        replay_offset_kb: 0,
                    });

                    // --- Funnel Step 4: `First_Disbursal_Success` (82% of Mandate-Approved users) ---
                    if slot_pct % 100 < 82 {
                        seq += 1;
                        all_events.push(WireEvent {
                            event_uuid: WireEvent::deterministic_uuid(
                                active_did,
                                &sess_onboard,
                                "First_Disbursal_Success",
                                user_day0_ts + 240_000,
                                seq,
                            ),
                            routing_seed: secondary_seed.clone(),
                            distinct_id: active_did.clone(),
                            session_id: sess_onboard.clone(),
                            event_name: "First_Disbursal_Success".to_string(),
                            client_event_ts_ms: user_day0_ts + 240_000,
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
            // Realistic decaying retention curve (~58% D1 -> ~44% D3 -> ~32% D7 -> ~25% D13) yielding 108,000+ unique events
            let retention_threshold = match d_off {
                1 => 58,
                2 => 50,
                3 => 44,
                4..=6 => 37,
                7..=10 => 32,
                _ => 25,
            };
            if ((u * 31 + (d_off as usize) * 17) % 100) < retention_threshold {
                let ret_ts = user_day0_ts + d_off * 86_400_000 + ((u % 600) as i64) * 1_000;
                let ret_sess = format!("sess_ret_{u:05}_d{d_off}");

                for (dt, ename) in [
                    (0i64, "App_Open"),
                    (12_000, "Quick_Grocery_Browse"),
                    (28_000, "Quick_Commerce_Order_Paid"),
                ] {
                    if ename == "Quick_Commerce_Order_Paid" && (u + d_off as usize) % 3 == 0 {
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
                            320 + ((u * 19 + d_off as usize * 43) % 1800) as i64,
                            None,
                        ),
                        replay_offset_kb: 0,
                    });
                }
            }
        }
    }

    id_graph.flush().await?;

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
    ctrl.qengine().storage().compact_all_projects().await?;

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
