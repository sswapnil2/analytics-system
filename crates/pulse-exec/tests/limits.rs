use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use object_store::memory::InMemory;
use pulse_exec::{
    eval_math_expr, validate_cohort_acyclic, BehavioralCohortDef, CatalogRegistry, QueryEngine,
    TimeRange,
};
use pulse_identity::IdentityRouter;
use pulse_storage::{StorageConfig, StorageEngine};

#[test]
fn p0_08_formula_unknown_var_and_trailing_junk_are_rejected() {
    let mut vars = HashMap::new();
    vars.insert("A".to_string(), 10.0);
    vars.insert("B".to_string(), 2.0);

    // Unknown variable must error, not silently return 0
    assert!(
        eval_math_expr("A + UNKNOWN_VAR", &vars).is_err(),
        "unknown variable should return Err"
    );

    // Trailing junk must error
    assert!(
        eval_math_expr("A B", &vars).is_err(),
        "trailing identifier should return Err"
    );
    assert!(
        eval_math_expr("(A)))", &vars).is_err(),
        "extra closing parens should return Err"
    );

    // Division by zero must return Ok(None), not 0.0
    assert_eq!(eval_math_expr("A / 0", &vars).unwrap(), None);
    assert_eq!(eval_math_expr("(A / (B - 2)) + 5", &vars).unwrap(), None);
    assert_eq!(eval_math_expr("(A / B) * 3", &vars).unwrap(), Some(15.0));
}

#[test]
fn p0_08_formula_deep_unary_and_nested_parens_do_not_stack_overflow() {
    let mut vars = HashMap::new();
    vars.insert("A".to_string(), 42.0);

    // 2,000,000 unary minuses previously aborted the process via stack overflow
    let huge_unary = format!("{}A", "-".repeat(2_000_000));
    assert!(eval_math_expr(&huge_unary, &vars).is_err());

    // 200 nested parentheses (> depth 64) must error cleanly without recursing unboundedly
    let deep_parens = format!("{}A{}", "(".repeat(200), ")".repeat(200));
    assert!(eval_math_expr(&deep_parens, &vars).is_err());
}

#[tokio::test]
async fn p0_09_cyclic_cohort_references_return_error_instead_of_infinite_recursion() {
    let store = Arc::new(InMemory::new());
    let id_router = Arc::new(IdentityRouter::new(store.clone()));
    let storage = StorageEngine::new(
        StorageConfig {
            num_buckets: 4,
            l0_flush_row_threshold: 10_000,
            l0_flush_max_age: Duration::from_secs(60),
            compaction_min_segments: 4,
        },
        store,
        id_router,
    );
    let catalogs = CatalogRegistry::new();
    let cat = catalogs.for_project("proj_cycle");
    // Mutual cycle: coh_a -> coh_b -> coh_a
    cat.saved_cohorts.insert(
        "coh_a".to_string(),
        BehavioralCohortDef::CohortRef {
            cohort_id: "coh_b".to_string(),
        },
    );
    cat.saved_cohorts.insert(
        "coh_b".to_string(),
        BehavioralCohortDef::CohortRef {
            cohort_id: "coh_a".to_string(),
        },
    );

    let qengine = QueryEngine::new(storage, 16 * 1024 * 1024, None, 0, catalogs)
        .await
        .unwrap();

    let res = qengine
        .preview_cohort_count(
            "proj_cycle",
            &BehavioralCohortDef::CohortRef {
                cohort_id: "coh_a".to_string(),
            },
            Some(TimeRange {
                start_ts_ms: 0,
                end_ts_ms: 10_000,
            }),
        )
        .await;
    assert!(
        res.is_err(),
        "cyclic cohort reference must fail with Err instead of recursing forever"
    );

    // Save-time cycle validator must also reject self-cycles and mutual cycles synchronously
    let cat_ref = qengine.catalogs().for_project("proj_cycle");
    assert!(validate_cohort_acyclic(
        Some("coh_self"),
        &BehavioralCohortDef::CohortRef {
            cohort_id: "coh_self".to_string()
        },
        &cat_ref
    )
    .is_err());
    assert!(validate_cohort_acyclic(
        Some("coh_c"),
        &BehavioralCohortDef::CohortRef {
            cohort_id: "coh_a".to_string()
        },
        &cat_ref
    )
    .is_err());
}
