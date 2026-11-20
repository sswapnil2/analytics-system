use std::sync::Arc;
use std::time::Duration;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use axum::Router;
use pulse_control::{build_control_router, ControlPlaneService, MetaStore};
use pulse_exec::{CatalogRegistry, QueryEngine};
use pulse_identity::IdentityRouter;
use pulse_storage::{build_from_url, StorageConfig, StorageEngine};
use serde_json::{json, Value};
use tower::ServiceExt;

const ROOT: &str = "sk_root_test_admin_secret_0123456789";

async fn setup_app(tmp: &std::path::Path) -> (Router, Arc<ControlPlaneService>, MetaStore) {
    std::env::set_var("PULSE_ADMIN_TOKEN", ROOT);
    let store = build_from_url("memory://").unwrap();
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
    let qengine = QueryEngine::new(storage, 16 * 1024 * 1024, None, 0, CatalogRegistry::new())
        .await
        .unwrap();
    let meta = MetaStore::connect_auto(tmp).await.unwrap();
    let ctrl = ControlPlaneService::new(meta.clone(), qengine, "proj_alpha", "pk_alpha_key")
        .await
        .unwrap();
    (build_control_router(ctrl.clone()), ctrl, meta)
}

async fn send(
    app: &Router,
    method: &str,
    uri: &str,
    bearer: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("authorization", format!("Bearer {bearer}"));
    let req_body = match body {
        Some(b) => {
            builder = builder.header("content-type", "application/json");
            Body::from(serde_json::to_vec(&b).unwrap())
        }
        None => Body::empty(),
    };
    let resp = app
        .clone()
        .oneshot(builder.body(req_body).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let bytes = to_bytes(resp.into_body(), 1024 * 1024).await.unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

async fn create_cohort(app: &Router, project: &str, name: &str, def: Value) -> String {
    let (s, v) = send(
        app,
        "POST",
        &format!("/v1/projects/{project}/cohorts"),
        ROOT,
        Some(json!({"name": name, "definition": def})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{v}");
    v["id"].as_str().unwrap().to_string()
}

fn buyers() -> Value {
    json!({"kind": "performed_event", "event_name": "Purchase", "min_count": 1})
}

#[tokio::test]
async fn update_renames_redefines_and_rejects_cycles() {
    let tmp = tempfile::tempdir().unwrap();
    let (app, _ctrl, _meta) = setup_app(tmp.path()).await;
    let id = create_cohort(&app, "proj_alpha", "Buyers", buyers()).await;
    let url = format!("/v1/projects/proj_alpha/cohorts/{id}");

    let (s, v) = send(
        &app,
        "PUT",
        &url,
        ROOT,
        Some(json!({
            "name": "  Big buyers ", "description": "3+ purchases",
            "definition": {"kind": "performed_event", "event_name": "Purchase", "min_count": 3}
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert_eq!(v["id"], id.as_str());
    assert_eq!(v["name"], "Big buyers");
    assert_eq!(v["definition"]["min_count"], 3);
    let (_, list) = send(&app, "GET", "/v1/projects/proj_alpha/cohorts", ROOT, None).await;
    assert_eq!(list.as_array().unwrap().len(), 1);
    assert_eq!(list[0]["description"], "3+ purchases");
    assert_eq!(list[0]["created_at_ms"], v["created_at_ms"]);

    let (s, _) = send(
        &app,
        "PUT",
        &url,
        ROOT,
        Some(json!({"name": " ", "definition": buyers()})),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST);

    // Self-cycle, directly and through another cohort.
    let (s, v) = send(
        &app,
        "PUT",
        &url,
        ROOT,
        Some(json!({
            "name": "Loop", "definition": {"kind": "cohort_ref", "cohort_id": id}
        })),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{v}");
    let other = create_cohort(
        &app,
        "proj_alpha",
        "Wraps",
        json!({"kind": "cohort_ref", "cohort_id": id}),
    )
    .await;
    let (s, _) = send(&app, "PUT", &url, ROOT, Some(json!({
        "name": "Loop", "definition": {"kind": "or", "clauses": [buyers(), {"kind": "cohort_ref", "cohort_id": other}]}
    }))).await;
    assert_eq!(s, StatusCode::BAD_REQUEST);

    let (s, _) = send(
        &app,
        "PUT",
        "/v1/projects/proj_alpha/cohorts/coh_missing",
        ROOT,
        Some(json!({"name": "X", "definition": buyers()})),
    )
    .await;
    assert_eq!(s, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn delete_refused_while_in_use_then_succeeds() {
    let tmp = tempfile::tempdir().unwrap();
    let (app, _ctrl, _meta) = setup_app(tmp.path()).await;
    let id = create_cohort(&app, "proj_alpha", "Buyers", buyers()).await;
    let url = format!("/v1/projects/proj_alpha/cohorts/{id}");

    // Used by another cohort.
    let wrap = create_cohort(
        &app,
        "proj_alpha",
        "Loyal",
        json!({"kind": "and", "clauses": [
            buyers(), {"kind": "cohort_ref", "cohort_id": id}
        ]}),
    )
    .await;
    let (s, v) = send(&app, "DELETE", &url, ROOT, None).await;
    assert_eq!(s, StatusCode::CONFLICT);
    assert!(
        v["error"]
            .as_str()
            .unwrap_or(&v.to_string())
            .contains("cohort 'Loyal'"),
        "{v}"
    );
    let (s, _) = send(
        &app,
        "DELETE",
        &format!("/v1/projects/proj_alpha/cohorts/{wrap}"),
        ROOT,
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    // Used by a flag.
    let (s, flag) = send(
        &app,
        "POST",
        "/v1/projects/proj_alpha/flags",
        ROOT,
        Some(json!({"key": "beta_ui", "targeting_cohort_id": id})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{flag}");
    let (s, v) = send(&app, "DELETE", &url, ROOT, None).await;
    assert_eq!(s, StatusCode::CONFLICT);
    assert!(v.to_string().contains("flag 'beta_ui'"), "{v}");
    let (s, _) = send(
        &app,
        "PATCH",
        &format!(
            "/v1/projects/proj_alpha/flags/{}",
            flag["id"].as_str().unwrap()
        ),
        ROOT,
        Some(json!({"targeting_cohort_id": ""})),
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    // Used by a saved report.
    let (s, rep) = send(&app, "POST", "/v1/projects/proj_alpha/saved-reports", ROOT, Some(json!({
        "name": "Buyer signups", "report_kind": "insights",
        "query_json": {"event": "signup", "filter": {"cohort": {"kind": "cohort_ref", "cohort_id": id}}}
    }))).await;
    assert_eq!(s, StatusCode::OK, "{rep}");
    let (s, v) = send(&app, "DELETE", &url, ROOT, None).await;
    assert_eq!(s, StatusCode::CONFLICT);
    assert!(v.to_string().contains("report 'Buyer signups'"), "{v}");
    let (s, _) = send(
        &app,
        "DELETE",
        &format!(
            "/v1/projects/proj_alpha/saved-reports/{}",
            rep["id"].as_str().unwrap()
        ),
        ROOT,
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    // Wrong project → 404, and the cohort survives.
    let (_, beta) = send(
        &app,
        "POST",
        "/v1/projects",
        ROOT,
        Some(json!({"name": "Beta"})),
    )
    .await;
    let beta_id = beta["project"]["id"].as_str().unwrap();
    let (s, _) = send(
        &app,
        "DELETE",
        &format!("/v1/projects/{beta_id}/cohorts/{id}"),
        ROOT,
        None,
    )
    .await;
    assert_eq!(s, StatusCode::NOT_FOUND);
    let (s, _) = send(
        &app,
        "PUT",
        &format!("/v1/projects/{beta_id}/cohorts/{id}"),
        ROOT,
        Some(json!({"name": "Stolen", "definition": buyers()})),
    )
    .await;
    assert_eq!(s, StatusCode::NOT_FOUND);

    let (s, v) = send(&app, "DELETE", &url, ROOT, None).await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert_eq!(v, json!({"deleted": id}));
    let (_, list) = send(&app, "GET", "/v1/projects/proj_alpha/cohorts", ROOT, None).await;
    assert_eq!(list, json!([]));
    let (s, _) = send(&app, "DELETE", &url, ROOT, None).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
}
