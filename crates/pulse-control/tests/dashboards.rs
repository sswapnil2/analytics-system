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

async fn create_report(app: &Router, project: &str, name: &str) -> String {
    let (s, v) = send(
        app,
        "POST",
        &format!("/v1/projects/{project}/saved-reports"),
        ROOT,
        Some(json!({"name": name, "report_kind": "insights", "query_json": {"event": "signup"}})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{v}");
    v["id"].as_str().unwrap().to_string()
}

async fn create_project(app: &Router) -> String {
    let (s, v) = send(
        app,
        "POST",
        "/v1/projects",
        ROOT,
        Some(json!({"name": "Beta"})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{v}");
    v["project"]["id"]
        .as_str()
        .or(v["id"].as_str())
        .unwrap()
        .to_string()
}

#[tokio::test]
async fn dashboard_crud_with_report_and_text_cards() {
    let tmp = tempfile::tempdir().unwrap();
    let (app, _ctrl, _meta) = setup_app(tmp.path()).await;
    let rep = create_report(&app, "proj_alpha", "Signups").await;
    let base = "/v1/projects/proj_alpha/dashboards";

    let (s, d) = send(
        &app,
        "POST",
        base,
        ROOT,
        Some(json!({"name": "  Growth  "})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{d}");
    let dash_id = d["id"].as_str().unwrap().to_string();
    assert_eq!(d["name"], "Growth");
    assert_eq!(d["cards"], json!([]));
    assert_eq!(d["created_by"], "admin token");
    assert_eq!(d["description"], Value::Null);

    let (s, d) = send(
        &app,
        "PUT",
        &format!("{base}/{dash_id}"),
        ROOT,
        Some(json!({
            "name": "Growth board",
            "description": "Weekly review",
            "cards": [
                {"kind": "report", "report_id": rep, "width": 2},
                {"id": "card_note", "kind": "text", "text": "## Notes", "width": 1}
            ]
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{d}");
    let cards = d["cards"].as_array().unwrap();
    assert_eq!(cards.len(), 2);
    assert!(cards[0]["id"].as_str().unwrap().starts_with("card_"));
    assert_eq!(cards[0]["report_id"], rep.as_str());
    assert_eq!(cards[0]["width"], 2);
    assert!(cards[0].get("text").is_none());
    assert_eq!(
        cards[1],
        json!({"id": "card_note", "kind": "text", "text": "## Notes", "width": 1})
    );

    let (s, got) = send(&app, "GET", &format!("{base}/{dash_id}"), ROOT, None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(got, d);
    assert_eq!(got["description"], "Weekly review");
    assert_eq!(got["created_by"], "admin token");

    // Newest updated first.
    tokio::time::sleep(Duration::from_millis(2)).await;
    let (_, d2) = send(&app, "POST", base, ROOT, Some(json!({"name": "Second"}))).await;
    let (_, list) = send(&app, "GET", base, ROOT, None).await;
    assert_eq!(list[0]["id"], d2["id"]);
    assert_eq!(list[1]["id"], dash_id.as_str());

    let (s, _) = send(&app, "DELETE", &format!("{base}/{dash_id}"), ROOT, None).await;
    assert_eq!(s, StatusCode::OK);
    let (s, _) = send(&app, "GET", &format!("{base}/{dash_id}"), ROOT, None).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
    let (s, _) = send(&app, "DELETE", &format!("{base}/{dash_id}"), ROOT, None).await;
    assert_eq!(s, StatusCode::NOT_FOUND);

    // Session users are recorded by email.
    let req = Request::builder()
        .method("POST")
        .uri("/v1/auth/signup")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"email": "ada@example.com", "password": "correct horse battery"}).to_string(),
        ))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let bytes = to_bytes(resp.into_body(), 1024 * 1024).await.unwrap();
    let token = serde_json::from_slice::<Value>(&bytes).unwrap()["token"]
        .as_str()
        .unwrap()
        .to_string();
    let (s, d) = send(&app, "POST", base, &token, Some(json!({"name": "Mine"}))).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(d["created_by"], "ada@example.com");

    // Saved report get / update.
    let rbase = format!("/v1/projects/proj_alpha/saved-reports/{rep}");
    let (s, r) = send(
        &app,
        "PUT",
        &rbase,
        ROOT,
        Some(json!({"name": "Signups v2", "query_json": {"event": "signup_v2"}})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{r}");
    assert_eq!(r["report_kind"], "insights");
    let (_, r) = send(&app, "GET", &rbase, ROOT, None).await;
    assert_eq!(r["name"], "Signups v2");
    assert_eq!(r["query_json"], json!({"event": "signup_v2"}));
    assert!(r["updated_at_ms"].as_i64().unwrap() >= r["created_at_ms"].as_i64().unwrap());
}

#[tokio::test]
async fn legacy_dashboard_rows_read_back_as_report_cards() {
    let tmp = tempfile::tempdir().unwrap();
    let (app, _ctrl, meta) = setup_app(tmp.path()).await;
    let MetaStore::Sqlite(pool) = &meta else {
        panic!("tests run on SQLite")
    };
    sqlx::query(
        "INSERT INTO dashboards (id, project_id, name, report_ids_json, created_at_ms)
         VALUES ('dash_legacy', 'proj_alpha', 'Old board', '[\"rep_a\",\"rep_b\"]', 1000)",
    )
    .execute(pool)
    .await
    .unwrap();

    let (s, d) = send(
        &app,
        "GET",
        "/v1/projects/proj_alpha/dashboards/dash_legacy",
        ROOT,
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{d}");
    assert_eq!(
        d["cards"],
        json!([
            {"id": "card_legacy_0", "kind": "report", "report_id": "rep_a", "width": 1},
            {"id": "card_legacy_1", "kind": "report", "report_id": "rep_b", "width": 1}
        ])
    );
    assert_eq!(d["updated_at_ms"], 1000);
    assert_eq!(d["created_by"], Value::Null);
}

#[tokio::test]
async fn deleting_a_report_prunes_its_cards() {
    let tmp = tempfile::tempdir().unwrap();
    let (app, _ctrl, meta) = setup_app(tmp.path()).await;
    let keep = create_report(&app, "proj_alpha", "Keep").await;
    let gone = create_report(&app, "proj_alpha", "Gone").await;

    let (_, d) = send(
        &app,
        "POST",
        "/v1/projects/proj_alpha/dashboards",
        ROOT,
        Some(json!({"name": "Board", "cards": [
            {"kind": "report", "report_id": keep},
            {"kind": "report", "report_id": gone},
            {"kind": "text", "text": "hi"}
        ]})),
    )
    .await;
    let dash_id = d["id"].as_str().unwrap().to_string();

    let (s, v) = send(
        &app,
        "DELETE",
        &format!("/v1/projects/proj_alpha/saved-reports/{gone}"),
        ROOT,
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["dashboards_updated"], 1);

    let (_, d) = send(
        &app,
        "GET",
        &format!("/v1/projects/proj_alpha/dashboards/{dash_id}"),
        ROOT,
        None,
    )
    .await;
    let cards = d["cards"].as_array().unwrap();
    assert_eq!(cards.len(), 2);
    assert_eq!(cards[0]["report_id"], keep.as_str());
    assert_eq!(cards[1]["kind"], "text");

    // Legacy column is kept in sync with the cards.
    let MetaStore::Sqlite(pool) = &meta else {
        panic!("tests run on SQLite")
    };
    let legacy: (String,) = sqlx::query_as("SELECT report_ids_json FROM dashboards WHERE id = ?")
        .bind(&dash_id)
        .fetch_one(pool)
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&legacy.0).unwrap(),
        json!([keep])
    );
}

#[tokio::test]
async fn dashboard_and_report_validation_errors() {
    let tmp = tempfile::tempdir().unwrap();
    let (app, _ctrl, _meta) = setup_app(tmp.path()).await;
    let rep = create_report(&app, "proj_alpha", "R").await;
    let base = "/v1/projects/proj_alpha/dashboards";

    let too_many: Vec<Value> = (0..61)
        .map(|_| json!({"kind": "text", "text": "x"}))
        .collect();
    let cases = vec![
        (
            json!({"name": "   "}),
            "name must be between 1 and 120 characters",
        ),
        (
            json!({"name": "x".repeat(121)}),
            "name must be between 1 and 120 characters",
        ),
        (
            json!({"name": "ok", "description": "d".repeat(1001)}),
            "description must be at most 1000 characters",
        ),
        (
            json!({"name": "ok", "cards": too_many}),
            "a dashboard can hold at most 60 cards",
        ),
        (
            json!({"name": "ok", "cards": [{"kind": "text", "text": "t".repeat(5001)}]}),
            "text cards are limited to 5000 characters",
        ),
        (
            json!({"name": "ok", "cards": [{"kind": "report", "report_id": rep, "width": 3}]}),
            "card width must be 1 (half row) or 2 (full row)",
        ),
        (
            json!({"name": "ok", "cards": [{"kind": "chart"}]}),
            "card kind must be 'report' or 'text'",
        ),
        (
            json!({"name": "ok", "cards": [{"id": "c1", "kind": "report", "report_id": "rep_nope"}]}),
            "report card 'c1' references unknown report 'rep_nope'",
        ),
        (
            json!({"name": "ok", "cards": [
                {"id": "dup", "kind": "text", "text": "a"},
                {"id": "dup", "kind": "text", "text": "b"}
            ]}),
            "duplicate card id 'dup'",
        ),
    ];
    for (body, msg) in cases {
        let (s, v) = send(&app, "POST", base, ROOT, Some(body)).await;
        assert_eq!(s, StatusCode::BAD_REQUEST, "{v}");
        assert_eq!(v["message"], msg);
    }

    let (s, v) = send(
        &app,
        "POST",
        "/v1/projects/proj_alpha/saved-reports",
        ROOT,
        Some(json!({"name": "R", "report_kind": "pie", "query_json": {}})),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    assert_eq!(
        v["message"],
        "report_kind must be one of funnel, insights, retention, flows"
    );
}

#[tokio::test]
async fn cross_project_ids_return_404() {
    let tmp = tempfile::tempdir().unwrap();
    let (app, _ctrl, _meta) = setup_app(tmp.path()).await;
    let beta = create_project(&app).await;
    let rep = create_report(&app, "proj_alpha", "Alpha report").await;
    let (_, d) = send(
        &app,
        "POST",
        "/v1/projects/proj_alpha/dashboards",
        ROOT,
        Some(json!({"name": "Alpha board"})),
    )
    .await;
    let dash = d["id"].as_str().unwrap().to_string();

    let dpath = format!("/v1/projects/{beta}/dashboards/{dash}");
    let rpath = format!("/v1/projects/{beta}/saved-reports/{rep}");
    let put_dash = json!({"name": "x", "cards": []});
    let put_rep = json!({"name": "x", "query_json": {}});
    for (method, path, body) in [
        ("GET", &dpath, None),
        ("PUT", &dpath, Some(put_dash)),
        ("DELETE", &dpath, None),
        ("GET", &rpath, None),
        ("PUT", &rpath, Some(put_rep)),
        ("DELETE", &rpath, None),
    ] {
        let (s, _) = send(&app, method, path, ROOT, body).await;
        assert_eq!(s, StatusCode::NOT_FOUND, "{method} {path}");
    }

    // A beta dashboard cannot reference an alpha report.
    let (s, _) = send(
        &app,
        "POST",
        &format!("/v1/projects/{beta}/dashboards"),
        ROOT,
        Some(json!({"name": "b", "cards": [{"kind": "report", "report_id": rep}]})),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST);

    // Alpha objects are untouched.
    let (s, _) = send(
        &app,
        "GET",
        &format!("/v1/projects/proj_alpha/dashboards/{dash}"),
        ROOT,
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK);
}
