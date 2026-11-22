use std::sync::Arc;
use std::time::Duration;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use axum::Router;
use pulse_control::{build_control_router, ControlPlaneService, MetaStore};
use pulse_exec::{CatalogRegistry, QueryEngine};
use pulse_identity::IdentityRouter;
use pulse_ingest::{build_ingest_router, IngestService};
use pulse_storage::{build_from_url, StorageConfig, StorageEngine};
use serde_json::{json, Value};
use tower::ServiceExt;

async fn setup_app(tmp: &std::path::Path) -> (Router, Arc<ControlPlaneService>, MetaStore) {
    std::env::set_var("PULSE_ADMIN_TOKEN", "sk_root_test_admin_secret_0123456789");
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
    let catalogs = CatalogRegistry::new();
    let qengine = QueryEngine::new(storage.clone(), 16 * 1024 * 1024, None, 0, catalogs)
        .await
        .unwrap();
    let meta = MetaStore::connect_auto(tmp).await.unwrap();
    let ctrl =
        ControlPlaneService::new(meta.clone(), qengine, "proj_alpha", "pk_alpha_initial_key")
            .await
            .unwrap();
    let ingest = IngestService::new(storage, ctrl.clone());
    ctrl.attach_ingest(ingest.clone()).await;

    let router = Router::new()
        .merge(build_ingest_router(ingest))
        .merge(build_control_router(ctrl.clone()));
    (router, ctrl, meta)
}

async fn send_json(
    app: &Router,
    method: &str,
    uri: &str,
    bearer: Option<&str>,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(tok) = bearer {
        builder = builder.header("authorization", format!("Bearer {tok}"));
    }
    let req_body = if let Some(b) = body {
        builder = builder.header("content-type", "application/json");
        Body::from(serde_json::to_vec(&b).unwrap())
    } else {
        Body::empty()
    };
    let resp = app
        .clone()
        .oneshot(builder.body(req_body).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let bytes = to_bytes(resp.into_body(), 4 * 1024 * 1024).await.unwrap();
    let val = serde_json::from_slice::<Value>(&bytes).unwrap_or(Value::Null);
    (status, val)
}

#[tokio::test]
async fn admin_routes_require_bearer_token_and_reject_public_ingest_keys() {
    let tmp = tempfile::tempdir().unwrap();
    let (app, _ctrl, _meta) = setup_app(tmp.path()).await;

    // 1. Unauthenticated requests to control/query/admin/MCP routes MUST return 401
    for (method, path) in [
        ("GET", "/v1/status"),
        ("GET", "/v1/projects"),
        ("GET", "/v1/projects/proj_alpha/api-keys"),
        ("POST", "/v1/admin/flush"),
        ("POST", "/v1/mcp"),
    ] {
        let body = if method == "POST" {
            Some(json!({"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}))
        } else {
            None
        };
        let (status, _) = send_json(&app, method, path, None, body).await;
        assert_eq!(
            status,
            StatusCode::UNAUTHORIZED,
            "route {method} {path} must reject unauthenticated caller with 401"
        );
    }

    // 2. Public `pk_` ingest key MUST NOT grant access to admin routes
    let (status_pk, _) = send_json(
        &app,
        "GET",
        "/v1/projects/proj_alpha/api-keys",
        Some("pk_alpha_initial_key"),
        None,
    )
    .await;
    assert!(
        status_pk == StatusCode::UNAUTHORIZED || status_pk == StatusCode::FORBIDDEN,
        "public pk_ key must not access admin api-keys route, got {status_pk}"
    );

    // 3. Valid admin token succeeds on admin routes
    let (status_ok, _) = send_json(
        &app,
        "GET",
        "/v1/status",
        Some("sk_root_test_admin_secret_0123456789"),
        None,
    )
    .await;
    assert_eq!(status_ok, StatusCode::OK);

    // 4. Public `/v1/ingest` and `/v1/flags/:key` still work with public `pk_` key
    let (flag_status, _) =
        send_json(&app, "GET", "/v1/flags/pk_alpha_initial_key", None, None).await;
    assert_eq!(flag_status, StatusCode::OK);
}

#[tokio::test]
async fn keys_never_exposed_in_status_or_list_and_stored_hashed() {
    let tmp = tempfile::tempdir().unwrap();
    let (app, _ctrl, _meta) = setup_app(tmp.path()).await;
    let admin = Some("sk_root_test_admin_secret_0123456789");

    // `/v1/status` must NEVER leak `default_api_key` or any `pk_`/`sk_` secret
    let (st, status_json) = send_json(&app, "GET", "/v1/status", admin, None).await;
    assert_eq!(st, StatusCode::OK);
    assert!(
        status_json.get("default_api_key").is_none(),
        "/v1/status must not expose default_api_key: {status_json}"
    );

    // Minting a key returns plaintext once, but listing keys never exposes plaintext
    let (create_st, created) = send_json(
        &app,
        "POST",
        "/v1/projects/proj_alpha/api-keys",
        admin,
        Some(json!({ "label": "Mobile Prod", "role": "ingest_and_flags" })),
    )
    .await;
    assert_eq!(create_st, StatusCode::OK);
    let minted_secret = created["plaintext_key"]
        .as_str()
        .expect("POST /api-keys should return one-time plaintext_key")
        .to_string();
    assert!(minted_secret.starts_with("pk_"));

    let (list_st, listed) =
        send_json(&app, "GET", "/v1/projects/proj_alpha/api-keys", admin, None).await;
    assert_eq!(list_st, StatusCode::OK);
    let listed_raw = serde_json::to_string(&listed).unwrap();
    assert!(
        !listed_raw.contains(&minted_secret),
        "GET /api-keys leaked plaintext secret: {listed_raw}"
    );

    // Inspect raw SQLite file bytes to verify the plaintext secret is not stored at rest
    let db_bytes = std::fs::read(tmp.path().join("pulse_meta.db")).unwrap_or_default();
    let wal_bytes = std::fs::read(tmp.path().join("pulse_meta.db-wal")).unwrap_or_default();
    assert!(
        !db_bytes
            .windows(minted_secret.len())
            .any(|w| w == minted_secret.as_bytes())
            && !wal_bytes
                .windows(minted_secret.len())
                .any(|w| w == minted_secret.as_bytes()),
        "SQLite database stored plaintext API key instead of SHA-256 hash!"
    );
}

#[tokio::test]
async fn cross_tenant_upserts_and_project_hijack_are_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let (app, _ctrl, _meta) = setup_app(tmp.path()).await;
    let admin = Some("sk_root_test_admin_secret_0123456789");

    // Create second tenant project `proj_beta`
    let (st_b, beta_created) = send_json(
        &app,
        "POST",
        "/v1/projects",
        admin,
        Some(json!({ "name": "Beta Corp" })),
    )
    .await;
    assert_eq!(st_b, StatusCode::OK);
    let beta_id = beta_created["project"]["id"].as_str().unwrap().to_string();

    // Attempt to hijack existing `proj_alpha` via `POST /v1/projects {"id": "proj_alpha", "name": "Pwned"}`
    let (hijack_st, _) = send_json(
        &app,
        "POST",
        "/v1/projects",
        admin,
        Some(json!({ "id": "proj_alpha", "name": "Pwned" })),
    )
    .await;
    assert!(
        hijack_st == StatusCode::BAD_REQUEST || hijack_st == StatusCode::CONFLICT,
        "POST /v1/projects with existing or client-supplied id must fail, got {hijack_st}"
    );

    // Create cohort in `proj_alpha`
    let (c_st, alpha_coh) = send_json(
        &app,
        "POST",
        "/v1/projects/proj_alpha/cohorts",
        admin,
        Some(json!({
            "name": "Alpha VIPs",
            "description": "Original Alpha Cohort",
            "definition": { "kind": "performed_event", "event_name": "Purchase", "min_count": 1 }
        })),
    )
    .await;
    assert_eq!(c_st, StatusCode::OK);
    let victim_coh_id = alpha_coh["id"].as_str().unwrap().to_string();

    // Attempt cross-tenant overwrite via `POST /v1/projects/<beta>/cohorts` supplying Alpha's cohort `id`
    let (overwrite_st, _) = send_json(
        &app,
        "POST",
        &format!("/v1/projects/{beta_id}/cohorts"),
        admin,
        Some(json!({
            "id": victim_coh_id,
            "name": "Overwritten By Beta",
            "description": "Malicious",
            "definition": { "kind": "performed_event", "event_name": "Hack", "min_count": 1 }
        })),
    )
    .await;
    assert!(
        overwrite_st == StatusCode::BAD_REQUEST || overwrite_st == StatusCode::CONFLICT,
        "client-supplied cross-tenant cohort id must be rejected, got {overwrite_st}"
    );

    // Verify Alpha's cohort is untouched
    let (_, alpha_cohorts) =
        send_json(&app, "GET", "/v1/projects/proj_alpha/cohorts", admin, None).await;
    assert_eq!(alpha_cohorts[0]["name"], "Alpha VIPs");
}

#[tokio::test]
async fn cross_project_idor_reads_and_mcp_project_override_blocked() {
    let tmp = tempfile::tempdir().unwrap();
    let (app, _ctrl, _meta) = setup_app(tmp.path()).await;
    let admin = Some("sk_root_test_admin_secret_0123456789");

    // Create second project `proj_beta` and mint a project-scoped `sk_` admin key for `proj_beta`
    let (_, beta_created) = send_json(
        &app,
        "POST",
        "/v1/projects",
        admin,
        Some(json!({ "name": "Beta Corp" })),
    )
    .await;
    let beta_id = beta_created["project"]["id"].as_str().unwrap().to_string();

    let (_, beta_sk_res) = send_json(
        &app,
        "POST",
        &format!("/v1/projects/{beta_id}/api-keys"),
        admin,
        Some(json!({ "label": "Beta Admin Key", "role": "admin" })),
    )
    .await;
    let beta_sk = beta_sk_res["plaintext_key"]
        .as_str()
        .unwrap_or("sk_missing")
        .to_string();

    // Create experiment in `proj_alpha` and lexicon entry in `proj_alpha`
    let (_, alpha_exp) = send_json(
        &app,
        "POST",
        "/v1/projects/proj_alpha/experiments",
        admin,
        Some(json!({
            "name": "Alpha Secret Pricing Experiment",
            "flag_key": "alpha_pricing",
            "hypothesis": "Top secret",
            "primary_metric": { "name": "Conv", "event_name": "Pay" }
        })),
    )
    .await;
    let alpha_exp_id = alpha_exp["id"].as_str().unwrap().to_string();

    let _ = send_json(
        &app,
        "PUT",
        "/v1/projects/proj_alpha/lexicon/events/Alpha_TopSecret_Event",
        admin,
        Some(json!({ "status": "active", "description": "confidential" })),
    )
    .await;

    // 1. Reading `proj_alpha`'s experiment via `proj_beta`'s URL MUST 404 even for RootAdmin!
    let (idor_st, _) = send_json(
        &app,
        "GET",
        &format!("/v1/projects/{beta_id}/experiments/{alpha_exp_id}/results"),
        admin,
        None,
    )
    .await;
    assert_eq!(
        idor_st,
        StatusCode::NOT_FOUND,
        "experiment belonging to proj_alpha must 404 under proj_beta URL"
    );

    // 2. `proj_beta`'s `sk_` key trying to read `proj_alpha` routes directly MUST return 403 Forbidden
    let (cross_st, _) = send_json(
        &app,
        "GET",
        "/v1/projects/proj_alpha/lexicon",
        Some(&beta_sk),
        None,
    )
    .await;
    assert_eq!(cross_st, StatusCode::FORBIDDEN);

    // 3. `proj_beta`'s `sk_` key calling `/v1/mcp` with `arguments.project_id = "proj_alpha"` MUST NOT read `proj_alpha`!
    let (mcp_st, mcp_body) = send_json(
        &app,
        "POST",
        "/v1/mcp",
        Some(&beta_sk),
        Some(json!({
            "jsonrpc": "2.0",
            "id": 99,
            "method": "tools/call",
            "params": {
                "name": "get_lexicon_schema",
                "arguments": { "project_id": "proj_alpha" }
            }
        })),
    )
    .await;
    assert_eq!(mcp_st, StatusCode::OK);
    let raw_mcp = serde_json::to_string(&mcp_body).unwrap();
    assert!(
        !raw_mcp.contains("Alpha_TopSecret_Event") && mcp_body.get("error").is_some(),
        "MCP allowed cross-project override to leak proj_alpha lexicon: {raw_mcp}"
    );
}

#[tokio::test]
async fn revoked_api_key_stops_working_at_once() {
    let tmp = tempfile::tempdir().unwrap();
    let (app, _ctrl, _meta) = setup_app(tmp.path()).await;
    let root = Some("sk_root_test_admin_secret_0123456789");
    let base = "/v1/projects/proj_alpha/api-keys";

    let (_, created) = send_json(
        &app,
        "POST",
        base,
        root,
        Some(json!({"label": "CI", "role": "admin"})),
    )
    .await;
    let key = created["plaintext_key"].as_str().unwrap().to_string();
    let key_id = created["record"]["id"].as_str().unwrap().to_string();
    let (s, _) = send_json(&app, "GET", base, Some(&key), None).await;
    assert_eq!(s, StatusCode::OK);

    let (s, _) = send_json(&app, "DELETE", &format!("{base}/{key_id}"), root, None).await;
    assert_eq!(s, StatusCode::OK);
    let (s, _) = send_json(&app, "GET", base, Some(&key), None).await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);
    let (_, listed) = send_json(&app, "GET", base, root, None).await;
    let rec = listed
        .as_array()
        .unwrap()
        .iter()
        .find(|k| k["id"] == key_id.as_str())
        .unwrap();
    assert_eq!(rec["revoked"], true);

    let (s, _) = send_json(&app, "DELETE", &format!("{base}/key_missing"), root, None).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
}

/// Ingest and query nodes reload the key cache every few seconds. A lookup that lands
/// mid-reload must still resolve: the cache has no database fallback, so a gap is a 401.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn api_key_stays_valid_while_caches_reload() {
    use pulse_ingest::GovernanceProvider;
    use std::sync::atomic::{AtomicBool, Ordering};

    let tmp = tempfile::tempdir().unwrap();
    let (_app, ctrl, _meta) = setup_app(tmp.path()).await;
    let stop = Arc::new(AtomicBool::new(false));
    let reader = {
        let (ctrl, stop) = (ctrl.clone(), stop.clone());
        tokio::spawn(async move {
            let mut misses = 0u64;
            while !stop.load(Ordering::Relaxed) {
                if ctrl
                    .authenticate_api_key("pk_alpha_initial_key")
                    .await
                    .is_none()
                {
                    misses += 1;
                }
                tokio::task::yield_now().await;
            }
            misses
        })
    };
    for _ in 0..200 {
        ctrl.reload_caches_from_db().await.unwrap();
    }
    stop.store(true, Ordering::Relaxed);
    assert_eq!(reader.await.unwrap(), 0, "key lookups failed during reload");
}
