use std::sync::Arc;
use std::time::Duration;

use axum::body::{to_bytes, Body};
use axum::http::{header, Request, StatusCode};
use axum::routing::post;
use axum::{Json, Router};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use pulse_control::{build_control_router, sha256_hex, AuthConfig, ControlPlaneService, MetaStore};
use pulse_exec::{CatalogRegistry, QueryEngine};
use pulse_identity::IdentityRouter;
use pulse_storage::{build_from_url, StorageConfig, StorageEngine};
use serde_json::{json, Value};
use tower::ServiceExt;

const ROOT: &str = "sk_root_test_admin_secret_0123456789";
const PUBLIC_URL: &str = "http://pulse.test";

fn auth_cfg(domains: &[&str], token_url: &str) -> AuthConfig {
    AuthConfig {
        google_client_id: Some("client-123.apps.googleusercontent.com".to_string()),
        google_client_secret: Some("secret-xyz".to_string()),
        public_url: PUBLIC_URL.to_string(),
        google_token_url: token_url.to_string(),
        allowed_email_domains: domains.iter().map(|d| d.to_string()).collect(),
        allow_new_orgs: false,
    }
}

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
    ctrl.set_auth_config(auth_cfg(&[], "http://127.0.0.1:1/unused"));
    (build_control_router(ctrl.clone()), ctrl, meta)
}

async fn send(
    app: &Router,
    method: &str,
    uri: &str,
    bearer: Option<&str>,
    body: Option<Value>,
) -> (StatusCode, Value, Option<String>) {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(tok) = bearer {
        builder = builder.header("authorization", format!("Bearer {tok}"));
    }
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
    let location = resp
        .headers()
        .get(header::LOCATION)
        .map(|v| v.to_str().unwrap().to_string());
    let bytes = to_bytes(resp.into_body(), 1024 * 1024).await.unwrap();
    let val = serde_json::from_slice::<Value>(&bytes).unwrap_or(Value::Null);
    (status, val, location)
}

async fn signup(app: &Router, email: &str) -> (StatusCode, Value) {
    let (s, v, _) = send(
        app,
        "POST",
        "/v1/auth/signup",
        None,
        // The setup code only matters for the first account; later sign-ups ignore it.
        Some(json!({"email": email, "password": "correct horse battery", "name": "Ada", "setup_code": ROOT})),
    )
    .await;
    (s, v)
}

fn query_param(url: &str, key: &str) -> Option<String> {
    reqwest::Url::parse(url)
        .unwrap()
        .query_pairs()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.into_owned())
}

#[tokio::test]
async fn signup_policy_first_user_then_domain_allowlist() {
    let tmp = tempfile::tempdir().unwrap();
    let (app, ctrl, _meta) = setup_app(tmp.path()).await;
    // Closed node: no new organisations, only allowed domains after the first account.
    ctrl.set_auth_config(auth_cfg(&[], "http://127.0.0.1:1/unused"));

    let (_, cfg, _) = send(&app, "GET", "/v1/auth/config", None, None).await;
    assert_eq!(
        cfg,
        json!({"password": true, "google": true, "signup_open": true, "setup_code_required": true, "org_name_required": false})
    );

    // A fresh node must not hand its first (owner) account to whoever reaches it first.
    let (s, _, _) = send(
        &app,
        "POST",
        "/v1/auth/signup",
        None,
        Some(json!({"email": "mallory@evil.test", "password": "correct horse battery"})),
    )
    .await;
    assert_eq!(s, StatusCode::FORBIDDEN);
    let (s, _, _) = send(
        &app,
        "POST",
        "/v1/auth/signup",
        None,
        Some(json!({"email": "mallory@evil.test", "password": "correct horse battery", "setup_code": "wrong"})),
    )
    .await;
    assert_eq!(s, StatusCode::FORBIDDEN);

    let (s, v) = signup(&app, "  Ada@Example.com ").await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert!(v["token"].as_str().unwrap().starts_with("ps_"));
    assert_eq!(v["user"]["email"], "ada@example.com");
    assert_eq!(v["user"]["has_password"], true);
    assert_eq!(v["user"]["has_google"], false);

    let (_, cfg, _) = send(&app, "GET", "/v1/auth/config", None, None).await;
    assert_eq!(cfg["signup_open"], false);

    let (s, v) = signup(&app, "bob@example.com").await;
    assert_eq!(s, StatusCode::FORBIDDEN);
    assert_eq!(
        v["message"],
        "Sign-ups are closed for this workspace. Ask an admin to add your email domain."
    );

    ctrl.set_auth_config(auth_cfg(&["example.com"], "http://127.0.0.1:1/unused"));
    let (s, _) = signup(&app, "bob@EXAMPLE.com").await;
    assert_eq!(s, StatusCode::OK);
    let (s, _) = signup(&app, "bob@example.com").await;
    assert_eq!(s, StatusCode::CONFLICT);
    let (s, _) = signup(&app, "eve@other.com").await;
    assert_eq!(s, StatusCode::FORBIDDEN);

    // Validation
    let (s, _, _) = send(
        &app,
        "POST",
        "/v1/auth/signup",
        None,
        Some(json!({"email": "carol@example.com", "password": "short"})),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    let (s, _, _) = send(
        &app,
        "POST",
        "/v1/auth/signup",
        None,
        Some(json!({"email": "not-an-email", "password": "long enough password"})),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn login_good_bad_and_throttled() {
    let tmp = tempfile::tempdir().unwrap();
    let (app, _ctrl, _meta) = setup_app(tmp.path()).await;
    signup(&app, "ada@example.com").await;

    let login = |pw: &'static str| {
        let app = app.clone();
        async move {
            send(
                &app,
                "POST",
                "/v1/auth/login",
                None,
                Some(json!({"email": "ADA@example.com", "password": pw})),
            )
            .await
        }
    };

    let (s, v, _) = login("correct horse battery").await;
    assert_eq!(s, StatusCode::OK);
    assert!(v["token"].as_str().unwrap().starts_with("ps_"));
    assert_eq!(v["user"]["email"], "ada@example.com");

    let (s, v, _) = send(
        &app,
        "POST",
        "/v1/auth/login",
        None,
        Some(json!({"email": "nobody@example.com", "password": "whatever123"})),
    )
    .await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);
    assert_eq!(v["message"], "Email or password is incorrect");

    for _ in 0..10 {
        let (s, v, _) = login("wrong password!").await;
        assert_eq!(s, StatusCode::UNAUTHORIZED);
        assert_eq!(v["message"], "Email or password is incorrect");
    }
    let (s, v, _) = login("correct horse battery").await;
    assert_eq!(s, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        v["message"],
        "Too many attempts. Try again in a few minutes."
    );
}

#[tokio::test]
async fn session_token_authorizes_admin_routes_until_logout_or_expiry() {
    let tmp = tempfile::tempdir().unwrap();
    let (app, _ctrl, meta) = setup_app(tmp.path()).await;
    let (_, v) = signup(&app, "ada@example.com").await;
    let token = v["token"].as_str().unwrap().to_string();
    let user_id = v["user"]["id"].as_str().unwrap().to_string();

    let (s, projects, _) = send(&app, "GET", "/v1/projects", Some(&token), None).await;
    assert_eq!(s, StatusCode::OK);
    assert!(projects
        .as_array()
        .unwrap()
        .iter()
        .any(|p| p["id"] == "proj_alpha"));

    // Users can create projects; they land in their own organisation.
    let (s, _, _) = send(
        &app,
        "POST",
        "/v1/projects",
        Some(&token),
        Some(json!({"name": "Beta"})),
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    let (s, me, _) = send(&app, "GET", "/v1/auth/me", Some(&token), None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(me["id"], user_id.as_str());
    assert_eq!(me["email"], "ada@example.com");
    assert_eq!(me["name"], "Ada");

    // Root token still works and is not a user.
    let (s, _, _) = send(&app, "GET", "/v1/projects", Some(ROOT), None).await;
    assert_eq!(s, StatusCode::OK);
    let (s, _, _) = send(&app, "GET", "/v1/auth/me", Some(ROOT), None).await;
    assert_eq!(s, StatusCode::NOT_FOUND);

    let (s, _, _) = send(&app, "POST", "/v1/auth/logout", Some(&token), None).await;
    assert_eq!(s, StatusCode::OK);
    let (s, _, _) = send(&app, "GET", "/v1/projects", Some(&token), None).await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);

    // Expired session row is rejected.
    let expired = "ps_expired_session_token_for_test";
    let now = chrono::Utc::now().timestamp_millis();
    meta.insert_session(&sha256_hex(expired), &user_id, now - 10_000, now - 1)
        .await
        .unwrap();
    let (s, _, _) = send(&app, "GET", "/v1/projects", Some(expired), None).await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn google_start_redirect_and_bad_state() {
    let tmp = tempfile::tempdir().unwrap();
    let (app, _ctrl, _meta) = setup_app(tmp.path()).await;

    let (s, _, loc) = send(&app, "GET", "/v1/auth/google/start", None, None).await;
    assert_eq!(s, StatusCode::FOUND);
    let loc = loc.unwrap();
    assert!(loc.starts_with("https://accounts.google.com/o/oauth2/v2/auth?"));
    assert_eq!(
        query_param(&loc, "client_id").as_deref(),
        Some("client-123.apps.googleusercontent.com")
    );
    assert_eq!(
        query_param(&loc, "redirect_uri").as_deref(),
        Some("http://pulse.test/v1/auth/google/callback")
    );
    assert_eq!(query_param(&loc, "state").unwrap().len(), 64);
    assert!(query_param(&loc, "nonce").is_some());
    assert_eq!(
        query_param(&loc, "scope").as_deref(),
        Some("openid email profile")
    );

    let (s, _, loc) = send(
        &app,
        "GET",
        "/v1/auth/google/callback?code=abc&state=bogus",
        None,
        None,
    )
    .await;
    assert_eq!(s, StatusCode::FOUND);
    assert_eq!(
        loc.unwrap(),
        "http://pulse.test/#/signin?error=google_state"
    );

    let (_, _, loc) = send(
        &app,
        "GET",
        "/v1/auth/google/callback?error=access_denied",
        None,
        None,
    )
    .await;
    assert_eq!(
        loc.unwrap(),
        "http://pulse.test/#/signin?error=google_denied"
    );
}

#[tokio::test]
async fn google_full_callback_against_stub_token_endpoint() {
    let tmp = tempfile::tempdir().unwrap();
    let (app, ctrl, _meta) = setup_app(tmp.path()).await;

    let (_, _, loc) = send(&app, "GET", "/v1/auth/google/start", None, None).await;
    let loc = loc.unwrap();
    let state = query_param(&loc, "state").unwrap();
    let nonce = query_param(&loc, "nonce").unwrap();

    let claims = json!({
        "iss": "https://accounts.google.com",
        "aud": "client-123.apps.googleusercontent.com",
        "sub": "google-sub-42",
        "email": "Grace@Example.com",
        "email_verified": true,
        "name": "Grace",
        "nonce": nonce,
        "exp": chrono::Utc::now().timestamp() + 600,
    });
    let id_token = format!(
        "{}.{}.unsigned",
        URL_SAFE_NO_PAD.encode(br#"{"alg":"none"}"#),
        URL_SAFE_NO_PAD.encode(claims.to_string())
    );
    let stub = Router::new().route(
        "/token",
        post(move |body: String| {
            let id_token = id_token.clone();
            async move {
                assert!(body.contains("grant_type=authorization_code"));
                assert!(body.contains("code=auth-code-1"));
                Json(json!({"access_token": "at", "id_token": id_token}))
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, stub).await.unwrap() });
    // Google sign-in can't carry a setup code, so on a fresh node it needs an allowed domain.
    ctrl.set_auth_config(auth_cfg(&["example.com"], &format!("http://{addr}/token")));

    let (s, _, loc) = send(
        &app,
        "GET",
        &format!("/v1/auth/google/callback?code=auth-code-1&state={state}"),
        None,
        None,
    )
    .await;
    assert_eq!(s, StatusCode::FOUND);
    let loc = loc.unwrap();
    let code = loc
        .strip_prefix("http://pulse.test/#/auth/callback?code=")
        .unwrap_or_else(|| panic!("unexpected redirect {loc}"))
        .to_string();

    // State is single use.
    let (_, _, loc2) = send(
        &app,
        "GET",
        &format!("/v1/auth/google/callback?code=auth-code-1&state={state}"),
        None,
        None,
    )
    .await;
    assert_eq!(
        loc2.unwrap(),
        "http://pulse.test/#/signin?error=google_state"
    );

    let (s, v, _) = send(
        &app,
        "POST",
        "/v1/auth/exchange",
        None,
        Some(json!({"code": code})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{v}");
    let token = v["token"].as_str().unwrap();
    assert_eq!(v["user"]["email"], "grace@example.com");
    assert_eq!(v["user"]["has_google"], true);
    assert_eq!(v["user"]["has_password"], false);

    // Code is single use.
    let (s, _, _) = send(
        &app,
        "POST",
        "/v1/auth/exchange",
        None,
        Some(json!({"code": code})),
    )
    .await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);

    let (s, _, _) = send(&app, "GET", "/v1/projects", Some(token), None).await;
    assert_eq!(s, StatusCode::OK);
}

#[tokio::test]
async fn each_signup_gets_its_own_org_and_cannot_see_others() {
    let tmp = tempfile::tempdir().unwrap();
    let (app, ctrl, _meta) = setup_app(tmp.path()).await;
    let mut cfg = auth_cfg(&[], "http://127.0.0.1:1/unused");
    cfg.allow_new_orgs = true;
    ctrl.set_auth_config(cfg);

    // The owner (setup code) gets the default org and its existing project.
    let (_, owner) = signup(&app, "ada@example.com").await;
    let owner_tok = owner["token"].as_str().unwrap().to_string();

    let (_, cfg, _) = send(&app, "GET", "/v1/auth/config", None, None).await;
    assert_eq!(cfg["org_name_required"], true);

    // A stranger without an org name is told to give one; with one, they get a separate org.
    let body = |org: Option<&str>| {
        let mut b = json!({"email": "eve@other.com", "password": "correct horse battery"});
        if let Some(o) = org {
            b["org_name"] = json!(o);
        }
        Some(b)
    };
    let (s, _, _) = send(&app, "POST", "/v1/auth/signup", None, body(None)).await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    let (s, eve, _) = send(&app, "POST", "/v1/auth/signup", None, body(Some("Acme"))).await;
    assert_eq!(s, StatusCode::OK, "{eve}");
    let eve_tok = eve["token"].as_str().unwrap().to_string();
    assert_ne!(eve["user"]["org_id"], "org_default");

    let (_, eve_projects, _) = send(&app, "GET", "/v1/projects", Some(&eve_tok), None).await;
    let eve_projects = eve_projects.as_array().unwrap().clone();
    assert_eq!(eve_projects.len(), 1);
    assert_eq!(eve_projects[0]["name"], "Acme");
    let acme = eve_projects[0]["id"].as_str().unwrap().to_string();

    // Status without a project id lands Eve in her own project, never the default one.
    let (s, st, _) = send(&app, "GET", "/v1/status", Some(&eve_tok), None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(st["active_project_id"], acme.as_str());

    // Eve can't read the owner's project, and the owner can't read Eve's.
    for uri in [
        "/v1/projects/proj_alpha/api-keys",
        "/v1/status?project_id=proj_alpha",
    ] {
        let (s, _, _) = send(&app, "GET", uri, Some(&eve_tok), None).await;
        assert_eq!(s, StatusCode::NOT_FOUND, "{uri}");
    }
    let (s, _, _) = send(
        &app,
        "GET",
        &format!("/v1/projects/{acme}/api-keys"),
        Some(&owner_tok),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::NOT_FOUND);
    let (_, owner_projects, _) = send(&app, "GET", "/v1/projects", Some(&owner_tok), None).await;
    assert!(owner_projects
        .as_array()
        .unwrap()
        .iter()
        .all(|p| p["id"] != acme.as_str()));

    // The root admin token still sees every org's projects.
    let (_, all, _) = send(&app, "GET", "/v1/projects", Some(ROOT), None).await;
    assert!(all
        .as_array()
        .unwrap()
        .iter()
        .any(|p| p["id"] == acme.as_str()));
}
