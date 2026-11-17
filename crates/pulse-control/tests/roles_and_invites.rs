use std::sync::Arc;
use std::time::Duration;

use axum::body::{to_bytes, Body};
use axum::http::{header, Request, StatusCode};
use pulse_control::{build_control_router, AuthConfig, ControlPlaneService, MetaStore};
use pulse_exec::{CatalogRegistry, QueryEngine};
use pulse_identity::IdentityRouter;
use pulse_storage::{build_from_url, StorageConfig, StorageEngine};
use serde_json::{json, Value};
use tower::ServiceExt;

mod common;

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
    ctrl.set_outbox_path(tmp.join("outbox"));
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

use axum::Router;

const PW: &str = "correct horse battery";

/// The first account: owner of the default org (which holds `proj_alpha`).
async fn owner_session(app: &Router) -> String {
    let (s, v, _) = send(
        app,
        "POST",
        "/v1/auth/signup",
        None,
        Some(json!({"email": "owner@acme.test", "password": PW, "name": "Olive", "setup_code": ROOT})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert_eq!(v["user"]["role"], "owner");
    v["token"].as_str().unwrap().to_string()
}

/// Invites `email` as `role` and accepts it; returns the new session token.
async fn invite_and_accept(app: &Router, by: &str, email: &str, role: &str) -> String {
    let (s, v, _) = send(
        app,
        "POST",
        "/v1/org/invites",
        Some(by),
        Some(json!({"email": email, "role": role})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{v}");
    let url = v["invite_url"].as_str().unwrap();
    let token = url.rsplit('/').next().unwrap().to_string();
    let (s, info, _) = send(app, "GET", &format!("/v1/auth/invites/{token}"), None, None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(info["email"], email);
    assert_eq!(info["role"], role);
    let (s, v, _) = send(
        app,
        "POST",
        "/v1/auth/invites/accept",
        None,
        Some(json!({"token": token, "password": PW, "name": "New"})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert_eq!(v["user"]["role"], role);
    // An invite works once.
    let (s, _, _) = send(
        app,
        "POST",
        "/v1/auth/invites/accept",
        None,
        Some(json!({"token": token, "password": PW})),
    )
    .await;
    assert_eq!(s, StatusCode::GONE);
    v["token"].as_str().unwrap().to_string()
}

#[tokio::test]
async fn viewer_reads_but_cannot_write_and_members_can() {
    let tmp = tempfile::tempdir().unwrap();
    let (app, _ctrl, _meta) = setup_app(tmp.path()).await;
    let owner = owner_session(&app).await;
    let viewer = invite_and_accept(&app, &owner, "vic@acme.test", "viewer").await;
    let member = invite_and_accept(&app, &owner, "mia@acme.test", "member").await;

    let reports = "/v1/projects/proj_alpha/saved-reports";
    let body =
        json!({"name": "Signups", "report_kind": "insights", "query_json": {"event": "signup"}});
    let (s, v, _) = send(&app, "POST", reports, Some(&member), Some(body.clone())).await;
    assert_eq!(s, StatusCode::OK, "{v}");
    let rep = v["id"].as_str().unwrap().to_string();

    let (s, list, _) = send(&app, "GET", reports, Some(&viewer), None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(list.as_array().unwrap().len(), 1);
    let (s, _, _) = send(&app, "GET", "/v1/org/members", Some(&viewer), None).await;
    assert_eq!(s, StatusCode::OK);

    let (s, v, _) = send(&app, "POST", reports, Some(&viewer), Some(body)).await;
    assert_eq!(s, StatusCode::FORBIDDEN, "{v}");
    assert!(v["message"].as_str().unwrap().contains("member"));
    let (s, _, _) = send(
        &app,
        "PUT",
        &format!("{reports}/{rep}"),
        Some(&viewer),
        Some(json!({"name": "x"})),
    )
    .await;
    assert_eq!(s, StatusCode::FORBIDDEN);
    let (s, _, _) = send(
        &app,
        "DELETE",
        &format!("{reports}/{rep}"),
        Some(&viewer),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::FORBIDDEN);

    // Keys and invites are admin-only.
    for tok in [&viewer, &member] {
        let (s, _, _) = send(
            &app,
            "GET",
            "/v1/projects/proj_alpha/api-keys",
            Some(tok),
            None,
        )
        .await;
        assert_eq!(s, StatusCode::FORBIDDEN);
        let (s, _, _) = send(
            &app,
            "POST",
            "/v1/org/invites",
            Some(tok),
            Some(json!({"email": "z@acme.test", "role": "viewer"})),
        )
        .await;
        assert_eq!(s, StatusCode::FORBIDDEN);
    }
    let (s, _, _) = send(
        &app,
        "GET",
        "/v1/projects/proj_alpha/api-keys",
        Some(&owner),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    // The root token keeps working as before.
    let (s, _, _) = send(
        &app,
        "GET",
        "/v1/projects/proj_alpha/api-keys",
        Some(ROOT),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK);
}

#[tokio::test]
async fn admins_manage_lower_roles_only_and_the_last_owner_stays() {
    let tmp = tempfile::tempdir().unwrap();
    let (app, _ctrl, _meta) = setup_app(tmp.path()).await;
    let owner = owner_session(&app).await;
    let admin = invite_and_accept(&app, &owner, "ann@acme.test", "admin").await;
    let _ = invite_and_accept(&app, &admin, "vic@acme.test", "viewer").await;

    let (_, members, _) = send(&app, "GET", "/v1/org/members", Some(&owner), None).await;
    let id_of = |email: &str| {
        members
            .as_array()
            .unwrap()
            .iter()
            .find(|m| m["email"] == email)
            .unwrap()["id"]
            .as_str()
            .unwrap()
            .to_string()
    };
    let (owner_id, admin_id, viewer_id) = (
        id_of("owner@acme.test"),
        id_of("ann@acme.test"),
        id_of("vic@acme.test"),
    );

    // Admins can't invite or grant admin/owner, or touch the owner.
    let (s, _, _) = send(
        &app,
        "POST",
        "/v1/org/invites",
        Some(&admin),
        Some(json!({"email": "x@acme.test", "role": "admin"})),
    )
    .await;
    assert_eq!(s, StatusCode::FORBIDDEN);
    let (s, _, _) = send(
        &app,
        "PATCH",
        &format!("/v1/org/members/{viewer_id}"),
        Some(&admin),
        Some(json!({"role": "admin"})),
    )
    .await;
    assert_eq!(s, StatusCode::FORBIDDEN);
    let (s, _, _) = send(
        &app,
        "PATCH",
        &format!("/v1/org/members/{owner_id}"),
        Some(&admin),
        Some(json!({"role": "viewer"})),
    )
    .await;
    assert_eq!(s, StatusCode::FORBIDDEN);
    let (s, v, _) = send(
        &app,
        "PATCH",
        &format!("/v1/org/members/{viewer_id}"),
        Some(&admin),
        Some(json!({"role": "member"})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{v}");
    // Only the owner renames the org.
    let (s, _, _) = send(
        &app,
        "PATCH",
        "/v1/org",
        Some(&admin),
        Some(json!({"name": "Acme"})),
    )
    .await;
    assert_eq!(s, StatusCode::FORBIDDEN);
    let (s, v, _) = send(
        &app,
        "PATCH",
        "/v1/org",
        Some(&owner),
        Some(json!({"name": "Acme"})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{v}");

    // Nobody changes their own role; the last owner can't be demoted.
    let (s, _, _) = send(
        &app,
        "PATCH",
        &format!("/v1/org/members/{owner_id}"),
        Some(&owner),
        Some(json!({"role": "admin"})),
    )
    .await;
    assert_eq!(s, StatusCode::FORBIDDEN);
    let (s, _, _) = send(
        &app,
        "PATCH",
        &format!("/v1/org/members/{admin_id}"),
        Some(&owner),
        Some(json!({"role": "owner"})),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    let (s, _, _) = send(
        &app,
        "PATCH",
        &format!("/v1/org/members/{owner_id}"),
        Some(&admin),
        Some(json!({"role": "member"})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "a second owner can demote the first");

    // A role change applies to the next request of an existing session.
    let (s, _, _) = send(
        &app,
        "GET",
        "/v1/projects/proj_alpha/api-keys",
        Some(&owner),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::FORBIDDEN);

    // Removing a member ends their sessions.
    let viewer2 = invite_and_accept(&app, &admin, "v2@acme.test", "viewer").await;
    let (_, members, _) = send(&app, "GET", "/v1/org/members", Some(&admin), None).await;
    let v2 = members
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["email"] == "v2@acme.test")
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let (s, _, _) = send(
        &app,
        "DELETE",
        &format!("/v1/org/members/{v2}"),
        Some(&admin),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    let (s, _, _) = send(&app, "GET", "/v1/auth/me", Some(&viewer2), None).await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn password_change_reset_and_sessions() {
    let tmp = tempfile::tempdir().unwrap();
    let (app, _ctrl, _meta) = setup_app(tmp.path()).await;
    let a = owner_session(&app).await;
    let login = |pw: &'static str| {
        let app = app.clone();
        async move {
            send(
                &app,
                "POST",
                "/v1/auth/login",
                None,
                Some(json!({"email": "owner@acme.test", "password": pw})),
            )
            .await
        }
    };
    let (_, v, _) = login(PW).await;
    let b = v["token"].as_str().unwrap().to_string();

    let (s, list, _) = send(&app, "GET", "/v1/auth/sessions", Some(&a), None).await;
    assert_eq!(s, StatusCode::OK);
    let list = list.as_array().unwrap();
    assert_eq!(list.len(), 2);
    assert_eq!(list.iter().filter(|x| x["current"] == true).count(), 1);
    assert!(list
        .iter()
        .all(|x| x["id"].as_str().unwrap().len() == 16 && x.get("token_hash").is_none()));

    let (s, _, _) = send(
        &app,
        "POST",
        "/v1/auth/password",
        Some(&a),
        Some(
            json!({"current_password": "wrong password!", "new_password": "a brand new password"}),
        ),
    )
    .await;
    assert_eq!(s, StatusCode::FORBIDDEN);
    let (s, v, _) = send(
        &app,
        "POST",
        "/v1/auth/password",
        Some(&a),
        Some(json!({"current_password": PW, "new_password": "a brand new password"})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert_eq!(v["other_sessions_signed_out"], 1);
    let (s, _, _) = send(&app, "GET", "/v1/auth/me", Some(&b), None).await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);
    let (s, _, _) = send(&app, "GET", "/v1/auth/me", Some(&a), None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(login(PW).await.0, StatusCode::UNAUTHORIZED);
    assert_eq!(login("a brand new password").await.0, StatusCode::OK);

    // Signing out one session by id.
    let (_, list, _) = send(&app, "GET", "/v1/auth/sessions", Some(&a), None).await;
    let other = list
        .as_array()
        .unwrap()
        .iter()
        .find(|x| x["current"] == false)
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let (s, _, _) = send(
        &app,
        "DELETE",
        &format!("/v1/auth/sessions/{other}"),
        Some(&a),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    let (_, list, _) = send(&app, "GET", "/v1/auth/sessions", Some(&a), None).await;
    assert_eq!(list.as_array().unwrap().len(), 1);

    // Reset: request always answers 204; an admin-issued link sets a new password once and ends every session.
    let (s, _, _) = send(
        &app,
        "POST",
        "/v1/auth/password-reset/request",
        None,
        Some(json!({"email": "nobody@acme.test"})),
    )
    .await;
    assert_eq!(s, StatusCode::NO_CONTENT);
    let viewer = invite_and_accept(&app, &a, "vic@acme.test", "viewer").await;
    let (_, members, _) = send(&app, "GET", "/v1/org/members", Some(&a), None).await;
    let vid = members
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["email"] == "vic@acme.test")
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let (s, v, _) = send(
        &app,
        "POST",
        &format!("/v1/org/members/{vid}/password-reset"),
        Some(&a),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{v}");
    let token = v["reset_url"]
        .as_str()
        .unwrap()
        .rsplit('/')
        .next()
        .unwrap()
        .to_string();
    let body = json!({"token": token, "new_password": "viewer's new password"});
    let (s, _, _) = send(
        &app,
        "POST",
        "/v1/auth/password-reset",
        None,
        Some(body.clone()),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    let (s, _, _) = send(&app, "POST", "/v1/auth/password-reset", None, Some(body)).await;
    assert_eq!(s, StatusCode::GONE);
    let (s, _, _) = send(&app, "GET", "/v1/auth/me", Some(&viewer), None).await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);
}

/// Signs up with an org name (or by domain) and follows the verification link from the log.
async fn signup_verified(
    app: &Router,
    logs: &common::Outbox,
    email: &str,
    org: Option<&str>,
) -> String {
    let mut body = json!({"email": email, "password": PW});
    if let Some(o) = org {
        body["org_name"] = json!(o);
    }
    let (s, v, _) = send(app, "POST", "/v1/auth/signup", None, Some(body)).await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert_eq!(v["verification_required"], true);
    let token = logs.last_token("verify");
    let (s, v, _) = send(
        app,
        "POST",
        "/v1/auth/verify-email",
        None,
        Some(json!({"token": token})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{v}");
    v["token"].as_str().unwrap().to_string()
}

fn member_id(members: &Value, email: &str) -> String {
    members
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["email"] == email)
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string()
}

#[tokio::test]
async fn existing_user_accepts_invite_and_moves_org() {
    let (log, _guard) = common::capture_logs();
    let tmp = tempfile::tempdir().unwrap();
    let logs = common::Outbox::at(tmp.path());
    let (app, ctrl, _meta) = setup_app(tmp.path()).await;
    let mut cfg = auth_cfg(&[], "http://127.0.0.1:1/unused");
    cfg.allow_new_orgs = true;
    ctrl.set_auth_config(cfg);
    let owner = owner_session(&app).await;
    let eve = signup_verified(&app, &logs, "eve@other.test", Some("Acme")).await;
    // Eve's Acme gains a second member, so Eve is the last owner of an org with others in it.
    let (_, v, _) = send(
        &app,
        "POST",
        "/v1/org/invites",
        Some(&eve),
        Some(json!({"email": "zed@other.test", "role": "member"})),
    )
    .await;
    let zed_link = v["invite_url"]
        .as_str()
        .unwrap()
        .rsplit('/')
        .next()
        .unwrap()
        .to_string();
    let (s, _, _) = send(
        &app,
        "POST",
        "/v1/auth/invites/accept",
        None,
        Some(json!({"token": zed_link, "password": PW})),
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    // The default org's owner invites Eve, who already has an account.
    let (s, v, _) = send(
        &app,
        "POST",
        "/v1/org/invites",
        Some(&owner),
        Some(json!({"email": "eve@other.test", "role": "member"})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{v}");
    let token = v["invite_url"]
        .as_str()
        .unwrap()
        .rsplit('/')
        .next()
        .unwrap()
        .to_string();
    let (_, info, _) = send(
        &app,
        "GET",
        &format!("/v1/auth/invites/{token}"),
        None,
        None,
    )
    .await;
    assert_eq!(info["has_account"], true);
    // Not by making a second account...
    let (s, _, _) = send(
        &app,
        "POST",
        "/v1/auth/invites/accept",
        None,
        Some(json!({"token": token, "password": PW})),
    )
    .await;
    assert_eq!(s, StatusCode::CONFLICT);
    // ...and not with someone else's session.
    let (s, _, _) = send(
        &app,
        "POST",
        "/v1/auth/join-org",
        Some(&owner),
        Some(json!({"token": token})),
    )
    .await;
    assert_eq!(s, StatusCode::FORBIDDEN);
    // Eve can't leave Acme ownerless while Zed is still in it.
    let (s, v, _) = send(
        &app,
        "POST",
        "/v1/auth/join-org",
        Some(&eve),
        Some(json!({"token": token})),
    )
    .await;
    assert_eq!(s, StatusCode::CONFLICT, "{v}");
    let (_, acme, _) = send(&app, "GET", "/v1/org/members", Some(&eve), None).await;
    let zed = member_id(&acme, "zed@other.test");
    let (s, _, _) = send(
        &app,
        "PATCH",
        &format!("/v1/org/members/{zed}"),
        Some(&eve),
        Some(json!({"role": "owner"})),
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    let (s, v, _) = send(
        &app,
        "POST",
        "/v1/auth/join-org",
        Some(&eve),
        Some(json!({"token": token})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert_eq!(v["org_id"], "org_default");
    assert_eq!(v["role"], "member");
    // The same session now works in the new org and no longer in the old one.
    let (s, _, _) = send(
        &app,
        "GET",
        "/v1/projects/proj_alpha/saved-reports",
        Some(&eve),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    let (_, projects, _) = send(&app, "GET", "/v1/projects", Some(&eve), None).await;
    assert!(projects
        .as_array()
        .unwrap()
        .iter()
        .all(|p| p["name"] != "Acme"));
    let (s, _, _) = send(
        &app,
        "POST",
        "/v1/auth/join-org",
        Some(&eve),
        Some(json!({"token": token})),
    )
    .await;
    assert_eq!(s, StatusCode::GONE);
    // Links go to the owner-only outbox; none may reach the log.
    assert!(
        !log.text().contains("/#/"),
        "an account link leaked into the log"
    );
}

#[tokio::test]
async fn unverified_squatter_cannot_block_an_invite() {
    let (log, _guard) = common::capture_logs();
    let tmp = tempfile::tempdir().unwrap();
    let logs = common::Outbox::at(tmp.path());
    let (app, ctrl, _meta) = setup_app(tmp.path()).await;
    ctrl.set_auth_config(auth_cfg(&["acme.test"], "http://127.0.0.1:1/unused"));
    let owner = owner_session(&app).await;
    let (_, v, _) = send(
        &app,
        "POST",
        "/v1/org/invites",
        Some(&owner),
        Some(json!({"email": "vic@acme.test", "role": "viewer"})),
    )
    .await;
    let token = v["invite_url"]
        .as_str()
        .unwrap()
        .rsplit('/')
        .next()
        .unwrap()
        .to_string();

    // A stranger signs up with Vic's address first but can't prove it.
    let (s, v, _) = send(
        &app,
        "POST",
        "/v1/auth/signup",
        None,
        Some(json!({"email": "vic@acme.test", "password": "stranger password"})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{v}");
    let squat = logs.last_token("verify");
    let (_, info, _) = send(
        &app,
        "GET",
        &format!("/v1/auth/invites/{token}"),
        None,
        None,
    )
    .await;
    assert_eq!(info["has_account"], false);

    // Vic accepts anyway; the unverified account is replaced and its link dies.
    let (s, v, _) = send(
        &app,
        "POST",
        "/v1/auth/invites/accept",
        None,
        Some(json!({"token": token, "password": PW})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert_eq!(v["user"]["role"], "viewer");
    let (s, _, _) = send(
        &app,
        "POST",
        "/v1/auth/verify-email",
        None,
        Some(json!({"token": squat})),
    )
    .await;
    assert_eq!(s, StatusCode::GONE);
    let (s, _, _) = send(
        &app,
        "POST",
        "/v1/auth/login",
        None,
        Some(json!({"email": "vic@acme.test", "password": "stranger password"})),
    )
    .await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);
    let (_, members, _) = send(&app, "GET", "/v1/org/members", Some(&owner), None).await;
    assert_eq!(members.as_array().unwrap().len(), 2);
    // Links go to the owner-only outbox; none may reach the log.
    assert!(
        !log.text().contains("/#/"),
        "an account link leaked into the log"
    );
}

#[tokio::test]
async fn no_orphan_org_when_the_email_is_taken() {
    let (log, _guard) = common::capture_logs();
    let tmp = tempfile::tempdir().unwrap();
    let logs = common::Outbox::at(tmp.path());
    let (app, ctrl, meta) = setup_app(tmp.path()).await;
    let mut cfg = auth_cfg(&[], "http://127.0.0.1:1/unused");
    cfg.allow_new_orgs = true;
    ctrl.set_auth_config(cfg);
    let _owner = owner_session(&app).await;
    let orgs = meta.count_orgs().await.unwrap();

    // Sign-up with an org name for an address that already has an account: refused, nothing created.
    let (s, _, _) = send(
        &app,
        "POST",
        "/v1/auth/signup",
        None,
        Some(json!({"email": "owner@acme.test", "password": PW, "org_name": "Dup"})),
    )
    .await;
    assert_eq!(s, StatusCode::CONFLICT);
    assert_eq!(meta.count_orgs().await.unwrap(), orgs);

    // Two pending sign-ups for one address: only the last link creates an org, exactly once.
    for name in ["One", "Two"] {
        let (s, _, _) = send(
            &app,
            "POST",
            "/v1/auth/signup",
            None,
            Some(json!({"email": "new@b.test", "password": PW, "org_name": name})),
        )
        .await;
        assert_eq!(s, StatusCode::OK);
    }
    assert_eq!(meta.count_orgs().await.unwrap(), orgs);
    let token = logs.last_token("verify");
    let (s, v, _) = send(
        &app,
        "POST",
        "/v1/auth/verify-email",
        None,
        Some(json!({"token": token})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert_eq!(meta.count_orgs().await.unwrap(), orgs + 1);
    // Links go to the owner-only outbox; none may reach the log.
    assert!(
        !log.text().contains("/#/"),
        "an account link leaked into the log"
    );
}

#[tokio::test]
async fn reset_links_go_to_the_outbox_once_and_die_on_password_change() {
    let (log, _guard) = common::capture_logs();
    let tmp = tempfile::tempdir().unwrap();
    let logs = common::Outbox::at(tmp.path());
    let (app, _ctrl, _meta) = setup_app(tmp.path()).await;
    let a = owner_session(&app).await;
    let request = || {
        send(
            &app,
            "POST",
            "/v1/auth/password-reset/request",
            None,
            Some(json!({"email": "owner@acme.test"})),
        )
    };

    let (s, _, _) = request().await;
    assert_eq!(s, StatusCode::NO_CONTENT);
    let token = logs.last_token("reset");
    assert!(
        !log.text().contains(&token),
        "reset token leaked into the log"
    );
    assert!(
        !log.text().contains("/#/"),
        "no link of any kind belongs in the log"
    );

    // A second request inside the window makes no new link.
    let before = logs.text();
    assert_eq!(request().await.0, StatusCode::NO_CONTENT);
    assert_eq!(logs.text(), before);

    // Changing the password kills the outstanding link.
    let (s, _, _) = send(
        &app,
        "POST",
        "/v1/auth/password",
        Some(&a),
        Some(json!({"current_password": PW, "new_password": "a brand new password"})),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    let (s, _, _) = send(
        &app,
        "POST",
        "/v1/auth/password-reset",
        None,
        Some(json!({"token": token, "new_password": "attacker password!"})),
    )
    .await;
    assert_eq!(s, StatusCode::GONE);

    // Invites hand the link to the inviter only.
    let (_, v, _) = send(
        &app,
        "POST",
        "/v1/org/invites",
        Some(&a),
        Some(json!({"email": "x@acme.test", "role": "viewer"})),
    )
    .await;
    let invite = v["invite_url"]
        .as_str()
        .unwrap()
        .rsplit('/')
        .next()
        .unwrap()
        .to_string();
    assert!(!log.text().contains(&invite) && !logs.text().contains(&invite));
}

#[tokio::test]
async fn viewers_cannot_bulk_export_and_last_owner_cannot_strand_an_org() {
    let tmp = tempfile::tempdir().unwrap();
    let (app, ctrl, _meta) = setup_app(tmp.path()).await;
    let mut cfg = auth_cfg(&[], "http://127.0.0.1:1/unused");
    cfg.allow_new_orgs = true;
    ctrl.set_auth_config(cfg);
    let owner = owner_session(&app).await;
    let viewer = invite_and_accept(&app, &owner, "vee@acme.test", "viewer").await;
    let (s, _, _) = send(
        &app,
        "GET",
        "/v1/projects/proj_alpha/cohorts/c1/export",
        Some(&viewer),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::FORBIDDEN);

    // The default org's only owner is invited elsewhere and may not leave it ownerless.
    let logs = common::Outbox::at(tmp.path());
    let solo = signup_verified(&app, &logs, "solo@b.test", Some("Solo")).await;
    let (_, v, _) = send(
        &app,
        "POST",
        "/v1/org/invites",
        Some(&solo),
        Some(json!({"email": "owner@acme.test", "role": "member"})),
    )
    .await;
    let token = v["invite_url"]
        .as_str()
        .unwrap()
        .rsplit('/')
        .next()
        .unwrap()
        .to_string();
    let (s, v, _) = send(
        &app,
        "POST",
        "/v1/auth/join-org",
        Some(&owner),
        Some(json!({"token": token})),
    )
    .await;
    assert_eq!(s, StatusCode::CONFLICT, "{v}");
}
