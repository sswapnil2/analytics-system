//! Roles, organisation members, invites, password change/reset and active sessions.
//!
//! Every route behind the bearer middleware is checked against [`required_role`] there, so a
//! handler only adds rules that depend on the target (who may change whom).

use std::sync::Arc;

use axum::extract::{DefaultBodyLimit, Path as AxumPath, State};
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::Response;
use axum::routing::{delete, get, patch, post};
use axum::{Extension, Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};
use uuid::Uuid;

use super::auth::{
    extract_session_cookie, hash_password, normalize_email, now_ms, random_hex, user_json,
    verify_password, SignupOrg, PENDING_ORG,
};
use super::{extract_admin_bearer, ApiRes, AuthPrincipal, ControlPlaneService, CtrlError};
use crate::db::OrgInvite;
use crate::models::{sha256_hex, Role, User};

const INVITE_TTL_MS: i64 = 7 * 24 * 60 * 60 * 1000;
const RESET_TTL_MS: i64 = 60 * 60 * 1000;
/// One self-service reset link per email per window.
const RESET_REQUEST_INTERVAL_MS: i64 = 5 * 60 * 1000;

/// The lowest role allowed to call `method path`, where `path` is the route pattern
/// (`/v1/projects/:id/...`). Reads and queries need viewer, admin paths admin, renaming the org
/// owner; every other write needs member, so a new write route is never open to viewers by accident.
pub(super) fn required_role(method: &Method, path: &str) -> Role {
    const ADMIN: &[&str] = &[
        "/v1/admin/flush",
        "/v1/projects/:id/api-keys",
        "/v1/projects/:id/api-keys/:key_id",
        "/v1/projects/:id/byok",
        "/v1/projects/:id/byok/:category",
        "/v1/projects/:id/byok-verify",
        "/v1/projects/:id/import",
        "/v1/projects/:id/export",
        "/v1/projects/:id/export/warehouse",
        // Bulk download of users and their properties, like /export.
        "/v1/projects/:id/cohorts/:cohort_id/export",
        // Every user who dropped off at a funnel step, as ids.
        "/v1/projects/:id/query/funnel/dropped",
        "/v1/projects/:id/deletions",
        "/v1/org/invites",
        "/v1/org/invites/:invite_id",
        "/v1/org/members/:user_id",
        "/v1/org/members/:user_id/password-reset",
    ];
    // POSTs that only read: queries and the caller's own account.
    const READ_POSTS: &[&str] = &[
        "/v1/query",
        "/v1/projects/:id/query/insights",
        "/v1/projects/:id/query/funnel",
        "/v1/projects/:id/query/retention",
        "/v1/projects/:id/query/flows",
        "/v1/projects/:id/cohorts/preview",
        "/v1/sessions",
        "/v1/projects/:id/sessions",
        "/v1/mcp",
        "/v1/auth/me",
        "/v1/auth/logout",
        "/v1/auth/password",
        "/v1/auth/sessions/:sid",
        "/v1/auth/sessions/revoke-others",
        "/v1/auth/join-org",
    ];
    let m = method;
    if path == "/v1/org" && m == Method::PATCH {
        Role::Owner
    } else if ADMIN.contains(&path)
        || (path == "/v1/projects" && m == Method::POST)
        || (path == "/v1/projects/:id" && m == Method::PATCH)
        || (path == "/v1/projects/:id/users/:distinct_id" && m == Method::DELETE)
    {
        Role::Admin
    } else if m == Method::GET || m == Method::HEAD || READ_POSTS.contains(&path) {
        Role::Viewer
    } else {
        Role::Member
    }
}

/// Routes for a signed-in caller; they sit behind the bearer middleware.
pub(super) fn account_router() -> Router<Arc<ControlPlaneService>> {
    Router::new()
        .route("/v1/auth/password", post(handle_change_password))
        .route("/v1/auth/sessions", get(handle_list_sessions))
        .route("/v1/auth/sessions/:sid", delete(handle_revoke_session))
        .route(
            "/v1/auth/sessions/revoke-others",
            post(handle_revoke_other_sessions),
        )
        .route("/v1/org", get(handle_get_org).patch(handle_rename_org))
        .route("/v1/org/members", get(handle_list_members))
        .route(
            "/v1/org/members/:user_id",
            patch(handle_set_member_role).delete(handle_remove_member),
        )
        .route(
            "/v1/org/members/:user_id/password-reset",
            post(handle_admin_reset_link),
        )
        .route(
            "/v1/org/invites",
            get(handle_list_invites).post(handle_create_invite),
        )
        .route("/v1/org/invites/:invite_id", delete(handle_delete_invite))
        .route("/v1/auth/join-org", post(handle_join_org))
}

/// Routes that work without a session: looking up and accepting an invite, and password reset.
pub(super) fn public_router() -> Router<Arc<ControlPlaneService>> {
    Router::new()
        .route("/v1/auth/invites/:token", get(handle_get_invite))
        .route("/v1/auth/invites/accept", post(handle_accept_invite))
        .route(
            "/v1/auth/password-reset/request",
            post(handle_request_reset),
        )
        .route("/v1/auth/password-reset", post(handle_reset_password))
        .route("/v1/auth/verify-email", post(handle_verify_email))
        .layer(DefaultBodyLimit::max(16 * 1024))
}

fn check_password(pw: &str) -> Result<(), CtrlError> {
    if (10..=256).contains(&pw.chars().count()) {
        Ok(())
    } else {
        Err(CtrlError::bad(
            "password must be between 10 and 256 characters",
        ))
    }
}

async fn hash(pw: String) -> Result<String, CtrlError> {
    tokio::task::spawn_blocking(move || hash_password(&pw))
        .await
        .map_err(CtrlError::internal)?
        .map_err(CtrlError::internal)
}

fn session_user(p: &AuthPrincipal) -> Result<&str, CtrlError> {
    match p {
        AuthPrincipal::User { user_id, .. } => Ok(user_id),
        _ => Err(CtrlError::not_found(
            "this credential is not a user session",
        )),
    }
}

/// The organisation an org-level call acts on. The root token manages the default organisation.
fn org_of(p: &AuthPrincipal) -> Result<String, CtrlError> {
    match p {
        AuthPrincipal::User { org_id, .. } => Ok(org_id.clone()),
        AuthPrincipal::RootAdmin => Ok("org_default".to_string()),
        AuthPrincipal::ProjectAdmin { .. } => Err(CtrlError::forbidden(
            "project keys can't manage the organisation; sign in as a user",
        )),
    }
}

fn parse_role(s: &str) -> Result<Role, CtrlError> {
    Role::parse(s).ok_or_else(|| CtrlError::bad("role must be viewer, member, admin or owner"))
}

fn current_hash(headers: &HeaderMap) -> Option<String> {
    extract_admin_bearer(headers)
        .or_else(|| extract_session_cookie(headers))
        .map(sha256_hex)
}

impl ControlPlaneService {
    async fn get_user(&self, id: &str) -> Result<User, CtrlError> {
        self.meta
            .get_user_by_id(id)
            .await
            .map_err(CtrlError::meta)?
            .ok_or_else(|| CtrlError::not_found("user not found"))
    }

    /// A member of `org_id` the caller is allowed to change: not themselves, and a lower role
    /// unless the caller is an owner.
    async fn manageable_member(
        &self,
        p: &AuthPrincipal,
        org_id: &str,
        user_id: &str,
    ) -> Result<User, CtrlError> {
        let target = self
            .get_user(user_id)
            .await
            .ok()
            .filter(|u| u.org_id == org_id)
            .ok_or_else(|| CtrlError::not_found("member not found"))?;
        if matches!(p, AuthPrincipal::User { user_id: me, .. } if me == user_id) {
            return Err(CtrlError::forbidden(
                "you can't change your own role or remove yourself",
            ));
        }
        if !p.role().outranks(target.role) {
            return Err(CtrlError::forbidden(format!(
                "only an owner can change an account with the {} role",
                target.role.as_str()
            )));
        }
        Ok(target)
    }

    async fn owner_count(&self, org_id: &str) -> Result<usize, CtrlError> {
        Ok(self
            .meta
            .list_org_users(org_id)
            .await
            .map_err(CtrlError::meta)?
            .iter()
            .filter(|u| u.role == Role::Owner)
            .count())
    }

    /// Creates a one-hour reset token for `user_id` and returns the Studio link for it. Any
    /// earlier unused link for the account stops working, so at most one is live.
    async fn new_reset_link(&self, user_id: &str) -> Result<(String, String), CtrlError> {
        let token = random_hex();
        self.meta
            .delete_password_resets(user_id)
            .await
            .map_err(CtrlError::meta)?;
        self.meta
            .insert_password_reset(&sha256_hex(&token), user_id, now_ms() + RESET_TTL_MS)
            .await
            .map_err(CtrlError::meta)?;
        let url = format!("{}/#/reset/{token}", self.auth_cfg.read().public_url);
        Ok((token, url))
    }
}

// ------------------- Own account -------------------

#[derive(Deserialize)]
pub(super) struct UpdateMeReq {
    name: Option<String>,
}

pub(super) async fn handle_update_me(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(p): Extension<AuthPrincipal>,
    Json(req): Json<UpdateMeReq>,
) -> ApiRes<Value> {
    let uid = session_user(&p)?;
    let name: Option<String> = req
        .name
        .map(|n| n.trim().chars().take(120).collect())
        .filter(|n: &String| !n.is_empty());
    ctrl.meta
        .update_user_name(uid, name.as_deref())
        .await
        .map_err(CtrlError::meta)?;
    Ok(Json(user_json(&ctrl.get_user(uid).await?)))
}

#[derive(Deserialize)]
struct ChangePasswordReq {
    current_password: String,
    new_password: String,
}

/// Changes the password and signs out every other session.
async fn handle_change_password(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(p): Extension<AuthPrincipal>,
    headers: HeaderMap,
    Json(req): Json<ChangePasswordReq>,
) -> ApiRes<Value> {
    let uid = session_user(&p)?;
    check_password(&req.new_password)?;
    let user = ctrl.get_user(uid).await?;
    let Some(phc) = user.password_hash.clone() else {
        return Err(CtrlError::bad(
            "this account signs in with Google; set a password with \"Forgot password\" on the sign-in page",
        ));
    };
    let current = req.current_password;
    let ok = tokio::task::spawn_blocking(move || verify_password(&current, &phc))
        .await
        .map_err(CtrlError::internal)?;
    if !ok {
        return Err(CtrlError::forbidden("current password is incorrect"));
    }
    let phc = hash(req.new_password).await?;
    ctrl.meta
        .update_user_password(uid, &phc)
        .await
        .map_err(CtrlError::meta)?;
    // A reset link someone else holds must not undo this change.
    ctrl.meta
        .delete_password_resets(uid)
        .await
        .map_err(CtrlError::meta)?;
    let keep = current_hash(&headers);
    let revoked = ctrl
        .meta
        .delete_user_sessions(uid, None, keep.as_deref())
        .await
        .map_err(CtrlError::meta)?;
    ctrl.record_enterprise_audit(
        &p.actor_label(),
        "auth.password_changed",
        &format!("user:{uid}"),
        json!({ "other_sessions_signed_out": revoked }),
    )
    .await;
    Ok(Json(
        json!({ "status": "ok", "other_sessions_signed_out": revoked }),
    ))
}

async fn handle_list_sessions(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(p): Extension<AuthPrincipal>,
    headers: HeaderMap,
) -> ApiRes<Value> {
    let uid = session_user(&p)?;
    let current = current_hash(&headers);
    let rows = ctrl
        .meta
        .list_sessions(uid, now_ms())
        .await
        .map_err(CtrlError::meta)?;
    let out: Vec<Value> = rows
        .into_iter()
        .map(|(hash, info)| {
            let mut v = serde_json::to_value(info).unwrap_or_default();
            v["current"] = json!(current.as_deref() == Some(hash.as_str()));
            v
        })
        .collect();
    Ok(Json(json!(out)))
}

async fn handle_revoke_session(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(p): Extension<AuthPrincipal>,
    AxumPath(sid): AxumPath<String>,
) -> ApiRes<Value> {
    let uid = session_user(&p)?;
    if sid.len() != 16 || !sid.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(CtrlError::not_found("session not found"));
    }
    let n = ctrl
        .meta
        .delete_user_sessions(uid, Some(&sid.to_ascii_lowercase()), None)
        .await
        .map_err(CtrlError::meta)?;
    if n == 0 {
        return Err(CtrlError::not_found("session not found"));
    }
    Ok(Json(json!({ "status": "ok" })))
}

async fn handle_revoke_other_sessions(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(p): Extension<AuthPrincipal>,
    headers: HeaderMap,
) -> ApiRes<Value> {
    let uid = session_user(&p)?;
    let keep = current_hash(&headers);
    let n = ctrl
        .meta
        .delete_user_sessions(uid, None, keep.as_deref())
        .await
        .map_err(CtrlError::meta)?;
    Ok(Json(json!({ "status": "ok", "signed_out": n })))
}

// ------------------- Organisation and members -------------------

async fn handle_get_org(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(p): Extension<AuthPrincipal>,
) -> ApiRes<Value> {
    let org_id = org_of(&p)?;
    let name = ctrl
        .meta
        .get_org_name(&org_id)
        .await
        .map_err(CtrlError::meta)?;
    Ok(Json(
        json!({ "id": org_id, "name": name, "role": p.role().as_str() }),
    ))
}

#[derive(Deserialize)]
struct RenameOrgReq {
    name: String,
}

async fn handle_rename_org(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(p): Extension<AuthPrincipal>,
    Json(req): Json<RenameOrgReq>,
) -> ApiRes<Value> {
    let org_id = org_of(&p)?;
    let name: String = req.name.trim().chars().take(80).collect();
    if name.is_empty() {
        return Err(CtrlError::bad("enter a name"));
    }
    ctrl.meta
        .rename_org(&org_id, &name)
        .await
        .map_err(CtrlError::meta)?;
    ctrl.record_enterprise_audit(
        &p.actor_label(),
        "org.renamed",
        &format!("org:{org_id}"),
        json!({ "name": name }),
    )
    .await;
    Ok(Json(
        json!({ "id": org_id, "name": name, "role": p.role().as_str() }),
    ))
}

async fn handle_list_members(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(p): Extension<AuthPrincipal>,
) -> ApiRes<Value> {
    let org_id = org_of(&p)?;
    let users = ctrl
        .meta
        .list_org_users(&org_id)
        .await
        .map_err(CtrlError::meta)?;
    let out: Vec<Value> = users
        .iter()
        .map(|u| {
            let mut v = user_json(u);
            v["created_at_ms"] = json!(u.created_at_ms);
            v
        })
        .collect();
    Ok(Json(json!(out)))
}

#[derive(Deserialize)]
struct SetRoleReq {
    role: String,
}

async fn handle_set_member_role(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(p): Extension<AuthPrincipal>,
    AxumPath(user_id): AxumPath<String>,
    Json(req): Json<SetRoleReq>,
) -> ApiRes<Value> {
    let org_id = org_of(&p)?;
    let role = parse_role(&req.role)?;
    let target = ctrl.manageable_member(&p, &org_id, &user_id).await?;
    if !p.role().outranks(role) {
        return Err(CtrlError::forbidden(format!(
            "only an owner can grant the {} role",
            role.as_str()
        )));
    }
    if target.role == Role::Owner && role != Role::Owner && ctrl.owner_count(&org_id).await? <= 1 {
        return Err(CtrlError::conflict(
            "an organisation needs at least one owner",
        ));
    }
    ctrl.meta
        .update_user_role(&org_id, &user_id, role)
        .await
        .map_err(CtrlError::meta)?;
    ctrl.record_enterprise_audit(
        &p.actor_label(),
        "member.role_changed",
        &format!("org:{org_id}"),
        json!({ "user_id": user_id, "email": target.email, "from": target.role.as_str(), "to": role.as_str() }),
    )
    .await;
    Ok(Json(json!({ "id": user_id, "role": role.as_str() })))
}

async fn handle_remove_member(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(p): Extension<AuthPrincipal>,
    AxumPath(user_id): AxumPath<String>,
) -> ApiRes<Value> {
    let org_id = org_of(&p)?;
    let target = ctrl.manageable_member(&p, &org_id, &user_id).await?;
    if target.role == Role::Owner && ctrl.owner_count(&org_id).await? <= 1 {
        return Err(CtrlError::conflict(
            "an organisation needs at least one owner",
        ));
    }
    ctrl.meta
        .delete_user(&org_id, &user_id)
        .await
        .map_err(CtrlError::meta)?;
    ctrl.record_enterprise_audit(
        &p.actor_label(),
        "member.removed",
        &format!("org:{org_id}"),
        json!({ "user_id": user_id, "email": target.email, "role": target.role.as_str() }),
    )
    .await;
    Ok(Json(json!({ "status": "ok" })))
}

/// Gives an admin a one-time link to hand to a member who lost their password (there is no mailer).
async fn handle_admin_reset_link(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(p): Extension<AuthPrincipal>,
    AxumPath(user_id): AxumPath<String>,
) -> ApiRes<Value> {
    let org_id = org_of(&p)?;
    let target = ctrl.manageable_member(&p, &org_id, &user_id).await?;
    let (_, url) = ctrl.new_reset_link(&target.id).await?;
    ctrl.record_enterprise_audit(
        &p.actor_label(),
        "auth.password_reset_link",
        &format!("org:{org_id}"),
        json!({ "user_id": user_id, "email": target.email }),
    )
    .await;
    Ok(Json(
        json!({ "reset_url": url, "expires_in_ms": RESET_TTL_MS }),
    ))
}

// ------------------- Invites -------------------

async fn handle_list_invites(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(p): Extension<AuthPrincipal>,
) -> ApiRes<Vec<OrgInvite>> {
    let org_id = org_of(&p)?;
    Ok(Json(
        ctrl.meta
            .list_invites(&org_id, now_ms())
            .await
            .map_err(CtrlError::meta)?,
    ))
}

#[derive(Deserialize)]
struct CreateInviteReq {
    email: String,
    role: String,
}

/// Creates an invite and returns its link once, to the inviter, who passes it on. It is never logged.
async fn handle_create_invite(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(p): Extension<AuthPrincipal>,
    Json(req): Json<CreateInviteReq>,
) -> ApiRes<Value> {
    let org_id = org_of(&p)?;
    let email =
        normalize_email(&req.email).ok_or_else(|| CtrlError::bad("enter a valid email address"))?;
    let role = parse_role(&req.role)?;
    if !p.role().outranks(role) {
        return Err(CtrlError::forbidden(format!(
            "only an owner can invite someone as {}",
            role.as_str()
        )));
    }
    // Someone with an account elsewhere can still be invited: they accept while signed in and move here.
    if ctrl
        .meta
        .get_user_by_email(&email)
        .await
        .map_err(CtrlError::meta)?
        .is_some_and(|u| u.org_id == org_id)
    {
        return Err(CtrlError::conflict("this person is already a member"));
    }
    let token = random_hex();
    let now = now_ms();
    let inv = OrgInvite {
        id: format!("inv_{}", &Uuid::new_v4().simple().to_string()[..12]),
        org_id: org_id.clone(),
        email,
        role: role.as_str().to_string(),
        invited_by: p.actor_label(),
        created_at_ms: now,
        expires_at_ms: now + INVITE_TTL_MS,
    };
    ctrl.meta
        .insert_invite(&inv, &sha256_hex(&token))
        .await
        .map_err(CtrlError::meta)?;
    let url = format!("{}/#/invite/{token}", ctrl.auth_cfg.read().public_url);
    ctrl.record_enterprise_audit(
        &p.actor_label(),
        "invite.created",
        &format!("org:{org_id}"),
        json!({ "invite_id": inv.id, "email": inv.email, "role": inv.role }),
    )
    .await;
    let mut out = serde_json::to_value(&inv).unwrap_or_default();
    out["invite_url"] = json!(url);
    Ok(Json(out))
}

async fn handle_delete_invite(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(p): Extension<AuthPrincipal>,
    AxumPath(invite_id): AxumPath<String>,
) -> ApiRes<Value> {
    let org_id = org_of(&p)?;
    if !ctrl
        .meta
        .delete_invite(&org_id, &invite_id)
        .await
        .map_err(CtrlError::meta)?
    {
        return Err(CtrlError::not_found("invite not found"));
    }
    ctrl.record_enterprise_audit(
        &p.actor_label(),
        "invite.revoked",
        &format!("org:{org_id}"),
        json!({ "invite_id": invite_id }),
    )
    .await;
    Ok(Json(json!({ "status": "ok" })))
}

fn invite_gone() -> CtrlError {
    CtrlError(
        StatusCode::GONE,
        "This invite has expired or was already used. Ask for a new one.".to_string(),
    )
}

async fn handle_get_invite(
    State(ctrl): State<Arc<ControlPlaneService>>,
    AxumPath(token): AxumPath<String>,
) -> ApiRes<Value> {
    let inv = ctrl
        .meta
        .get_invite(&sha256_hex(&token), now_ms())
        .await
        .map_err(CtrlError::meta)?
        .ok_or_else(invite_gone)?;
    let org_name = ctrl
        .meta
        .get_org_name(&inv.org_id)
        .await
        .map_err(CtrlError::meta)?;
    // A verified account must sign in and join; an unverified one is replaced when the invite is accepted.
    let has_account = ctrl
        .meta
        .get_user_by_email(&inv.email)
        .await
        .map_err(CtrlError::meta)?
        .is_some_and(|u| u.org_id != PENDING_ORG);
    Ok(Json(
        json!({ "email": inv.email, "role": inv.role, "org_name": org_name, "has_account": has_account }),
    ))
}

#[derive(Deserialize)]
struct AcceptInviteReq {
    token: String,
    password: String,
    name: Option<String>,
}

async fn handle_accept_invite(
    State(ctrl): State<Arc<ControlPlaneService>>,
    headers: HeaderMap,
    uri: Uri,
    Json(req): Json<AcceptInviteReq>,
) -> std::result::Result<Response, CtrlError> {
    check_password(&req.password)?;
    let now = now_ms();
    let inv = ctrl
        .meta
        .get_invite(&sha256_hex(&req.token), now)
        .await
        .map_err(CtrlError::meta)?
        .ok_or_else(invite_gone)?;
    let existing = ctrl
        .meta
        .get_user_by_email(&inv.email)
        .await
        .map_err(CtrlError::meta)?;
    if existing.is_some_and(|u| u.org_id != PENDING_ORG) {
        return Err(CtrlError::conflict(
            "You already have an account. Sign in, then open this invite link again to join.",
        ));
    }
    let phc = hash(req.password).await?;
    if !ctrl
        .meta
        .accept_invite(&inv.id, now)
        .await
        .map_err(CtrlError::meta)?
    {
        return Err(invite_gone());
    }
    // The invite link proves the address, so an unverified sign-up with it is replaced.
    let user = ctrl
        .insert_pending_user(User {
            id: format!("user_{}", Uuid::new_v4().simple()),
            email: inv.email.clone(),
            name: req
                .name
                .map(|n| n.trim().chars().take(120).collect::<String>())
                .filter(|n| !n.is_empty()),
            password_hash: Some(phc),
            google_sub: None,
            created_at_ms: now,
            org_id: String::new(),
            role: Role::Viewer,
        })
        .await?;
    let role = Role::parse(&inv.role).unwrap_or(Role::Viewer);
    ctrl.meta
        .set_user_org(&user.id, &inv.org_id, role)
        .await
        .map_err(CtrlError::meta)?;
    let user = User {
        org_id: inv.org_id.clone(),
        role,
        ..user
    };
    ctrl.record_enterprise_audit(
        &format!("user:{}", user.id),
        "invite.accepted",
        &format!("org:{}", inv.org_id),
        json!({ "invite_id": inv.id, "email": inv.email, "role": inv.role }),
    )
    .await;
    ctrl.session_response(&user, &headers, &uri).await
}

// ------------------- Password reset -------------------

#[derive(Deserialize)]
struct ResetRequestReq {
    email: String,
}

/// Always answers 204 so it can't be used to find out which emails have accounts. When the account
/// exists, the reset link goes to the outbox file (there is no mailer), at most once per 5 minutes.
async fn handle_request_reset(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Json(req): Json<ResetRequestReq>,
) -> std::result::Result<StatusCode, CtrlError> {
    if let Some(email) = normalize_email(&req.email) {
        let user = ctrl
            .meta
            .get_user_by_email(&email)
            .await
            .map_err(CtrlError::meta)?;
        // Unverified sign-ups verify first; a reset link would be a second way to prove the address.
        let now = now_ms();
        let recent = ctrl
            .auth_state
            .reset_requests
            .get(&email)
            .is_some_and(|t| now - *t < RESET_REQUEST_INTERVAL_MS);
        if let Some(user) = user.filter(|u| u.org_id != PENDING_ORG && !recent) {
            let requests = &ctrl.auth_state.reset_requests;
            if requests.len() >= 10_000 {
                requests.retain(|_, t| now - *t < RESET_REQUEST_INTERVAL_MS);
            }
            requests.insert(email.clone(), now);
            let (_, url) = ctrl.new_reset_link(&user.id).await?;
            ctrl.deliver_link("reset", &email, &url);
        }
    }
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
struct ResetPasswordReq {
    token: String,
    new_password: String,
}

/// Sets a new password from a reset link and signs out every session of that account.
async fn handle_reset_password(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Json(req): Json<ResetPasswordReq>,
) -> ApiRes<Value> {
    check_password(&req.new_password)?;
    let phc = hash(req.new_password).await?;
    let uid = ctrl
        .meta
        .take_password_reset(&sha256_hex(&req.token), now_ms())
        .await
        .map_err(CtrlError::meta)?
        .ok_or_else(|| {
            CtrlError(
                StatusCode::GONE,
                "This reset link has expired or was already used.".to_string(),
            )
        })?;
    ctrl.meta
        .update_user_password(&uid, &phc)
        .await
        .map_err(CtrlError::meta)?;
    ctrl.meta
        .delete_password_resets(&uid)
        .await
        .map_err(CtrlError::meta)?;
    ctrl.meta
        .delete_user_sessions(&uid, None, None)
        .await
        .map_err(CtrlError::meta)?;
    ctrl.record_enterprise_audit(
        &format!("user:{uid}"),
        "auth.password_reset",
        &format!("user:{uid}"),
        json!({}),
    )
    .await;
    Ok(Json(json!({ "status": "ok" })))
}

// ------------------- Email verification and joining -------------------

#[derive(Deserialize)]
struct TokenReq {
    token: String,
}

/// Finishes a password sign-up: the account joins (or creates) its organisation and is signed in.
/// The sign-up policy is checked again here, in case it changed while the link was waiting.
async fn handle_verify_email(
    State(ctrl): State<Arc<ControlPlaneService>>,
    headers: HeaderMap,
    uri: Uri,
    Json(req): Json<TokenReq>,
) -> std::result::Result<Response, CtrlError> {
    let gone = || {
        CtrlError(
            StatusCode::GONE,
            "This link has expired or was already used. Sign up again for a new one.".to_string(),
        )
    };
    let (uid, org_name) = ctrl
        .meta
        .take_email_verification(&sha256_hex(&req.token), now_ms())
        .await
        .map_err(CtrlError::meta)?
        .ok_or_else(gone)?;
    let user = ctrl.get_user(&uid).await.map_err(|_| gone())?;
    if user.org_id != PENDING_ORG {
        return Err(gone());
    }
    let (allow_new_orgs, domain_ok) = {
        let cfg = ctrl.auth_cfg.read();
        let domain = user
            .email
            .rsplit_once('@')
            .map(|(_, d)| d)
            .unwrap_or_default();
        (
            cfg.allow_new_orgs,
            cfg.allowed_email_domains
                .iter()
                .any(|d| d.eq_ignore_ascii_case(domain)),
        )
    };
    let org = match org_name {
        Some(name) if allow_new_orgs => SignupOrg::New(name),
        None if domain_ok => SignupOrg::Default,
        _ => {
            return Err(CtrlError::forbidden(
                "Sign-ups are closed for this workspace.",
            ))
        }
    };
    let (org_id, role) = ctrl.assign_org(&uid, org).await?;
    ctrl.record_enterprise_audit(
        &format!("user:{uid}"),
        "auth.email_verified",
        &format!("org:{org_id}"),
        json!({ "email": user.email, "role": role.as_str() }),
    )
    .await;
    let user = User {
        org_id,
        role,
        ..user
    };
    ctrl.session_response(&user, &headers, &uri).await
}

/// A signed-in account accepts an invite addressed to its email and moves to that organisation.
/// A user belongs to one organisation, so this leaves the current one; the last owner of an
/// organisation that still has other members must hand over ownership first.
async fn handle_join_org(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(p): Extension<AuthPrincipal>,
    Json(req): Json<TokenReq>,
) -> ApiRes<Value> {
    let uid = session_user(&p)?;
    let now = now_ms();
    let inv = ctrl
        .meta
        .get_invite(&sha256_hex(&req.token), now)
        .await
        .map_err(CtrlError::meta)?
        .ok_or_else(invite_gone)?;
    let user = ctrl.get_user(uid).await?;
    if user.email != inv.email {
        return Err(CtrlError::forbidden(format!(
            "This invite is for {}. Sign in with that account to accept it.",
            inv.email
        )));
    }
    if user.org_id == inv.org_id {
        return Err(CtrlError::conflict(
            "you're already a member of this organisation",
        ));
    }
    // Leaving as the last owner would strand the organisation, its projects and live ingest keys
    // where nobody can reach them, even when nobody else is in it.
    if user.role == Role::Owner && ctrl.owner_count(&user.org_id).await? <= 1 {
        return Err(CtrlError::conflict(
            "You're the only owner of your current organisation, so leaving it would leave its projects with nobody to manage them. Invite someone as owner there first, or accept this invite from a different account.",
        ));
    }
    if !ctrl
        .meta
        .accept_invite(&inv.id, now)
        .await
        .map_err(CtrlError::meta)?
    {
        return Err(invite_gone());
    }
    let role = Role::parse(&inv.role).unwrap_or(Role::Viewer);
    ctrl.meta
        .set_user_org(uid, &inv.org_id, role)
        .await
        .map_err(CtrlError::meta)?;
    ctrl.record_enterprise_audit(
        &p.actor_label(),
        "invite.accepted",
        &format!("org:{}", inv.org_id),
        json!({ "invite_id": inv.id, "email": inv.email, "role": inv.role, "left_org": user.org_id }),
    )
    .await;
    Ok(Json(user_json(&User {
        org_id: inv.org_id,
        role,
        ..user
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_guard_table() {
        let r = |m: Method, p: &str| required_role(&m, p);
        assert_eq!(
            r(Method::GET, "/v1/projects/:id/saved-reports"),
            Role::Viewer
        );
        assert_eq!(
            r(Method::POST, "/v1/projects/:id/query/funnel"),
            Role::Viewer
        );
        assert_eq!(
            r(Method::PUT, "/v1/projects/:id/saved-reports/:rep_id"),
            Role::Member
        );
        assert_eq!(r(Method::POST, "/v1/projects/:id/dashboards"), Role::Member);
        assert_eq!(
            r(Method::POST, "/v1/projects/:id/some-new-write"),
            Role::Member
        );
        assert_eq!(
            r(Method::PUT, "/v1/projects/:id/cohorts/:cohort_id"),
            Role::Member
        );
        assert_eq!(
            r(Method::DELETE, "/v1/projects/:id/cohorts/:cohort_id"),
            Role::Member
        );
        assert_eq!(r(Method::GET, "/v1/projects/:id/api-keys"), Role::Admin);
        assert_eq!(
            r(Method::GET, "/v1/projects/:id/cohorts/:cohort_id/export"),
            Role::Admin
        );
        assert_eq!(
            r(Method::DELETE, "/v1/projects/:id/users/:distinct_id"),
            Role::Admin
        );
        assert_eq!(
            r(Method::POST, "/v1/projects/:id/query/funnel/dropped"),
            Role::Admin
        );
        assert_eq!(r(Method::POST, "/v1/projects"), Role::Admin);
        assert_eq!(r(Method::PATCH, "/v1/projects/:id"), Role::Admin);
        assert_eq!(r(Method::GET, "/v1/org/members"), Role::Viewer);
        assert_eq!(r(Method::PATCH, "/v1/org/members/:user_id"), Role::Admin);
        assert_eq!(r(Method::PATCH, "/v1/org"), Role::Owner);
        assert_eq!(r(Method::POST, "/v1/auth/password"), Role::Viewer);
    }

    #[test]
    fn outranks() {
        assert!(Role::Owner.outranks(Role::Owner));
        assert!(Role::Admin.outranks(Role::Member));
        assert!(!Role::Admin.outranks(Role::Admin));
        assert!(Role::Member.outranks(Role::Viewer));
        assert!(!Role::Viewer.outranks(Role::Viewer));
    }
}
