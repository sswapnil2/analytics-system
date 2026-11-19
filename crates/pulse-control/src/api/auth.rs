//! Dashboard user accounts: email/password and Google sign-in, backed by `ps_` session tokens.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::Argon2;
use axum::extract::{DefaultBodyLimit, Query, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use chrono::Utc;
use dashmap::DashMap;
use rand::RngCore;
use serde::Deserialize;
use serde_json::{json, Value};
use uuid::Uuid;

use super::{extract_admin_bearer, ApiRes, AuthPrincipal, ControlPlaneService, CtrlError};
use crate::models::{sha256_bytes, sha256_hex, Role, User};

const SESSION_TTL_MS: i64 = 30 * 24 * 60 * 60 * 1000;
const SESSION_TTL_SECS: i64 = SESSION_TTL_MS / 1000;
pub(super) const SESSION_COOKIE_NAME: &str = "pulse_session";
const GOOGLE_STATE_TTL_MS: i64 = 10 * 60 * 1000;
const LOGIN_CODE_TTL_MS: i64 = 60 * 1000;
const LOGIN_FAILURE_WINDOW_MS: i64 = 15 * 60 * 1000;
const MAX_LOGIN_FAILURES: usize = 10;
/// Upper bound for each in-memory map so unauthenticated callers cannot grow them without limit.
const MAX_PENDING_ENTRIES: usize = 10_000;
const GOOGLE_AUTH_URL: &str = "https://accounts.google.com/o/oauth2/v2/auth";

const SIGNUP_CLOSED: &str =
    "Sign-ups are closed for this workspace. Ask an admin to add your email domain.";
const SETUP_CODE_NEEDED: &str = "Enter the setup code to create the first account. It's the root admin token: the PULSE_ADMIN_TOKEN you set, or the one saved in the data directory on first start.";
const BAD_LOGIN: &str = "Email or password is incorrect";

/// Sign-in configuration, read once from the environment; tests replace it via
/// [`ControlPlaneService::set_auth_config`].
#[derive(Debug, Clone)]
pub struct AuthConfig {
    pub google_client_id: Option<String>,
    pub google_client_secret: Option<String>,
    /// Origin the browser uses to reach this node, without a trailing slash.
    pub public_url: String,
    pub google_token_url: String,
    /// Lowercased domains whose addresses join the default organisation (the node owner's).
    pub allowed_email_domains: Vec<String>,
    /// Whether anyone may sign up and create a new, separate organisation (`PULSE_ALLOW_NEW_ORGS`, default on).
    pub allow_new_orgs: bool,
}

impl AuthConfig {
    pub fn from_env() -> Self {
        let var = |k: &str| {
            std::env::var(k)
                .ok()
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        };
        let port = var("PORT").unwrap_or_else(|| "8080".to_string());
        Self {
            google_client_id: var("PULSE_GOOGLE_CLIENT_ID"),
            google_client_secret: var("PULSE_GOOGLE_CLIENT_SECRET"),
            public_url: var("PULSE_PUBLIC_URL")
                .unwrap_or_else(|| format!("http://localhost:{port}"))
                .trim_end_matches('/')
                .to_string(),
            google_token_url: "https://oauth2.googleapis.com/token".to_string(),
            allowed_email_domains: var("PULSE_ALLOWED_EMAIL_DOMAINS")
                .unwrap_or_default()
                .split(',')
                .map(|d| d.trim().trim_start_matches('@').to_ascii_lowercase())
                .filter(|d| !d.is_empty())
                .collect(),
            allow_new_orgs: !matches!(
                var("PULSE_ALLOW_NEW_ORGS").as_deref(),
                Some("0" | "false" | "no" | "off")
            ),
        }
    }

    fn google_creds(&self) -> Option<(&str, &str)> {
        Some((
            self.google_client_id.as_deref()?,
            self.google_client_secret.as_deref()?,
        ))
    }

    fn redirect_uri(&self) -> String {
        format!("{}/v1/auth/google/callback", self.public_url)
    }
}

/// Short-lived in-memory sign-in state (single node).
#[derive(Default)]
pub(super) struct AuthState {
    /// OAuth `state` -> (`nonce`, expires_at_ms).
    google_states: DashMap<String, (String, i64)>,
    /// One-time login code -> (user_id, expires_at_ms).
    login_codes: DashMap<String, (String, i64)>,
    /// Lowercased email -> recent failed login timestamps.
    login_failures: DashMap<String, Vec<i64>>,
    /// Lowercased email -> when its last password-reset link was made.
    pub(super) reset_requests: DashMap<String, i64>,
}

impl ControlPlaneService {
    pub fn set_auth_config(&self, cfg: AuthConfig) {
        *self.auth_cfg.write() = cfg;
    }

    /// Where sign-up verification and password-reset links are written. `pulse-node` sets
    /// `<data dir>/outbox`; without it those links are dropped.
    pub fn set_outbox_path(&self, path: std::path::PathBuf) {
        *self.outbox_path.write() = Some(path);
    }

    /// Pulse has no mailer. A link that proves an email address goes to the owner-only outbox file,
    /// never to the log: anyone who can read shipped or shared logs must not be able to use it.
    pub(super) fn deliver_link(&self, kind: &str, email: &str, url: &str) {
        let Some(path) = self.outbox_path.read().clone() else {
            tracing::warn!(
                kind,
                "no outbox configured; a {kind} link for an account was not delivered"
            );
            return;
        };
        let line = format!("{} {kind} {email} {url}\n", Utc::now().to_rfc3339());
        match append_owner_only(&path, &line) {
            Ok(()) => tracing::info!(kind, "{kind} link written to {}", path.display()),
            Err(e) => tracing::warn!(
                kind,
                "couldn't write {kind} link to {}: {e}",
                path.display()
            ),
        }
    }

    /// Allowed when the email's domain is on the allowlist, or for the very first account when the
    /// caller proves they run the node by giving the root admin token as the setup code.
    /// Without that, whoever reached a fresh public node first would own every project.
    /// Which organisation a new account lands in, or `None` when it may not sign up.
    /// - The first account on a node needs the root admin token as a setup code (or an allowed
    ///   domain) and owns the default organisation and its existing data.
    /// - Later accounts with an organisation name get a new, separate organisation.
    /// - Later accounts in an allowed domain without one join the default organisation.
    async fn signup_org(
        &self,
        email: &str,
        setup_code: Option<&str>,
        org_name: Option<&str>,
    ) -> Result<Option<(SignupOrg, bool)>, CtrlError> {
        let domain = email.rsplit_once('@').map(|(_, d)| d).unwrap_or_default();
        let (domain_ok, allow_new_orgs) = {
            let cfg = self.auth_cfg.read();
            (
                cfg.allowed_email_domains
                    .iter()
                    .any(|d| d.eq_ignore_ascii_case(domain)),
                cfg.allow_new_orgs,
            )
        };
        if self.meta.count_users().await.map_err(CtrlError::meta)? == 0 {
            let code_ok = setup_code
                .map(str::trim)
                .filter(|c| !c.is_empty())
                .is_some_and(|c| {
                    constant_time_eq::constant_time_eq(&sha256_bytes(c), &self.root_admin_hash)
                });
            // The setup code proves the caller runs the node, so that account needs no email check.
            return Ok((domain_ok || code_ok).then_some((SignupOrg::Default, code_ok)));
        }
        let org_name = org_name.map(str::trim).filter(|n| !n.is_empty());
        Ok(match org_name {
            Some(name) if allow_new_orgs => {
                Some((SignupOrg::New(name.chars().take(80).collect()), false))
            }
            _ if domain_ok => Some((SignupOrg::Default, false)),
            _ => None,
        })
    }

    /// Puts an existing account (inserted first, in `PENDING_ORG`) into its sign-up organisation.
    /// A new organisation is created only now, after the account row exists, so a sign-up that
    /// loses a race on its email never leaves an empty organisation behind. Whoever creates an
    /// org owns it; the first account in the default org owns it; later ones are members.
    pub(super) async fn assign_org(
        &self,
        user_id: &str,
        org: SignupOrg,
    ) -> Result<(String, Role), CtrlError> {
        let (org_id, role) = match org {
            SignupOrg::Default => {
                // One statement decides owner-or-member, so two first sign-ups can't both own it.
                let role = self
                    .meta
                    .join_default_org(user_id)
                    .await
                    .map_err(CtrlError::meta)?;
                return Ok(("org_default".to_string(), role));
            }
            SignupOrg::New(name) => {
                let id = format!("org_{}", &Uuid::new_v4().simple().to_string()[..12]);
                self.meta
                    .insert_org(&id, &name, now_ms())
                    .await
                    .map_err(CtrlError::meta)?;
                self.create_project_in_org(&id, &name, None).await?;
                (id, Role::Owner)
            }
        };
        self.meta
            .set_user_org(user_id, &org_id, role)
            .await
            .map_err(CtrlError::meta)?;
        Ok((org_id, role))
    }

    /// Inserts a new account with no organisation yet. An unverified account with the same email
    /// is replaced: it has no session and no data, and must not let a stranger squat an address.
    pub(super) async fn insert_pending_user(&self, mut user: User) -> Result<User, CtrlError> {
        if let Some(old) = self
            .meta
            .get_user_by_email(&user.email)
            .await
            .map_err(CtrlError::meta)?
            .filter(|u| u.org_id == PENDING_ORG)
        {
            self.meta
                .delete_user(PENDING_ORG, &old.id)
                .await
                .map_err(CtrlError::meta)?;
        }
        user.org_id = PENDING_ORG.to_string();
        user.role = Role::Viewer;
        self.meta
            .insert_user(&user)
            .await
            .map_err(CtrlError::meta)?;
        Ok(user)
    }

    async fn create_session(
        &self,
        user_id: &str,
        headers: &HeaderMap,
    ) -> Result<String, CtrlError> {
        let token = format!("ps_{}", random_hex());
        let now = now_ms();
        let ua = headers
            .get(header::USER_AGENT)
            .and_then(|v| v.to_str().ok())
            .map(|v| v.chars().take(256).collect::<String>());
        self.meta
            .insert_session(
                &sha256_hex(&token),
                user_id,
                now,
                now + SESSION_TTL_MS,
                ua.as_deref(),
            )
            .await
            .map_err(CtrlError::meta)?;
        Ok(token)
    }

    pub(super) async fn session_response(
        &self,
        user: &User,
        headers: &HeaderMap,
        uri: &Uri,
    ) -> std::result::Result<Response, CtrlError> {
        let token = self.create_session(&user.id, headers).await?;
        self.record_enterprise_audit(
            &format!("user:{}", user.id),
            "auth.login",
            &format!("org:{}", user.org_id),
            json!({ "email": user.email }),
        )
        .await;
        let is_https = is_https_request(headers, Some(uri))
            || self.auth_cfg.read().public_url.starts_with("https://");
        let cookie = build_session_cookie(&token, is_https);
        let mut resp = Json(json!({ "token": token, "user": user_json(user) })).into_response();
        resp.headers_mut()
            .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
        if let Ok(val) = HeaderValue::from_str(&cookie) {
            resp.headers_mut().append(header::SET_COOKIE, val);
        }
        Ok(resp)
    }
}

pub(super) fn is_https_request(headers: &HeaderMap, uri: Option<&Uri>) -> bool {
    if uri.is_some_and(|u| u.scheme_str() == Some("https")) {
        return true;
    }
    if headers
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(',').next())
        .is_some_and(|first| first.trim().eq_ignore_ascii_case("https"))
    {
        return true;
    }
    if headers
        .get(header::FORWARDED)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| {
            v.split(',')
                .next()
                .unwrap_or("")
                .split(';')
                .any(|part| part.trim().eq_ignore_ascii_case("proto=https"))
        })
    {
        return true;
    }
    false
}

pub(super) fn build_session_cookie(token: &str, secure: bool) -> String {
    let secure_attr = if secure { "; Secure" } else { "" };
    format!(
        "{SESSION_COOKIE_NAME}={token}; HttpOnly; SameSite=Lax; Path=/; Max-Age={SESSION_TTL_SECS}{secure_attr}"
    )
}

pub(super) fn build_clear_session_cookie(secure: bool) -> String {
    let secure_attr = if secure { "; Secure" } else { "" };
    format!("{SESSION_COOKIE_NAME}=; HttpOnly; SameSite=Lax; Path=/; Max-Age=0{secure_attr}")
}

pub(super) fn extract_session_cookie(headers: &HeaderMap) -> Option<&str> {
    for val in headers.get_all(header::COOKIE) {
        let Ok(raw) = val.to_str() else {
            continue;
        };
        for part in raw.split(';') {
            let trimmed = part.trim();
            if let Some(v) = trimmed.strip_prefix("pulse_session=") {
                let tok = v.trim();
                if !tok.is_empty() {
                    return Some(tok);
                }
            }
        }
    }
    None
}

/// Placeholder organisation for sign-ups that haven't verified their email. It has no projects.
pub(super) const PENDING_ORG: &str = "org_pending";
const VERIFY_TTL_MS: i64 = 24 * 60 * 60 * 1000;

pub(super) enum SignupOrg {
    Default,
    New(String),
}

pub(super) fn public_auth_router() -> Router<Arc<ControlPlaneService>> {
    Router::new()
        .route("/v1/auth/config", get(handle_config))
        .route("/v1/auth/signup", post(handle_signup))
        .route("/v1/auth/login", post(handle_login))
        .route("/v1/auth/exchange", post(handle_exchange))
        .route("/v1/auth/google/start", get(handle_google_start))
        .route("/v1/auth/google/callback", get(handle_google_callback))
        .layer(DefaultBodyLimit::max(16 * 1024))
}

fn append_owner_only(path: &std::path::Path, line: &str) -> std::io::Result<()> {
    use std::io::Write;
    let mut opts = std::fs::OpenOptions::new();
    opts.append(true).create(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut opts, 0o600);
    opts.open(path)?.write_all(line.as_bytes())
}

pub(super) fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

pub(super) fn random_hex() -> String {
    let mut raw = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut raw);
    hex::encode(raw)
}

pub(super) fn user_json(u: &User) -> Value {
    json!({
        "id": u.id,
        "org_id": u.org_id,
        "email": u.email,
        "name": u.name,
        "has_password": u.password_hash.is_some(),
        "has_google": u.google_sub.is_some(),
        "role": u.role.as_str(),
    })
}

/// Lowercases and checks a basic `local@domain.tld` shape.
pub(super) fn normalize_email(raw: &str) -> Option<String> {
    let email = raw.trim().to_lowercase();
    let (local, domain) = email.split_once('@')?;
    let ok = !local.is_empty()
        && email.len() <= 254
        && !domain.contains('@')
        && !email.chars().any(char::is_whitespace)
        && domain.contains('.')
        && domain.split('.').all(|p| !p.is_empty());
    ok.then_some(email)
}

/// Inserts into a TTL map, sweeping expired entries first once it gets large.
fn insert_pending(
    map: &DashMap<String, (String, i64)>,
    key: String,
    value: String,
    expires_at_ms: i64,
) -> Result<(), CtrlError> {
    let now = now_ms();
    if map.len() >= MAX_PENDING_ENTRIES / 10 {
        map.retain(|_, (_, exp)| *exp > now);
    }
    if map.len() >= MAX_PENDING_ENTRIES {
        return Err(CtrlError(
            StatusCode::SERVICE_UNAVAILABLE,
            "too many pending sign-ins, try again shortly".to_string(),
        ));
    }
    map.insert(key, (value, expires_at_ms));
    Ok(())
}

/// Removes `key` (single use) and returns its value if it has not expired.
fn take_pending(map: &DashMap<String, (String, i64)>, key: &str) -> Option<String> {
    map.remove(key)
        .filter(|(_, (_, exp))| *exp > now_ms())
        .map(|(_, (v, _))| v)
}

pub(super) fn hash_password(password: &str) -> Result<String, String> {
    let mut salt = [0u8; 16];
    rand::rngs::OsRng.fill_bytes(&mut salt);
    let salt = SaltString::encode_b64(&salt).map_err(|e| e.to_string())?;
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|h| h.to_string())
        .map_err(|e| e.to_string())
}

pub(super) fn verify_password(password: &str, phc: &str) -> bool {
    PasswordHash::new(phc)
        .map(|h| {
            Argon2::default()
                .verify_password(password.as_bytes(), &h)
                .is_ok()
        })
        .unwrap_or(false)
}

/// Fixed hash verified when the account is missing or has no password, so login timing is uniform.
fn dummy_hash() -> &'static str {
    static DUMMY: OnceLock<String> = OnceLock::new();
    DUMMY.get_or_init(|| hash_password("pulse-dummy-password").expect("argon2 dummy hash"))
}

fn found(location: &str) -> Response {
    (
        StatusCode::FOUND,
        [
            (header::LOCATION, location.to_string()),
            (header::CACHE_CONTROL, "no-store".to_string()),
        ],
    )
        .into_response()
}

async fn handle_config(State(ctrl): State<Arc<ControlPlaneService>>) -> ApiRes<Value> {
    let (google, domains_configured) = {
        let cfg = ctrl.auth_cfg.read();
        (
            cfg.google_creds().is_some(),
            !cfg.allowed_email_domains.is_empty(),
        )
    };
    let allow_new_orgs = ctrl.auth_cfg.read().allow_new_orgs;
    let no_users = ctrl.meta.count_users().await.map_err(CtrlError::meta)? == 0;
    Ok(Json(json!({
        "password": true,
        "google": google,
        "signup_open": no_users || domains_configured || allow_new_orgs,
        // The first account needs the root admin token as a setup code unless its domain is allowlisted.
        "setup_code_required": no_users,
        // After that, a new account names its organisation (blank joins the default org for allowed domains).
        "org_name_required": !no_users && allow_new_orgs,
    })))
}

#[derive(Deserialize)]
struct SignupReq {
    email: String,
    password: String,
    name: Option<String>,
    #[serde(default)]
    setup_code: Option<String>,
    #[serde(default)]
    org_name: Option<String>,
    /// First account only: renames the default project.
    #[serde(default)]
    project_name: Option<String>,
}

async fn handle_signup(
    State(ctrl): State<Arc<ControlPlaneService>>,
    headers: HeaderMap,
    uri: Uri,
    Json(req): Json<SignupReq>,
) -> std::result::Result<Response, CtrlError> {
    let email =
        normalize_email(&req.email).ok_or_else(|| CtrlError::bad("enter a valid email address"))?;
    let pw_len = req.password.chars().count();
    if !(10..=256).contains(&pw_len) {
        return Err(CtrlError::bad(
            "password must be between 10 and 256 characters",
        ));
    }
    let Some((org, verified)) = ctrl
        .signup_org(&email, req.setup_code.as_deref(), req.org_name.as_deref())
        .await?
    else {
        if ctrl.meta.count_users().await.map_err(CtrlError::meta)? == 0 {
            return Err(CtrlError::forbidden(SETUP_CODE_NEEDED));
        }
        if ctrl.auth_cfg.read().allow_new_orgs {
            return Err(CtrlError::bad("Enter your organisation's name."));
        }
        return Err(CtrlError::forbidden(SIGNUP_CLOSED));
    };

    let password = req.password;
    let password_hash = tokio::task::spawn_blocking(move || hash_password(&password))
        .await
        .map_err(CtrlError::internal)?
        .map_err(CtrlError::internal)?;
    // The account row goes in first; the email's unique constraint settles concurrent sign-ups
    // before any organisation is created.
    let user = ctrl
        .insert_pending_user(User {
            id: format!("user_{}", Uuid::new_v4().simple()),
            email,
            name: req
                .name
                .map(|n| n.trim().to_string())
                .filter(|n| !n.is_empty()),
            password_hash: Some(password_hash),
            google_sub: None,
            created_at_ms: now_ms(),
            org_id: String::new(),
            role: Role::Viewer,
        })
        .await?;

    if verified {
        let first_owner = matches!(org, SignupOrg::Default);
        let (org_id, role) = ctrl.assign_org(&user.id, org).await?;
        // The node's first owner names the default organisation and project it takes over.
        if first_owner && role == Role::Owner {
            let clean = |s: &Option<String>| {
                s.as_deref()
                    .map(str::trim)
                    .filter(|n| !n.is_empty())
                    .map(|n| n.chars().take(80).collect::<String>())
            };
            if let Some(name) = clean(&req.org_name) {
                ctrl.meta
                    .rename_org(&org_id, &name)
                    .await
                    .map_err(CtrlError::meta)?;
            }
            if let Some(name) = clean(&req.project_name) {
                ctrl.meta
                    .rename_project(&ctrl.default_project_id, &name)
                    .await
                    .map_err(CtrlError::meta)?;
            }
        }
        let user = ctrl
            .meta
            .get_user_by_id(&user.id)
            .await
            .map_err(CtrlError::meta)?
            .ok_or_else(|| CtrlError::internal("account vanished"))?;
        return ctrl.session_response(&user, &headers, &uri).await;
    }

    // Everyone else proves the address first. Pulse has no mailer: the link goes to the node log.
    let token = random_hex();
    let org_name = match &org {
        SignupOrg::New(name) => Some(name.as_str()),
        SignupOrg::Default => None,
    };
    ctrl.meta
        .insert_email_verification(
            &sha256_hex(&token),
            &user.id,
            org_name,
            now_ms() + VERIFY_TTL_MS,
        )
        .await
        .map_err(CtrlError::meta)?;
    let url = format!("{}/#/verify/{token}", ctrl.auth_cfg.read().public_url);
    // ponytail: the outbox file is the mailer. Add SMTP when someone needs links delivered.
    ctrl.deliver_link("verify", &user.email, &url);
    Ok(Json(json!({ "verification_required": true, "email": user.email })).into_response())
}

#[derive(Deserialize)]
struct LoginReq {
    email: String,
    password: String,
}

async fn handle_login(
    State(ctrl): State<Arc<ControlPlaneService>>,
    headers: HeaderMap,
    uri: Uri,
    Json(req): Json<LoginReq>,
) -> std::result::Result<Response, CtrlError> {
    let email = req.email.trim().to_lowercase();
    let now = now_ms();
    let failures = &ctrl.auth_state.login_failures;
    let throttled = failures
        .get_mut(&email)
        .map(|mut ts| {
            ts.retain(|t| *t > now - LOGIN_FAILURE_WINDOW_MS);
            ts.len() >= MAX_LOGIN_FAILURES
        })
        .unwrap_or(false);
    if throttled {
        return Err(CtrlError(
            StatusCode::TOO_MANY_REQUESTS,
            "Too many attempts. Try again in a few minutes.".to_string(),
        ));
    }

    let user = ctrl
        .meta
        .get_user_by_email(&email)
        .await
        .map_err(CtrlError::meta)?;
    let phc = user
        .as_ref()
        .and_then(|u| u.password_hash.clone())
        .unwrap_or_else(|| dummy_hash().to_string());
    let has_password = user.as_ref().is_some_and(|u| u.password_hash.is_some());
    let password = req.password;
    let matched = tokio::task::spawn_blocking(move || verify_password(&password, &phc))
        .await
        .map_err(CtrlError::internal)?;

    match user {
        Some(user) if matched && has_password && user.org_id == PENDING_ORG => {
            Err(CtrlError::forbidden(
                "Verify your email first with the link from sign-up. To get a new link, sign up again.",
            ))
        }
        Some(user) if matched && has_password => {
            failures.remove(&email);
            ctrl.session_response(&user, &headers, &uri).await
        }
        _ => {
            if failures.len() >= MAX_PENDING_ENTRIES {
                failures.retain(|_, ts| ts.iter().any(|t| *t > now - LOGIN_FAILURE_WINDOW_MS));
            }
            let count = {
                let mut ts = failures.entry(email.clone()).or_default();
                ts.push(now);
                ts.len()
            };
            // One audit entry when an email gets locked out, not one per guess.
            if count == MAX_LOGIN_FAILURES {
                ctrl.record_enterprise_audit(
                    "anonymous",
                    "auth.login_throttled",
                    "auth",
                    json!({ "email": email, "failures": count }),
                )
                .await;
            }
            Err(CtrlError::unauthorized(BAD_LOGIN))
        }
    }
}

#[derive(Deserialize)]
struct ExchangeReq {
    code: String,
}

async fn handle_exchange(
    State(ctrl): State<Arc<ControlPlaneService>>,
    headers: HeaderMap,
    uri: Uri,
    Json(req): Json<ExchangeReq>,
) -> std::result::Result<Response, CtrlError> {
    let invalid = || CtrlError::unauthorized("Sign-in code is invalid or expired");
    let user_id = take_pending(&ctrl.auth_state.login_codes, &req.code).ok_or_else(invalid)?;
    let user = ctrl
        .meta
        .get_user_by_id(&user_id)
        .await
        .map_err(CtrlError::meta)?
        .ok_or_else(invalid)?;
    ctrl.session_response(&user, &headers, &uri).await
}

pub(super) async fn handle_me(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
) -> ApiRes<Value> {
    let AuthPrincipal::User { user_id, .. } = principal else {
        return Err(CtrlError::not_found(
            "this credential is not a user session",
        ));
    };
    let user = ctrl
        .meta
        .get_user_by_id(&user_id)
        .await
        .map_err(CtrlError::meta)?
        .ok_or_else(|| CtrlError::unauthorized("user no longer exists"))?;
    Ok(Json(user_json(&user)))
}

pub(super) async fn handle_logout(
    State(ctrl): State<Arc<ControlPlaneService>>,
    headers: HeaderMap,
    uri: Uri,
) -> std::result::Result<Response, CtrlError> {
    if let Some(token) = extract_admin_bearer(&headers)
        .or_else(|| extract_session_cookie(&headers))
        .filter(|t| t.starts_with("ps_"))
    {
        ctrl.meta
            .delete_session(&sha256_hex(token))
            .await
            .map_err(CtrlError::meta)?;
    }
    let clear_cookie = build_clear_session_cookie(is_https_request(&headers, Some(&uri)));
    let mut resp = Json(json!({ "status": "ok" })).into_response();
    if let Ok(val) = HeaderValue::from_str(&clear_cookie) {
        resp.headers_mut().append(header::SET_COOKIE, val);
    }
    Ok(resp)
}

async fn handle_google_start(State(ctrl): State<Arc<ControlPlaneService>>) -> Response {
    let cfg = ctrl.auth_cfg.read().clone();
    let Some((client_id, _)) = cfg.google_creds() else {
        return CtrlError::not_found("Google sign-in is not configured").into_response();
    };
    let (state, nonce) = (random_hex(), random_hex());
    if let Err(e) = insert_pending(
        &ctrl.auth_state.google_states,
        state.clone(),
        nonce.clone(),
        now_ms() + GOOGLE_STATE_TTL_MS,
    ) {
        return e.into_response();
    }
    let redirect_uri = cfg.redirect_uri();
    match reqwest::Url::parse_with_params(
        GOOGLE_AUTH_URL,
        &[
            ("response_type", "code"),
            ("client_id", client_id),
            ("redirect_uri", redirect_uri.as_str()),
            ("scope", "openid email profile"),
            ("state", state.as_str()),
            ("nonce", nonce.as_str()),
            ("prompt", "select_account"),
        ],
    ) {
        Ok(url) => found(url.as_str()),
        Err(e) => CtrlError::internal(e).into_response(),
    }
}

#[derive(Deserialize)]
struct GoogleCallbackQuery {
    code: Option<String>,
    state: Option<String>,
    error: Option<String>,
}

async fn handle_google_callback(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Query(q): Query<GoogleCallbackQuery>,
) -> Response {
    let cfg = ctrl.auth_cfg.read().clone();
    match google_callback(&ctrl, &cfg, q).await {
        Ok(code) => found(&format!("{}/#/auth/callback?code={code}", cfg.public_url)),
        Err(err) => found(&format!("{}/#/signin?error={err}", cfg.public_url)),
    }
}

/// Runs the OAuth callback, returning a one-time login code or a short error code for the UI.
async fn google_callback(
    ctrl: &ControlPlaneService,
    cfg: &AuthConfig,
    q: GoogleCallbackQuery,
) -> Result<String, &'static str> {
    // Consume the state even when Google reports an error, so it cannot be replayed.
    let nonce = q
        .state
        .as_deref()
        .and_then(|s| take_pending(&ctrl.auth_state.google_states, s));
    if q.error.is_some() {
        return Err("google_denied");
    }
    let nonce = nonce.ok_or("google_state")?;
    let (client_id, client_secret) = cfg.google_creds().ok_or("google_token")?;
    let code = q.code.ok_or("google_token")?;

    let claims = fetch_id_token_claims(cfg, client_id, client_secret, &code)
        .await
        .ok_or("google_token")?;
    let iss_ok = matches!(
        claims["iss"].as_str(),
        Some("accounts.google.com" | "https://accounts.google.com")
    );
    let aud_ok = match &claims["aud"] {
        Value::String(a) => a == client_id,
        Value::Array(list) => list.iter().any(|a| a.as_str() == Some(client_id)),
        _ => false,
    };
    let exp_ok = claims["exp"]
        .as_i64()
        .is_some_and(|exp| exp > Utc::now().timestamp());
    let nonce_ok = claims["nonce"].as_str() == Some(nonce.as_str());
    let sub = claims["sub"].as_str().filter(|s| !s.is_empty());
    let (true, true, true, true, Some(sub)) = (iss_ok, aud_ok, exp_ok, nonce_ok, sub) else {
        return Err("google_token");
    };
    let verified = claims["email_verified"] == Value::Bool(true)
        || claims["email_verified"].as_str() == Some("true");
    if !verified {
        return Err("google_unverified");
    }
    let email = claims["email"]
        .as_str()
        .and_then(normalize_email)
        .ok_or("google_token")?;

    let meta = &ctrl.meta;
    let user_id = if let Some(u) = meta
        .get_user_by_google_sub(sub)
        .await
        .map_err(|_| "google_token")?
    {
        u.id
    } else if let Some(u) = meta
        .get_user_by_email(&email)
        .await
        .map_err(|_| "google_token")?
        // An unverified password sign-up is not linked: the branch below replaces it.
        .filter(|u| u.org_id != PENDING_ORG)
    {
        // Link by verified email, but never re-point an account already bound to another Google identity.
        if u.google_sub.is_some() {
            return Err("google_token");
        }
        meta.set_user_google_sub(&u.id, sub)
            .await
            .map_err(|_| "google_token")?;
        u.id
    } else {
        // Google can't carry an org name, so a new Google account gets a workspace named after its owner.
        let display = claims["name"]
            .as_str()
            .filter(|n| !n.trim().is_empty())
            .unwrap_or_else(|| email.split('@').next().unwrap_or("My"));
        let org_name = format!("{display}'s workspace");
        let Some(org) = ctrl
            .signup_org(&email, None, Some(&org_name))
            .await
            .map_err(|_| "google_token")?
        else {
            return Err("signup_closed");
        };
        let (org, _) = org;
        let user = User {
            id: format!("user_{}", Uuid::new_v4().simple()),
            email,
            name: claims["name"]
                .as_str()
                .map(str::to_string)
                .filter(|n| !n.is_empty()),
            password_hash: None,
            google_sub: Some(sub.to_string()),
            created_at_ms: now_ms(),
            org_id: String::new(),
            role: Role::Viewer,
        };
        // Google has already verified the email, so the account joins its organisation at once.
        let user = ctrl
            .insert_pending_user(user)
            .await
            .map_err(|_| "google_token")?;
        ctrl.assign_org(&user.id, org)
            .await
            .map_err(|_| "google_token")?;
        user.id
    };

    let login_code = random_hex();
    insert_pending(
        &ctrl.auth_state.login_codes,
        login_code.clone(),
        user_id,
        now_ms() + LOGIN_CODE_TTL_MS,
    )
    .map_err(|_| "google_token")?;
    Ok(login_code)
}

/// Exchanges the authorization code and returns the decoded `id_token` payload.
async fn fetch_id_token_claims(
    cfg: &AuthConfig,
    client_id: &str,
    client_secret: &str,
    code: &str,
) -> Option<Value> {
    let redirect_uri = cfg.redirect_uri();
    let resp = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .ok()?
        .post(&cfg.google_token_url)
        .form(&[
            ("code", code),
            ("client_id", client_id),
            ("client_secret", client_secret),
            ("redirect_uri", redirect_uri.as_str()),
            ("grant_type", "authorization_code"),
        ])
        .send()
        .await
        .ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let body: Value = resp.json().await.ok()?;
    let id_token = body["id_token"].as_str()?;
    // The signature is not verified: this id_token came straight from Google's token endpoint over
    // TLS in direct response to our client-authenticated request, which OIDC Core 3.1.3.7 allows
    // in place of signature validation. Do NOT reuse this for id_tokens received any other way.
    let payload = id_token.split('.').nth(1)?;
    let bytes = URL_SAFE_NO_PAD.decode(payload.trim_end_matches('=')).ok()?;
    serde_json::from_slice(&bytes).ok()
}
