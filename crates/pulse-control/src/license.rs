use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::Utc;
use pulse_storage::ByokEncryptionStatus;
use ring::signature::{self, Ed25519KeyPair, UnparsedPublicKey};
use serde::{Deserialize, Serialize};

pub const LICENSE_PREFIX: &str = "pulse_lic_v1.";
/// 14-day read/write grace period after offline license expiry (`M5`).
pub const LICENSE_GRACE_PERIOD_MS: i64 = 14 * 86_400_000;

/// Public Ed25519 key for verifying offline commercial licenses. Private key stays strictly offline (`C1`).
const DEFAULT_VENDOR_ED25519_PUBKEY_HEX: &str =
    "9a18325c29fa84a0143ae936f993b2a967c0bf78c504a23410e52e14954038ae";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EditionTier {
    Community,
    CommercialByok,
}

impl EditionTier {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Community => "community",
            Self::CommercialByok => "commercial_byok",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommercialFeature {
    ByokAiCopilot,
    WarehouseExport,
    EnterpriseAuditLog,
    WebhookAlerts,
}

impl CommercialFeature {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ByokAiCopilot => "byok_ai_copilot",
            Self::WarehouseExport => "warehouse_export",
            Self::EnterpriseAuditLog => "enterprise_audit_log",
            Self::WebhookAlerts => "webhook_alerts",
        }
    }

    pub fn all_names() -> Vec<String> {
        vec![
            Self::ByokAiCopilot.as_str().to_string(),
            Self::WarehouseExport.as_str().to_string(),
            Self::EnterpriseAuditLog.as_str().to_string(),
            Self::WebhookAlerts.as_str().to_string(),
        ]
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LicenseHealth {
    Active,
    GracePeriod,
    Expired,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LicenseClaims {
    pub license_id: String,
    pub customer_org: String,
    pub edition: EditionTier,
    pub features: Vec<String>,
    pub issued_at_ms: i64,
    pub expires_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemLicenseRecord {
    pub license_key: String,
    pub customer_org: String,
    pub edition: String,
    pub features: Vec<String>,
    pub expires_at_ms: i64,
    pub activated_at_ms: i64,
    pub max_seen_ts_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LicenseStatusResponse {
    pub edition: String,
    pub health: String,
    pub is_commercial: bool,
    pub in_grace_period: bool,
    pub customer_org: Option<String>,
    pub license_id: Option<String>,
    pub expires_at_ms: Option<i64>,
    pub grace_ends_at_ms: Option<i64>,
    pub features: Vec<String>,
    pub encryption: ByokEncryptionStatus,
}

fn active_vendor_pubkey_bytes() -> Result<Vec<u8>, String> {
    let raw_hex = std::env::var("PULSE_LICENSE_PUBKEY_HEX")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_VENDOR_ED25519_PUBKEY_HEX.to_string());
    let decoded = hex::decode(raw_hex.trim())
        .map_err(|_| "invalid PULSE_LICENSE_PUBKEY_HEX encoding".to_string())?;
    if decoded.len() != 32 {
        return Err("Ed25519 public key must be 32 bytes (64 hex chars)".to_string());
    }
    Ok(decoded)
}

/// Evaluates whether verified license claims are Active, in the 14-day GracePeriod, or Expired,
/// clamping the clock against `max_seen_ts_ms` (`M5`).
pub fn evaluate_license_health(
    claims: &LicenseClaims,
    wall_now_ms: i64,
    max_seen_ts_ms: i64,
) -> LicenseHealth {
    let effective_now = wall_now_ms.max(max_seen_ts_ms);
    if effective_now <= claims.expires_at_ms {
        LicenseHealth::Active
    } else if effective_now <= claims.expires_at_ms.saturating_add(LICENSE_GRACE_PERIOD_MS) {
        LicenseHealth::GracePeriod
    } else {
        LicenseHealth::Expired
    }
}

/// Verifies an offline Ed25519-signed Commercial license token (`pulse_lic_v1.<payload_b64>.<sig_hex>`).
/// Note: rejects completely expired licenses past their 14-day grace period, while permitting activation
/// within `[issued, expires + grace]` as reflected by `evaluate_license_health`.
pub fn verify_license_signature(license_key: &str) -> Result<LicenseClaims, String> {
    let trimmed = license_key.trim();
    let body = trimmed
        .strip_prefix(LICENSE_PREFIX)
        .ok_or_else(|| "invalid license key prefix (expected pulse_lic_v1.)".to_string())?;
    let (payload_b64, sig_hex) = body
        .split_once('.')
        .ok_or_else(|| "malformed license key payload".to_string())?;

    let sig_bytes = hex::decode(sig_hex.trim())
        .map_err(|_| "malformed license key Ed25519 signature encoding".to_string())?;
    let pubkey_bytes = active_vendor_pubkey_bytes()?;
    let pubkey = UnparsedPublicKey::new(&signature::ED25519, &pubkey_bytes);
    pubkey
        .verify(payload_b64.as_bytes(), &sig_bytes)
        .map_err(|_| "Ed25519 license signature verification failed".to_string())?;

    let payload_bytes = URL_SAFE_NO_PAD
        .decode(payload_b64)
        .map_err(|_| "malformed base64 license claims".to_string())?;
    let claims: LicenseClaims = serde_json::from_slice(&payload_bytes)
        .map_err(|e| format!("invalid license claims payload: {e}"))?;

    if claims.edition != EditionTier::CommercialByok {
        return Err("license does not grant commercial_byok edition".to_string());
    }
    Ok(claims)
}

/// Verifies signature and checks expiration (including 14-day grace window).
pub fn verify_license_key(license_key: &str) -> Result<LicenseClaims, String> {
    let claims = verify_license_signature(license_key)?;
    let now_ms = Utc::now().timestamp_millis();
    if matches!(
        evaluate_license_health(&claims, now_ms, 0),
        LicenseHealth::Expired
    ) {
        return Err("license key and grace period have expired".to_string());
    }
    Ok(claims)
}

/// Signs a `LicenseClaims` payload using a vendor-supplied PKCS#8 Ed25519 private key (`C1`).
/// Used strictly by external vendor tooling & tests — no vendor private key is embedded in Pulse.
pub fn sign_license_ed25519(pkcs8_der: &[u8], claims: &LicenseClaims) -> Result<String, String> {
    let key_pair = Ed25519KeyPair::from_pkcs8(pkcs8_der)
        .map_err(|_| "invalid Ed25519 PKCS#8 private key".to_string())?;
    let json_bytes = serde_json::to_vec(claims).map_err(|e| e.to_string())?;
    let payload_b64 = URL_SAFE_NO_PAD.encode(json_bytes);
    let sig = key_pair.sign(payload_b64.as_bytes());
    Ok(format!(
        "{LICENSE_PREFIX}{payload_b64}.{}",
        hex::encode(sig.as_ref())
    ))
}
