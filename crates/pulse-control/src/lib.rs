pub mod api;
pub mod copilot;
pub mod db;
pub mod experiments;
pub mod license;
pub mod mcp;
pub mod models;

pub use api::{
    build_admin_control_router, build_control_router, build_public_flag_router, AuthConfig,
    AuthPrincipal, BootstrapCredentials, ControlPlaneService,
};
pub use copilot::{CopilotRequest, CopilotResponse};
pub use db::{MetaError, MetaStore, OrgInvite, SessionInfo, SessionUser};
pub use experiments::{
    build_edge_flag_bundle, build_edge_flag_bundle_with_cohorts, compute_sample_ratio_mismatch,
    compute_statistical_lift_table, evaluate_edge_flag, evaluate_experiment_results,
};
pub use license::{
    evaluate_license_health, sign_license_ed25519, verify_license_key, verify_license_signature,
    CommercialFeature, EditionTier, LicenseClaims, LicenseHealth, LicenseStatusResponse,
    SystemLicenseRecord, LICENSE_GRACE_PERIOD_MS,
};
pub use mcp::{handle_mcp_request, JsonRpcError, JsonRpcRequest, JsonRpcResponse};
pub use models::*;
