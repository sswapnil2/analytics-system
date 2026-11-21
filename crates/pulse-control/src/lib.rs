pub mod api;
pub mod copilot;
pub mod db;
pub mod experiments;
pub mod mcp;
pub mod models;

pub use api::{
    build_admin_control_router, build_control_router, build_public_flag_router, AuthConfig,
    AuthPrincipal, BootstrapCredentials, ControlPlaneService,
};
pub use copilot::{CopilotRequest, CopilotResponse};
pub use db::{MetaError, MetaStore, OrgInvite, SessionInfo, SessionUser};
pub use experiments::{
    build_edge_flag_bundle, build_edge_flag_bundle_with_cohorts, compare_means, compute_lift_table,
    compute_sample_ratio_mismatch, compute_statistical_lift_table, evaluate_edge_flag,
    evaluate_experiment_results, sequential_rho, Comparison, LiftOptions, Moments,
};
pub use mcp::{handle_mcp_request, JsonRpcError, JsonRpcRequest, JsonRpcResponse};
pub use models::*;
