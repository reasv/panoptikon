//! `GET /api/client-config`: what may this client do, and how should it
//! behave? Always answerable regardless of ruleset (the policy layer
//! exempts it — a client must be able to ask what it may do), so
//! restricted UIs can discover which controls to hide instead of watching
//! requests 403.

use axum::http::{Method, header};
use axum::{Extension, Json, extract::State, response::IntoResponse};
use serde::Serialize;
use std::sync::Arc;
use utoipa::ToSchema;

use crate::api_error::ApiError;
use crate::config::{PolicyConfig, Settings};
use crate::policy::{PolicyContext, ruleset_allows};
use crate::proxy::ProxyState;

/// Coarse feature switches derived from the matched policy's ruleset. Each
/// capability is one representative probe from the real route list in
/// main.rs, evaluated with the exact rule-matching code enforcement uses
/// (`policy::ruleset_allows`) — true means the probe request would pass the
/// ruleset gate.
#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct ClientCapabilities {
    /// POST /api/search/pql
    pub search: bool,
    /// GET /api/items/item
    pub items: bool,
    /// PUT /api/bookmarks/ns/{namespace}/{sha256}
    pub bookmarks: bool,
    /// POST /api/jobs/folders/rescan
    pub scan_jobs: bool,
    /// POST /api/open/file/{sha256}
    pub open_files: bool,
    /// POST /api/db/create
    pub db_create: bool,
    /// POST /api/inference/predict/{group}/{inference_id}
    pub inference: bool,
    /// POST /api/pinboards
    pub pinboards: bool,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct ClientConfigResponse {
    /// Name of the policy that matched this request.
    pub policy: String,
    /// Ruleset-derived feature switches (see ClientCapabilities).
    pub capabilities: ClientCapabilities,
    /// The policy's `[policies.client]` table, verbatim (empty object when
    /// unset). Free-form; recognized-by-convention keys include
    /// `search_throttle_ms`, `disable_backend_open`, and `relay_enabled`
    /// (Relay is enabled when the key is absent).
    pub client: serde_json::Value,
    /// True only when this Server process is the bundled sidecar owned by
    /// Panoptikon Desktop and the matched policy opts into Desktop authority.
    pub desktop_managed: bool,
    /// True only for a policy explicitly marked as the local Desktop client
    /// while the private parent-shell bridge is configured.
    pub desktop_shell_available: bool,
}

/// The probe table: (capability, method, representative real route). Paths
/// with placeholders use plausible concrete values — rule matching is
/// path/path_prefix based, so any concrete instance of the route behaves
/// identically.
fn derive_capabilities(settings: &Settings, policy: &PolicyConfig) -> ClientCapabilities {
    let allows = |method: Method, path: &str| ruleset_allows(settings, policy, &method, path);
    ClientCapabilities {
        search: allows(Method::POST, "/api/search/pql"),
        items: allows(Method::GET, "/api/items/item"),
        bookmarks: allows(Method::PUT, "/api/bookmarks/ns/default/probe"),
        scan_jobs: allows(Method::POST, "/api/jobs/folders/rescan"),
        open_files: allows(Method::POST, "/api/open/file/probe"),
        db_create: allows(Method::POST, "/api/db/create"),
        inference: allows(Method::POST, "/api/inference/predict/group/probe"),
        pinboards: allows(Method::POST, "/api/pinboards"),
    }
}

fn desktop_shell_available(policy: &PolicyConfig, managed: bool, bridge_configured: bool) -> bool {
    desktop_managed_for_policy(policy, managed) && bridge_configured
}

fn desktop_managed_for_policy(policy: &PolicyConfig, managed: bool) -> bool {
    managed
        && policy
            .client
            .get("desktop")
            .and_then(serde_json::Value::as_bool)
            == Some(true)
}

pub(crate) fn build_client_config(
    settings: &Settings,
    policy: &PolicyConfig,
) -> ClientConfigResponse {
    ClientConfigResponse {
        policy: policy.name.clone(),
        capabilities: derive_capabilities(settings, policy),
        client: policy.client.clone(),
        desktop_managed: desktop_managed_for_policy(policy, crate::desktop::is_managed()),
        desktop_shell_available: desktop_shell_available(
            policy,
            crate::desktop::is_managed(),
            crate::api::desktop::desktop_bridge_is_configured(),
        ),
    }
}

#[utoipa::path(
    get,
    operation_id = "client_config",
    path = "/api/client-config",
    tag = "client",
    summary = "Get the matched policy's client configuration and capabilities",
    description = "Returns the name of the policy that matched this request, coarse capability \
booleans derived from the policy's ruleset (which controls to show), and the policy's free-form \
`[policies.client]` table verbatim. Always allowed regardless of ruleset restrictions.",
    responses(
        (status = 200, description = "Client configuration for the matched policy", body = ClientConfigResponse)
    )
)]
pub async fn client_config(
    State(state): State<Arc<ProxyState>>,
    Extension(context): Extension<PolicyContext>,
) -> Result<impl IntoResponse, ApiError> {
    let settings = &state.settings;
    let policy = settings
        .policies
        .iter()
        .find(|policy| policy.name == context.policy_name)
        .ok_or_else(|| {
            // Unreachable in practice: the policy layer selected this name
            // out of the same settings moments ago.
            tracing::error!(policy = %context.policy_name, "matched policy missing from config");
            ApiError::internal("matched policy missing from configuration")
        })?;
    Ok((
        // The response is policy-scoped: a shared/intermediary cache keyed
        // on the path alone could serve one audience's capabilities to
        // another, so it must never be stored.
        [(header::CACHE_CONTROL, "no-store")],
        Json(build_client_config(settings, policy)),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Settings with the shipped-config policy shapes: an allow_all
    /// "desktop" policy with a [policies.client] table, and a "demo" policy
    /// on the restricted_demo ruleset copied verbatim from
    /// config/server/default.toml.
    fn two_policy_settings() -> Settings {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("gw.toml");
        std::fs::write(
            &path,
            r#"
[server]
host = "127.0.0.1"
port = 9155

[upstreams.ui]
base_url = "http://127.0.0.1:6339"

[upstreams.api]
base_url = "http://127.0.0.1:6342"

[rulesets.allow_all]
allow_all = true

[rulesets.restricted_demo]
allow = [
    { methods = ["GET"], path_prefix = "/docs" },
    { methods = ["GET"], path = "/openapi.json" },
    { methods = ["GET", "POST"], path_prefix = "/api/search/" },
    { methods = ["GET"], path_prefix = "/api/items/" },
    { methods = ["GET", "POST", "DELETE", "PUT"], path_prefix = "/api/bookmarks/" },
    { methods = ["GET"], path = "/api/db" },
    { methods = ["GET"], path = "/api/inference/cache" },
]

[[policies]]
name = "desktop"
ruleset = "allow_all"

[policies.match]
hosts = ["localhost"]

[policies.index_db]
default = "default"
allow = "*"

[policies.user_data_db]
default = "default"
allow = "*"

[policies.client]
search_throttle_ms = 100

[[policies]]
name = "demo"
ruleset = "restricted_demo"

[policies.match]
hosts = ["demo.example.com"]

[policies.index_db]
default = "public"
allow = ["public"]

[policies.user_data_db]
default = "default"
allow = ["default"]

[policies.client]
search_throttle_ms = 1500
disable_backend_open = true
"#,
        )
        .unwrap();
        Settings::load(Some(path)).unwrap()
    }

    /// Capability derivation against the shipped restricted_demo ruleset:
    /// search/items/bookmarks stay usable, everything with side effects on
    /// the host (jobs, open, db create, inference, pinboard writes) is off.
    /// The response carries the [policies.client] table verbatim.
    #[test]
    fn restricted_demo_capabilities() {
        let settings = two_policy_settings();
        let policy = &settings.policies[1];
        assert_eq!(policy.name, "demo");
        let response = build_client_config(&settings, policy);

        assert_eq!(response.policy, "demo");
        let caps = &response.capabilities;
        assert!(caps.search);
        assert!(caps.items);
        assert!(caps.bookmarks);
        assert!(!caps.scan_jobs);
        assert!(!caps.open_files);
        assert!(!caps.db_create);
        assert!(!caps.inference);
        assert!(!caps.pinboards);
        assert_eq!(
            response.client,
            serde_json::json!({ "search_throttle_ms": 1500, "disable_backend_open": true })
        );
    }

    /// allow_all: everything true, client table passed through.
    #[test]
    fn allow_all_capabilities() {
        let settings = two_policy_settings();
        let policy = &settings.policies[0];
        assert_eq!(policy.name, "desktop");
        let response = build_client_config(&settings, policy);

        assert_eq!(response.policy, "desktop");
        let caps = &response.capabilities;
        assert!(
            caps.search
                && caps.items
                && caps.bookmarks
                && caps.scan_jobs
                && caps.open_files
                && caps.db_create
                && caps.inference
                && caps.pinboards
        );
        assert_eq!(
            response.client,
            serde_json::json!({ "search_throttle_ms": 100 })
        );
    }

    /// The handler responds with Cache-Control: no-store (the body is
    /// policy-scoped, so intermediaries must never cache it) and reports
    /// the policy from the request's PolicyContext.
    #[tokio::test]
    async fn handler_sets_no_store_and_uses_matched_policy() {
        let settings = Arc::new(two_policy_settings());
        let upstream = crate::proxy::Upstream::parse("api", "http://127.0.0.1:1").unwrap();
        let client = crate::inferio_client::InferenceApiClient::new_with_metadata_cache(
            "http://127.0.0.1:1".to_string(),
            false,
        )
        .unwrap();
        let state = Arc::new(ProxyState::new(
            upstream.clone(),
            upstream.clone(),
            upstream,
            client,
            0,
            Arc::clone(&settings),
            Arc::new(crate::policy_token::TokenKey::random()),
            tokio::sync::watch::channel(false).1,
        ));
        let context = PolicyContext {
            policy_name: "demo".to_string(),
            db_action: crate::policy::DbAction::Skipped,
            selected_by: crate::policy::PolicySelection::ListenerHost,
            search_cache: true,
        };

        let response = client_config(State(state), Extension(context))
            .await
            .map(IntoResponse::into_response)
            .unwrap_or_else(|_| panic!("handler must succeed"));
        assert_eq!(
            response
                .headers()
                .get(header::CACHE_CONTROL)
                .and_then(|value| value.to_str().ok()),
            Some("no-store")
        );
        let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["policy"], "demo");
        assert_eq!(json["capabilities"]["scan_jobs"], false);
        assert_eq!(json["client"]["search_throttle_ms"], 1500);
    }

    /// A policy with no ruleset at all (unrestricted) also yields all-true.
    #[test]
    fn no_ruleset_means_all_capabilities() {
        let settings = two_policy_settings();
        let mut policy = settings.policies[0].clone();
        policy.ruleset = None;
        let caps = derive_capabilities(&settings, &policy);
        assert!(caps.search && caps.scan_jobs && caps.db_create && caps.inference);
    }

    #[test]
    fn desktop_bridge_requires_management_configuration_and_policy_opt_in() {
        let settings = two_policy_settings();
        let mut desktop = settings.policies[0].clone();
        desktop.client["desktop"] = serde_json::Value::Bool(true);
        assert!(desktop_shell_available(&desktop, true, true));
        assert!(!desktop_shell_available(&desktop, false, true));
        assert!(!desktop_shell_available(&desktop, true, false));
        assert!(!desktop_shell_available(&settings.policies[1], true, true));
        assert!(desktop_managed_for_policy(&desktop, true));
        assert!(!desktop_managed_for_policy(&desktop, false));
        assert!(!desktop_managed_for_policy(&settings.policies[1], true));
    }
}
