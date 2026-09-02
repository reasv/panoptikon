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
    /// POST /api/pinboards/search
    ///
    /// Separate from `pinboards` because that probe is a *write*: a policy
    /// granting read-only board access would report `pinboards: false` while
    /// the library search still works.
    pub pinboard_search: bool,
    /// POST /api/video/transcode
    ///
    /// The write probe of the video surface: a policy may serve already
    /// encoded artifacts (`GET /api/video/artifact`) while denying new
    /// conversions, so this is deliberately not probed off the GET.
    pub video_transcode: bool,
    /// POST /api/video/compose
    ///
    /// Separate from `video_transcode` because the two are separately
    /// rule-able and mean different work: a composition is strictly heavier
    /// (N decoders and their loop buffers at once, holding the pool), so a
    /// policy may allow single-file clips while denying mosaics. The client's
    /// animated-mosaic controls gate on this one.
    pub video_compose: bool,
}

/// The animated raw floor, verbatim from
/// [`crate::visual_tiers`] (docs/grid-scroll-performance-implementation.md
/// §2, step B2).
///
/// A grid cell decides `<img>` vs `<video>` from four fields of its search
/// result — `type` and `duration` say whether the picture moves, `size` and
/// `width`/`height` say whether it clears the floor — against these two
/// numbers, which is the same rule and the same arithmetic the scan used to
/// decide what to store. Surfaced rather than duplicated in the UI so the two
/// sides cannot drift.
///
/// Clearing the floor is necessary but not sufficient: an item above it is
/// served a loop *once the backfill has written one*, and an item whose H.264
/// encode came out no smaller than its source keeps serving the source
/// permanently (the settled keep-the-original edge). A cell that mounts a
/// `<video>` must therefore fall back to its poster when playback errors —
/// the F6 contract in the plan document.
///
/// Server-derived constants, not policy-scoped configuration: every policy
/// sees the same floor, because it is a property of what the scan wrote.
#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct AnimatedThumbnailFloor {
    /// An animated item at or below **both** of these is served as its
    /// original file at every grid tier: no loop is stored for it, so a cell
    /// renders it as an image.
    pub max_file_size: u64,
    /// The longer side, in pixels. Both sides must be within it.
    pub max_side: u32,
}

impl AnimatedThumbnailFloor {
    fn current() -> Self {
        Self {
            max_file_size: crate::visual_tiers::ANIMATED_RAW_MAX_FILE_SIZE,
            max_side: crate::visual_tiers::ANIMATED_RAW_MAX_SIDE,
        }
    }
}

/// The display-tier loop trigger, verbatim from [`crate::visual_tiers`]
/// (docs/thumbnail-format-implementation.md §2, R3).
///
/// The gallery's large view decides `<video>` vs `<img>` from four fields of
/// the item it is showing — `type` and `duration` say whether the picture
/// moves, `size` and `width`/`height` say whether it clears the trigger —
/// against these three numbers, which is the same arithmetic the scan used to
/// decide whether to store a loop at all. Surfaced rather than duplicated in
/// the UI so the two sides cannot drift, and so the client needs no request to
/// find out (a wasted round trip per animated item, and an error latch on the
/// ones that answer with an image).
///
/// Any **one** of the three firing is enough; they are not a conjunction.
#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct DisplayLoopTrigger {
    /// Bytes. An animated original larger than this is answered with a loop.
    pub max_bytes: u64,
    /// The shorter side, in pixels.
    pub max_short_side: u32,
    /// Total pixels.
    pub max_pixels: u64,
}

impl DisplayLoopTrigger {
    fn current() -> Self {
        Self {
            max_bytes: crate::visual_tiers::DISPLAY_MAX_FILE_SIZE_ANIMATED,
            max_short_side: crate::visual_tiers::DISPLAY_MAX_SHORT_SIDE,
            max_pixels: crate::visual_tiers::DISPLAY_MAX_PIXELS,
        }
    }
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
    /// The animated raw floor the thumbnail endpoint serves by (see
    /// [`AnimatedThumbnailFloor`]).
    pub animated_floor: AnimatedThumbnailFloor,
    /// The display-tier loop trigger (see [`DisplayLoopTrigger`]).
    ///
    /// Always `Some` today: the loop ladder is unconditional. The `Option` is
    /// reserved for a build that stores no loops at all, which would publish
    /// `null` here rather than a bound it does not serve by — with nothing to
    /// evaluate, every animated item is an `<img>` on its original file and
    /// no client mounts a `<video>`.
    pub display_loop_trigger: Option<DisplayLoopTrigger>,
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
        pinboard_search: allows(Method::POST, "/api/pinboards/search"),
        video_transcode: allows(Method::POST, "/api/video/transcode"),
        video_compose: allows(Method::POST, "/api/video/compose"),
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
        animated_floor: AnimatedThumbnailFloor::current(),
        // One condition, one place: the trigger is published exactly while
        // the floor is.
        display_loop_trigger: Some(DisplayLoopTrigger::current()),
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
        assert!(!caps.pinboard_search);
        assert!(!caps.video_transcode);
        assert!(!caps.video_compose);
        assert_eq!(
            response.client,
            serde_json::json!({ "search_throttle_ms": 1500, "disable_backend_open": true })
        );
        // Server-derived, not policy-scoped: a restricted policy sees the
        // same floor the desktop one does, because it is a property of what
        // the scan wrote rather than of what this client may do.
        assert_eq!(
            response.animated_floor.max_file_size,
            crate::visual_tiers::ANIMATED_RAW_MAX_FILE_SIZE
        );
        assert_eq!(
            response.animated_floor.max_side,
            crate::visual_tiers::ANIMATED_RAW_MAX_SIDE
        );
    }

    /// The display-tier loop trigger on the wire (§5): three keys, always all
    /// three, carrying the same numbers the scan decided with.
    ///
    /// The gallery large view mounts a `<video>` on the bare thumbnail URL for
    /// an animated item over this trigger and an `<img>` under it, with no
    /// request to find out which — so a client and a server that disagree here
    /// produce an error latch on one class of item and a still frame where a
    /// loop was stored on the other. Server-derived, like the floor beside it:
    /// a restricted policy sees the same numbers, because they are a property
    /// of what the scan wrote.
    #[test]
    fn the_display_loop_trigger_is_published_verbatim() {
        let settings = two_policy_settings();
        let response = build_client_config(&settings, &settings.policies[1]);
        let trigger = response
            .display_loop_trigger
            .as_ref()
            .expect("the loop ladder is unconditional today");
        assert_eq!(
            (
                trigger.max_bytes,
                trigger.max_short_side,
                trigger.max_pixels
            ),
            (
                crate::visual_tiers::DISPLAY_MAX_FILE_SIZE_ANIMATED,
                crate::visual_tiers::DISPLAY_MAX_SHORT_SIDE,
                crate::visual_tiers::DISPLAY_MAX_PIXELS
            )
        );
        // Written out, because these are the numbers the UI hard-codes nothing
        // against and every one of them is a frozen client contract.
        let json = serde_json::to_value(&response).unwrap();
        assert_eq!(
            json["display_loop_trigger"],
            serde_json::json!({
                "max_bytes": 5_242_880u64,
                "max_short_side": 4096u32,
                "max_pixels": 24_000_000u64,
            }),
            "all three keys, always, and never the 2560 rendition cap"
        );

        // And the absent case is a JSON `null`, not a missing key: a build
        // that serves no loops has to *say* so, or a client reading the field
        // as optional would read "no answer" as "no trigger" and mount a
        // `<video>` on every animation there is.
        let json = serde_json::to_value(ClientConfigResponse {
            display_loop_trigger: None,
            ..response
        })
        .unwrap();
        assert_eq!(json["display_loop_trigger"], serde_json::Value::Null);
        assert!(
            json.as_object()
                .unwrap()
                .contains_key("display_loop_trigger")
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
                && caps.pinboard_search
                && caps.video_transcode
                && caps.video_compose
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

    /// A read-only-boards ruleset: browsing and searching the library is
    /// allowed, creating/updating boards is not. `pinboards` (a write probe)
    /// and `pinboard_search` (a read probe) must disagree here — which is the
    /// whole reason the second capability exists.
    #[test]
    fn read_only_boards_splits_the_two_pinboard_capabilities() {
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

[rulesets.read_only_boards]
allow = [
    { methods = ["GET", "POST"], path_prefix = "/api/search/" },
    { methods = ["GET"], path_prefix = "/api/pinboards" },
    { methods = ["POST"], path = "/api/pinboards/search" },
]

[[policies]]
name = "reader"
ruleset = "read_only_boards"

[policies.match]
hosts = ["reader.example.com"]

[policies.index_db]
default = "default"
allow = "*"

[policies.user_data_db]
default = "default"
allow = "*"
"#,
        )
        .unwrap();
        let settings = Settings::load(Some(path)).unwrap();
        let caps = derive_capabilities(&settings, &settings.policies[0]);
        assert!(!caps.pinboards);
        assert!(caps.pinboard_search);
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
