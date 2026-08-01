use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;

/// An external dependency whose absence blocks a pipeline stage
/// (docs/failed-media-retry-design.md). The string form is persisted in the
/// extraction/scan ledgers and appears in logs, so it is stable data rather
/// than a display detail.
/// `Ord` so a job can aggregate the distinct blockers it hit in a `BTreeSet`
/// and report them in a stable order; the ordering itself carries no meaning.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Blocker {
    Pdfium,
    HtmlRenderer,
    Ffmpeg,
}

impl Blocker {
    pub fn as_str(self) -> &'static str {
        match self {
            Blocker::Pdfium => "pdfium",
            Blocker::HtmlRenderer => "html-renderer",
            Blocker::Ffmpeg => "ffmpeg",
        }
    }

    /// Parses the persisted form back. Used by the auto-heal probe, which
    /// reads the distinct blockers out of the ledger and only resolves those.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "pdfium" => Some(Blocker::Pdfium),
            "html-renderer" => Some(Blocker::HtmlRenderer),
            "ffmpeg" => Some(Blocker::Ffmpeg),
            _ => None,
        }
    }
}

impl std::fmt::Display for Blocker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The failure taxonomy of docs/failed-media-retry-design.md.
///
/// `Generic` is the default every existing construction site keeps, and it is
/// what the job pipeline treats as *transient* (I/O, worker crash, inference
/// server down, DB busy): counted, never persisted, retried next run. The
/// three explicit classes are the ones the ledgers store; unclassified always
/// means transient, so the pipeline can never be stricter than the consumer
/// that produced the verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ApiErrorKind {
    #[default]
    Generic,
    Input,
    Blocked {
        blocker: Blocker,
    },
    /// Reserved for the classified batch-1 OOM path of the GPU-compat work
    /// (the worker reports it as a plain message today); the ledger and the
    /// audit surface already carry the class.
    #[allow(dead_code)]
    Resource,
}

impl ApiErrorKind {
    /// The `error_class` value stored in the ledgers, or `None` for the
    /// transient class, which is never persisted.
    pub fn persisted_class(self) -> Option<&'static str> {
        match self {
            ApiErrorKind::Generic => None,
            ApiErrorKind::Input => Some("input"),
            ApiErrorKind::Blocked { .. } => Some("blocked"),
            ApiErrorKind::Resource => Some("resource"),
        }
    }

    pub fn blocker(self) -> Option<Blocker> {
        match self {
            ApiErrorKind::Blocked { blocker } => Some(blocker),
            _ => None,
        }
    }
}

/// Confirmation threshold for a deterministic verdict: the failing decode ran
/// on bytes the gateway had already read successfully, so one attempt settles
/// it.
pub const SKIP_AFTER_CONFIRMED: i64 = 1;

/// Confirmation threshold for an ambiguous verdict: the failing tool did its
/// own file I/O (ffmpeg/ffprobe/pdfium/the HTML renderer), where a corrupt
/// file and a transient mount hiccup surface identically. Such a row needs a
/// second failure, in a *later* run, before it suppresses anything.
pub const SKIP_AFTER_AMBIGUOUS: i64 = 2;

#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    detail: String,
    kind: ApiErrorKind,
    /// Only meaningful for the persisted classes; see the two constants
    /// above. Carried on the error rather than derived from `kind` because
    /// the threshold is a property of the *classification site*, not of the
    /// class: an `input` verdict from an in-memory decode is confirmed at 1,
    /// the same class from an external tool's own read is not.
    skip_after: i64,
}

impl ApiError {
    pub fn new(status: StatusCode, detail: impl Into<String>) -> Self {
        Self {
            status,
            detail: detail.into(),
            kind: ApiErrorKind::Generic,
            skip_after: SKIP_AFTER_CONFIRMED,
        }
    }

    pub fn bad_request(detail: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, detail)
    }

    pub fn not_found(detail: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, detail)
    }

    pub fn forbidden(detail: impl Into<String>) -> Self {
        Self::new(StatusCode::FORBIDDEN, detail)
    }

    pub fn internal(detail: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, detail)
    }
}

/// The classified half of the constructor set, kept in its own block so the
/// `allow` covering the not-yet-wired phases cannot mask a genuinely dead
/// constructor above.
impl ApiError {
    /// The pipeline's own decoder or tool rejected the payload. Defaults to
    /// the confirmed threshold, which is right for a decode of bytes the
    /// gateway already read; an external tool that did its own I/O must say
    /// so with [`ApiError::input_unconfirmed`] (or `with_skip_after`).
    pub fn input(detail: impl Into<String>) -> Self {
        Self::classified(ApiErrorKind::Input, SKIP_AFTER_CONFIRMED, detail)
    }

    /// An `input` verdict from a stage that read the file itself, so a single
    /// failure does not settle it. See [`SKIP_AFTER_AMBIGUOUS`].
    pub fn input_unconfirmed(detail: impl Into<String>) -> Self {
        Self::classified(ApiErrorKind::Input, SKIP_AFTER_AMBIGUOUS, detail)
    }

    /// A required external dependency is not installed. Spawn failures
    /// (`ENOENT` on ffmpeg and friends) are this, never `input`.
    pub fn blocked(blocker: Blocker, detail: impl Into<String>) -> Self {
        Self::classified(
            ApiErrorKind::Blocked { blocker },
            SKIP_AFTER_CONFIRMED,
            detail,
        )
    }

    /// The item individually exceeds a resource limit on *this machine* — a
    /// decode memory ceiling, a classified batch-1 OOM. A verdict on the
    /// budget rather than the payload, so it must never be `input`: it is
    /// clearable by a retry directive after the ceiling is raised, not by
    /// calling the file corrupt.
    pub fn resource(detail: impl Into<String>) -> Self {
        Self::classified(ApiErrorKind::Resource, SKIP_AFTER_CONFIRMED, detail)
    }

    pub fn kind(&self) -> ApiErrorKind {
        self.kind
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }

    pub fn skip_after(&self) -> i64 {
        self.skip_after
    }

    pub fn blocker(&self) -> Option<Blocker> {
        self.kind.blocker()
    }

    /// The `error_class` to persist, or `None` when this failure is transient
    /// and must not be recorded at all.
    pub fn persisted_class(&self) -> Option<&'static str> {
        self.kind.persisted_class()
    }

    /// Classified failures all surface as 500s: they are produced inside the
    /// job pipeline, never by a request handler, and the taxonomy rides
    /// alongside the status rather than changing it.
    fn classified(kind: ApiErrorKind, skip_after: i64, detail: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            detail: detail.into(),
            kind,
            skip_after,
        }
    }
}

/// The one classification tool no site needs yet: `with_skip_after`, for a
/// site that needs a threshold its constructor does not imply. (`resource` is
/// wired by the extraction-side image classifiers; the batch-1 OOM path still
/// leaves it unclassified on the dispatch side.) Kept behind its own `allow`
/// so the wired constructors above stay lint-covered.
#[allow(dead_code)]
impl ApiError {
    /// Overrides the confirmation threshold of an already-classified error.
    /// A threshold on a transient error is a caller bug: nothing persists it,
    /// so the tuning silently does nothing.
    pub fn with_skip_after(mut self, skip_after: i64) -> Self {
        debug_assert!(
            self.kind.persisted_class().is_some(),
            "skip_after is meaningless on a transient error"
        );
        self.skip_after = skip_after;
        self
    }
}

/// The single error body shape every gateway error path serializes.
/// (Unlike FastAPI there is no structured 422 validation body: axum
/// extractor rejections and all `ApiError`s use this `detail` string.)
#[derive(Serialize, utoipa::ToSchema)]
pub(crate) struct ErrorBody {
    detail: String,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = Json(ErrorBody {
            detail: self.detail,
        });
        (self.status, body).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;

    // The existing constructors must stay transient: everything that predates
    // the taxonomy is systemic by definition, and a `Generic` error is never
    // written to a ledger.
    #[test]
    fn plain_constructors_are_transient() {
        for error in [
            ApiError::bad_request("bad"),
            ApiError::not_found("gone"),
            ApiError::forbidden("no"),
            ApiError::internal("boom"),
            ApiError::new(StatusCode::CONFLICT, "conflict"),
        ] {
            assert_eq!(error.kind(), ApiErrorKind::Generic);
            assert_eq!(error.persisted_class(), None);
            assert_eq!(error.blocker(), None);
        }
    }

    #[test]
    fn classified_constructors_carry_class_and_threshold() {
        let input = ApiError::input("truncated jpeg");
        assert_eq!(input.kind(), ApiErrorKind::Input);
        assert_eq!(input.persisted_class(), Some("input"));
        assert_eq!(input.skip_after(), SKIP_AFTER_CONFIRMED);
        assert_eq!(input.detail(), "truncated jpeg");

        // The threshold is a property of the classification site, not the
        // class: the same `input` verdict from a tool that read the file
        // itself needs a second failure before it counts.
        let ambiguous = ApiError::input_unconfirmed("ffmpeg exit 1");
        assert_eq!(ambiguous.kind(), ApiErrorKind::Input);
        assert_eq!(ambiguous.skip_after(), SKIP_AFTER_AMBIGUOUS);
        assert_eq!(
            ApiError::input("x")
                .with_skip_after(SKIP_AFTER_AMBIGUOUS)
                .skip_after(),
            SKIP_AFTER_AMBIGUOUS
        );

        let blocked = ApiError::blocked(Blocker::Pdfium, "pdfium unavailable");
        assert_eq!(
            blocked.kind(),
            ApiErrorKind::Blocked {
                blocker: Blocker::Pdfium
            }
        );
        assert_eq!(blocked.persisted_class(), Some("blocked"));
        assert_eq!(blocked.blocker(), Some(Blocker::Pdfium));
        assert_eq!(blocked.skip_after(), SKIP_AFTER_CONFIRMED);

        let resource = ApiError::resource("batch-1 OOM");
        assert_eq!(resource.kind(), ApiErrorKind::Resource);
        assert_eq!(resource.persisted_class(), Some("resource"));
        assert_eq!(resource.blocker(), None);
    }

    // The persisted strings are database values: renaming one silently
    // orphans every existing ledger row and every shipped retry directive.
    #[test]
    fn persisted_strings_are_stable_and_round_trip() {
        assert_eq!(Blocker::Pdfium.as_str(), "pdfium");
        assert_eq!(Blocker::HtmlRenderer.as_str(), "html-renderer");
        assert_eq!(Blocker::Ffmpeg.as_str(), "ffmpeg");
        for blocker in [Blocker::Pdfium, Blocker::HtmlRenderer, Blocker::Ffmpeg] {
            assert_eq!(Blocker::parse(blocker.as_str()), Some(blocker));
            assert_eq!(blocker.to_string(), blocker.as_str());
        }
        assert_eq!(Blocker::parse("imagemagick"), None);
        assert_eq!(ApiErrorKind::default(), ApiErrorKind::Generic);
    }

    // The taxonomy rides alongside the HTTP behaviour and must not change it:
    // status and body shape stay exactly what every handler already returns.
    #[tokio::test]
    async fn classification_does_not_change_the_response() {
        let response = ApiError::input("truncated jpeg").into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&body[..], br#"{"detail":"truncated jpeg"}"#);

        let response = ApiError::not_found("gone").into_response();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&body[..], br#"{"detail":"gone"}"#);
    }
}
