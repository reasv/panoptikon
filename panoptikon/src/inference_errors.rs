//! User-facing messages for the three ways a request to the inference server
//! fails before any model sees it: the server's policy refused it (403), the
//! TLS handshake failed, or the server could not be reached. Other inference
//! failures keep the messages of their call sites.

use axum::http::StatusCode;
use hyper_tls::native_tls;

use crate::api_error::ApiError;
use crate::inferio_client::inference_failure;

/// The README section that explains the server-side setup.
const REMOTE_INFERENCE_DOC: &str = "\"Remote inference\" in the README";

/// Context naming the inference endpoint an error came from, for callers
/// that talk to more than one (the job pool).
#[derive(Debug, Clone)]
pub(crate) struct InferenceEndpoint(pub String);

impl std::fmt::Display for InferenceEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "inference endpoint {}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum UpstreamFailure {
    /// The server answered 403.
    Refused,
    /// The TLS handshake failed; carries the TLS library's reason.
    Tls(String),
    /// No connection or no answer: refused, DNS, timeout. Carries the
    /// innermost cause.
    Unreachable { reason: String, timed_out: bool },
}

impl UpstreamFailure {
    /// Classifies an inference client error; `None` for every other failure.
    pub(crate) fn classify(err: &anyhow::Error) -> Option<Self> {
        if inference_failure(err).is_some_and(|failure| failure.status == 403) {
            return Some(Self::Refused);
        }
        if let Some(tls) = err
            .chain()
            .find_map(|cause| cause.downcast_ref::<native_tls::Error>())
        {
            return Some(Self::Tls(tls.to_string()));
        }
        let transport = err
            .chain()
            .find_map(|cause| cause.downcast_ref::<reqwest::Error>())?;
        if !transport.is_connect() && !transport.is_timeout() {
            return None;
        }
        let reason = err.chain().last().map(ToString::to_string)?;
        Some(Self::Unreachable {
            reason,
            timed_out: transport.is_timeout(),
        })
    }

    pub(crate) fn message(&self, base_url: &str) -> String {
        match self {
            Self::Refused => format!(
                "The inference server at {base_url} refused the request (403): check its \
                 host/endpoint policy; see {REMOTE_INFERENCE_DOC}"
            ),
            Self::Tls(reason) => format!(
                "TLS handshake with {base_url} failed: {reason}; for a private CA set \
                 SSL_CERT_FILE"
            ),
            Self::Unreachable { reason, .. } => {
                format!("Could not reach the inference server at {base_url}: {reason}")
            }
        }
    }

    /// 504 for a timeout, 502 for everything else: the failure is upstream.
    pub(crate) fn status(&self) -> StatusCode {
        match self {
            Self::Unreachable {
                timed_out: true, ..
            } => StatusCode::GATEWAY_TIMEOUT,
            _ => StatusCode::BAD_GATEWAY,
        }
    }

    pub(crate) fn api_error(&self, base_url: &str) -> ApiError {
        ApiError::new(self.status(), self.message(base_url))
    }
}

/// The endpoint named by an [`InferenceEndpoint`] context on `err`, else
/// `base_url`.
fn endpoint_url<'a>(err: &'a anyhow::Error, base_url: &'a str) -> &'a str {
    err.downcast_ref::<InferenceEndpoint>()
        .map_or(base_url, |endpoint| endpoint.0.as_str())
}

/// The message for `err` if it is one of the three failures.
pub(crate) fn upstream_message(err: &anyhow::Error, base_url: &str) -> Option<String> {
    UpstreamFailure::classify(err).map(|failure| failure.message(endpoint_url(err, base_url)))
}

/// The API error for `err` if it is one of the three failures, else
/// `fallback()`.
pub(crate) fn upstream_api_error(
    err: &anyhow::Error,
    base_url: &str,
    fallback: impl FnOnce() -> ApiError,
) -> ApiError {
    match UpstreamFailure::classify(err) {
        Some(failure) => failure.api_error(endpoint_url(err, base_url)),
        None => fallback(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inferio_client::InferenceApiClient;
    use axum::{Router, routing::get};

    async fn serve(app: Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{addr}")
    }

    fn client(base_url: &str) -> InferenceApiClient {
        InferenceApiClient::new_with_metadata_cache(base_url, false).unwrap()
    }

    #[tokio::test]
    async fn a_policy_refusal_is_a_502_naming_the_policy() {
        let url = serve(Router::new().route(
            "/api/inference/metadata",
            get(|| async { StatusCode::FORBIDDEN }),
        ))
        .await;
        let err = client(&url).get_metadata().await.unwrap_err();
        assert_eq!(
            UpstreamFailure::classify(&err),
            Some(UpstreamFailure::Refused)
        );
        let api = UpstreamFailure::Refused.api_error(&url);
        assert_eq!(api.detail(), UpstreamFailure::Refused.message(&url));
        assert!(
            api.detail()
                .contains(&format!("at {url} refused the request (403)"))
        );
        assert!(api.detail().contains("\"Remote inference\""));
        assert_eq!(UpstreamFailure::Refused.status(), StatusCode::BAD_GATEWAY);
    }

    #[tokio::test]
    async fn other_server_answers_are_not_classified() {
        let url = serve(Router::new().route(
            "/api/inference/metadata",
            get(|| async { StatusCode::NOT_FOUND }),
        ))
        .await;
        let err = client(&url).get_metadata().await.unwrap_err();
        assert_eq!(UpstreamFailure::classify(&err), None);
        let api = upstream_api_error(&err, &url, || ApiError::internal("kept"));
        assert_eq!(api.detail(), "kept");
    }

    #[tokio::test]
    async fn a_closed_port_is_unreachable_with_the_os_reason() {
        let dead = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", dead.local_addr().unwrap());
        drop(dead);
        let err = client(&url).get_metadata().await.unwrap_err();
        let Some(UpstreamFailure::Unreachable { reason, timed_out }) =
            UpstreamFailure::classify(&err)
        else {
            panic!("not unreachable: {err:#}");
        };
        assert!(!timed_out);
        assert!(
            reason.to_lowercase().contains("refused"),
            "reason: {reason}"
        );
        let message = upstream_message(&err, &url).unwrap();
        assert_eq!(
            message,
            format!("Could not reach the inference server at {url}: {reason}")
        );
    }

    /// A peer that answers the ClientHello with plain HTTP fails the
    /// handshake inside the TLS library, like an untrusted certificate does.
    #[tokio::test]
    async fn a_failed_handshake_is_a_tls_failure() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            while let Ok((mut socket, _)) = listener.accept().await {
                let mut buf = [0u8; 1024];
                let _ = socket.read(&mut buf).await;
                let _ = socket
                    .write_all(b"HTTP/1.1 400 Bad Request\r\ncontent-length: 0\r\n\r\n")
                    .await;
            }
        });
        let url = format!("https://{addr}");
        let err = client(&url).get_metadata().await.unwrap_err();
        let Some(UpstreamFailure::Tls(reason)) = UpstreamFailure::classify(&err) else {
            panic!("not a TLS failure: {err:#}");
        };
        assert!(!reason.is_empty());
        let message = upstream_message(&err, &url).unwrap();
        assert_eq!(
            message,
            format!(
                "TLS handshake with {url} failed: {reason}; for a private CA set SSL_CERT_FILE"
            )
        );
        assert_eq!(
            UpstreamFailure::Tls(reason).status(),
            StatusCode::BAD_GATEWAY
        );
    }

    #[test]
    fn a_timeout_is_a_504() {
        let failure = UpstreamFailure::Unreachable {
            reason: "operation timed out".into(),
            timed_out: true,
        };
        assert_eq!(failure.status(), StatusCode::GATEWAY_TIMEOUT);
    }

    #[test]
    fn an_endpoint_context_names_that_endpoint() {
        let failure = crate::inferio_client::InferenceFailure::parse(
            reqwest::StatusCode::FORBIDDEN,
            None,
            "",
        );
        let err = anyhow::Error::new(failure)
            .context(InferenceEndpoint("http://gpu-b:7777".into()))
            .context("model x failed to load on all 2 inference endpoints");
        let message = upstream_message(&err, "http://gpu-a:7777").unwrap();
        assert!(
            message.contains("at http://gpu-b:7777 refused"),
            "{message}"
        );
    }
}
