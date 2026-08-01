//! Typed per-item error slots of the predict protocol.
//!
//! See `docs/inferio-worker-protocol.md` ("Per-item error slots") and
//! `docs/failed-media-retry-design.md` ("Batch isolation and the worker
//! protocol"). An output slot may carry a typed failure *instead of* a
//! payload, so one undecodable input can no longer take its healthy
//! batch-mates down with it — and so the consumer that actually decoded the
//! media (the Python worker, PIL with truncation enabled) is the one that
//! calls it bad.
//!
//! The wire shape is identical on both hops — msgpack worker -> orchestrator
//! and JSON orchestrator -> HTTP client — so the vocabulary lives here once:
//!
//! ```text
//! {"__error__": {"class": "input" | "transient", "message": "<text>"}}
//! ```
//!
//! Compatibility: absence of error slots is exactly today's protocol, and
//! nothing in the gateway emits them unless a worker does. A slot carrying
//! the reserved key but a malformed body is a *protocol violation*, never a
//! payload: guessing there would silently turn a broken worker into an
//! "undecodable file" verdict.

use serde_json::{Value as JsonValue, json};

/// Reserved map key marking an output slot as a typed error. Chosen for the
/// same reason `__type__` was on the base64 wrapper: a dunder key cannot
/// collide with a model's own JSON output in practice.
pub const ERROR_SLOT_KEY: &str = "__error__";

/// The classes an error slot may declare. Deliberately *not* the full ledger
/// taxonomy: a worker can only ever speak about the payload it was handed
/// (`input`) or about a failure of its own that the caller should retry
/// (`transient`). `blocked`/`resource` are gateway-side verdicts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotErrorClass {
    /// The worker's own decoder rejected *this input's* payload.
    Input,
    /// Something else went wrong for this slot; never persisted anywhere.
    Transient,
}

impl SlotErrorClass {
    pub fn as_str(self) -> &'static str {
        match self {
            SlotErrorClass::Input => "input",
            SlotErrorClass::Transient => "transient",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "input" => Some(SlotErrorClass::Input),
            "transient" => Some(SlotErrorClass::Transient),
            _ => None,
        }
    }
}

/// One typed slot failure, aligned with the input at the same position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotError {
    pub class: SlotErrorClass,
    pub message: String,
}

impl SlotError {
    /// The JSON envelope form, as emitted by the HTTP predict response.
    pub fn to_json(&self) -> JsonValue {
        json!({
            ERROR_SLOT_KEY: {
                "class": self.class.as_str(),
                "message": self.message,
            }
        })
    }
}

impl std::fmt::Display for SlotError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} error: {}", self.class.as_str(), self.message)
    }
}

/// The peer answered with a shape the protocol does not define.
///
/// Typed on purpose: a violation is *deterministic*, so callers must be able
/// to tell it apart from an ordinary failure and skip the retries that only
/// make sense for non-deterministic ones (an isolation pass re-submitting the
/// same inputs to the same broken server buys nothing but GPU time).
///
/// The typing does not survive every hop, though. It fires when the client's
/// own response parsing detects the violation (the remote-server case); a
/// violation the *local orchestrator* detects on the msgpack hop kills the
/// worker and surfaces through the HTTP layer as a generic 500, which callers
/// cannot downcast — those take the bounded isolation pass anyway. Best
/// effort, not a cross-hop guarantee.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolViolation {
    pub message: String,
}

impl ProtocolViolation {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for ProtocolViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "inference protocol violation: {}", self.message)
    }
}

impl std::error::Error for ProtocolViolation {}

/// Builds a slot error from the two wire fields, rejecting anything the
/// protocol does not define. Shared by the msgpack (worker) and JSON (HTTP)
/// decoders so both are strict in exactly the same way.
pub fn slot_error_from_parts(
    class: Option<&str>,
    message: Option<&str>,
) -> Result<SlotError, String> {
    let Some(class) = class else {
        return Err(format!("error slot has no string `class`: {ERROR_SLOT_KEY}"));
    };
    let Some(class) = SlotErrorClass::parse(class) else {
        return Err(format!("error slot has an unknown class {class:?}"));
    };
    let Some(message) = message else {
        return Err("error slot has no string `message`".to_owned());
    };
    Ok(SlotError {
        class,
        message: message.to_owned(),
    })
}

/// Reads one JSON output slot. `None` means an ordinary payload;
/// `Some(Err(..))` means the reserved key was present but the body was
/// malformed, which callers must treat as a protocol violation.
pub fn slot_error_from_json(value: &JsonValue) -> Option<Result<SlotError, String>> {
    let body = value.as_object()?.get(ERROR_SLOT_KEY)?;
    let Some(body) = body.as_object() else {
        return Some(Err(format!(
            "`{ERROR_SLOT_KEY}` is not an object: {body}"
        )));
    };
    Some(slot_error_from_parts(
        body.get("class").and_then(JsonValue::as_str),
        body.get("message").and_then(JsonValue::as_str),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    // The persisted/wire strings are a contract with the Python worker:
    // renaming one silently turns every error slot into an unparseable frame.
    #[test]
    fn class_strings_round_trip() {
        for class in [SlotErrorClass::Input, SlotErrorClass::Transient] {
            assert_eq!(SlotErrorClass::parse(class.as_str()), Some(class));
        }
        assert_eq!(SlotErrorClass::parse("blocked"), None);
        assert_eq!(ERROR_SLOT_KEY, "__error__");
    }

    #[test]
    fn json_round_trips_through_the_envelope_form() {
        let error = SlotError {
            class: SlotErrorClass::Input,
            message: "Unreadable image: truncated".to_owned(),
        };
        let encoded = error.to_json();
        assert_eq!(
            encoded,
            json!({"__error__": {"class": "input", "message": "Unreadable image: truncated"}})
        );
        assert_eq!(slot_error_from_json(&encoded), Some(Ok(error)));
    }

    // An ordinary payload must never be mistaken for an error slot — not a
    // string, not a list, and not an object without the reserved key.
    #[test]
    fn ordinary_payloads_are_not_error_slots() {
        for value in [
            json!("text"),
            json!([1, 2, 3]),
            json!({"transcription": "hello", "confidence": 1.0}),
            json!({"__type__": "base64", "content": "AAA="}),
            json!(null),
        ] {
            assert_eq!(slot_error_from_json(&value), None, "{value}");
        }
    }

    // Present-but-malformed is a protocol violation, never a payload and
    // never a guessed class: a broken worker must not be able to fabricate an
    // "undecodable media" verdict by accident.
    #[test]
    fn a_malformed_error_slot_is_rejected() {
        for value in [
            json!({"__error__": "boom"}),
            json!({"__error__": {"message": "no class"}}),
            json!({"__error__": {"class": "blocked", "message": "not ours"}}),
            json!({"__error__": {"class": "input"}}),
            json!({"__error__": {"class": "input", "message": 7}}),
        ] {
            assert!(
                matches!(slot_error_from_json(&value), Some(Err(_))),
                "{value} must be rejected"
            );
        }
    }
}
