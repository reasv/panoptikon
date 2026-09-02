//! Policy tokens: HMAC-signed, short-lived policy selectors for SSR.
//!
//! The gateway injects
//! `x-panoptikon-policy: <policy>.<expiry>.<origin_b64url>.<hmac_hex>`
//! into every request it proxies to the UI upstream, naming the policy the
//! policy layer matched for that request and the loopback origin of the
//! listener it arrived on. When the Next.js server renders a page it echoes
//! the token on its own API calls back to the gateway, and the policy layer
//! selects the named policy instead of matching by listener/host — so
//! server-side rendering acts with the authority of the browser request
//! that triggered it, not with the authority of the UI server's own
//! network position.
//!
//! The origin claim exists for the UI, not for the gateway. A Next.js
//! server this gateway launched is told where to send its SSR API calls
//! through `PANOPTIKON_API_URL`; one it did NOT launch (`[upstreams.ui]
//! local = false` with a hand-run `next start`) has only a compiled-in
//! default port, which is right for exactly one gateway on the machine and
//! silently wrong for every other — that is how a scratch gateway's pages
//! once rendered another instance's library. The claim tells such a server
//! which gateway is actually in front of it. Precedence on the UI side is
//! env first: `PANOPTIKON_API_URL` always wins when set, and the claim is
//! honored only when it is a plain-http loopback origin. The gateway never
//! routes or selects on the claim; it is inside the signed message only so
//! a token cannot be re-pointed without breaking its HMAC. A forged token
//! fails verification on whichever listener it reaches and that listener's
//! ordinary fallback applies, exactly as for any other invalid token.
//!
//! Threat model: the UI process holds no authority of its own — the token
//! is minted per request and expires after [`TOKEN_TTL_SECS`]. A forged,
//! tampered, expired, or absent token is ignored and selection falls back
//! to listener/host matching, so deployments point the SSR at a listener
//! whose policy is the most restricted one.
//!
//! The key is random per gateway boot by default. `[server]
//! policy_token_key` (hex, 32 bytes) pins it for the niche multi-gateway
//! setup where one gateway's UI upstream is reached through another.

use anyhow::Context;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use hmac::{Hmac, Mac};
use rand::RngCore;
use sha2::Sha256;

use crate::config::Settings;

type HmacSha256 = Hmac<Sha256>;

/// Header carrying the policy token. Injected on UI-bound proxied requests,
/// verified-then-consumed at policy-layer ingress (never forwarded).
pub(crate) const POLICY_TOKEN_HEADER: &str = "x-panoptikon-policy";

/// Token lifetime. Long enough for a slow SSR render to finish its API
/// fan-out, short enough that a leaked token is useless quickly.
const TOKEN_TTL_SECS: u64 = 300;

/// Why a presented token was ignored (logged at debug; selection then
/// falls back to listener/host matching).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TokenError {
    /// Not `<name>.<expiry>.<hmac_hex>` (or non-decodable pieces).
    Malformed,
    /// Structure fine, HMAC does not verify under our key.
    BadHmac,
    /// HMAC fine, expiry in the past.
    Expired,
}

impl TokenError {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            TokenError::Malformed => "malformed",
            TokenError::BadHmac => "bad-hmac",
            TokenError::Expired => "expired",
        }
    }
}

/// What a verified token asserts: the policy the gateway matched for the
/// browser request, and the loopback base URL of the listener that request
/// arrived on (e.g. `http://127.0.0.1:6342`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TokenClaims<'a> {
    pub(crate) policy: &'a str,
    pub(crate) origin: String,
}

/// The in-memory HMAC key for policy tokens. Deliberately no Debug/Display:
/// the key must never end up in logs.
pub struct TokenKey([u8; 32]);

impl TokenKey {
    /// The configured `[server] policy_token_key` (hex, exactly 32 bytes)
    /// or a fresh random per-boot key when unset.
    pub fn from_settings(settings: &Settings) -> anyhow::Result<Self> {
        match settings.server.policy_token_key.as_deref() {
            Some(hex_key) => {
                let bytes = hex::decode(hex_key.trim())
                    .context("server.policy_token_key is not valid hex")?;
                let key: [u8; 32] = bytes.try_into().map_err(|bytes: Vec<u8>| {
                    anyhow::anyhow!(
                        "server.policy_token_key must be 32 bytes (64 hex chars), got {}",
                        bytes.len()
                    )
                })?;
                Ok(Self(key))
            }
            None => Ok(Self::random()),
        }
    }

    /// A fresh random 256-bit key (per gateway boot). ThreadRng is a
    /// CSPRNG seeded from OS entropy.
    pub fn random() -> Self {
        let mut key = [0u8; 32];
        rand::rng().fill_bytes(&mut key);
        Self(key)
    }

    /// Mint `<policy>.<expiry>.<origin_b64url>.<hmac_hex>` expiring
    /// [`TOKEN_TTL_SECS`] from now. `origin` is the loopback base URL of the
    /// listener the request arrived on (module docs).
    pub(crate) fn mint(&self, policy_name: &str, origin: &str) -> String {
        self.sign(policy_name, origin, unix_now() + TOKEN_TTL_SECS)
    }

    /// Sign with an explicit expiry (mint's core; separate for tests). The
    /// origin is base64url-encoded without padding so its segment can never
    /// contain the `.` separator, whatever host and port it names.
    pub(crate) fn sign(&self, policy_name: &str, origin: &str, expiry_unix: u64) -> String {
        let origin_segment = URL_SAFE_NO_PAD.encode(origin.as_bytes());
        let message = format!("{policy_name}.{expiry_unix}.{origin_segment}");
        let mut mac = HmacSha256::new_from_slice(&self.0).expect("HMAC accepts any key length");
        mac.update(message.as_bytes());
        let tag = mac.finalize().into_bytes();
        format!("{message}.{}", hex::encode(tag))
    }

    /// Verify a presented token against the current time, returning its
    /// claims. Whether the named policy exists is the caller's check — this
    /// only proves we minted the token and it is fresh.
    pub(crate) fn verify<'a>(&self, token: &'a str) -> Result<TokenClaims<'a>, TokenError> {
        self.verify_at(token, unix_now())
    }

    /// [`Self::verify`] with an explicit "now" (separate for tests).
    ///
    /// Policy names may themselves contain `.`, so the token is split from
    /// the right: the last three segments are the expiry, the origin and
    /// the tag, the rest is the name. The HMAC comparison is constant-time
    /// (`Mac::verify_slice` compares via `subtle`); it runs before the
    /// expiry check so the code path for a forged token does not depend on
    /// the claimed expiry.
    pub(crate) fn verify_at<'a>(
        &self,
        token: &'a str,
        now: u64,
    ) -> Result<TokenClaims<'a>, TokenError> {
        let mut segments = token.rsplitn(4, '.');
        let tag_hex = segments.next().ok_or(TokenError::Malformed)?;
        let origin_segment = segments.next().ok_or(TokenError::Malformed)?;
        let expiry_str = segments.next().ok_or(TokenError::Malformed)?;
        let name = segments.next().ok_or(TokenError::Malformed)?;
        if name.is_empty() {
            return Err(TokenError::Malformed);
        }
        let expiry: u64 = expiry_str.parse().map_err(|_| TokenError::Malformed)?;
        let origin = URL_SAFE_NO_PAD
            .decode(origin_segment)
            .ok()
            .and_then(|bytes| String::from_utf8(bytes).ok())
            .filter(|origin| !origin.is_empty())
            .ok_or(TokenError::Malformed)?;
        let tag = hex::decode(tag_hex).map_err(|_| TokenError::Malformed)?;

        let mut mac = HmacSha256::new_from_slice(&self.0).expect("HMAC accepts any key length");
        // The signed message is everything before the tag separator.
        mac.update(&token.as_bytes()[..token.len() - tag_hex.len() - 1]);
        // Constant-time comparison: verify_slice goes through subtle's
        // CtOption, never a byte-by-byte early-exit compare.
        mac.verify_slice(&tag).map_err(|_| TokenError::BadHmac)?;

        if expiry < now {
            return Err(TokenError::Expired);
        }
        Ok(TokenClaims {
            policy: name,
            origin,
        })
    }
}

pub(crate) fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    const ORIGIN: &str = "http://127.0.0.1:6342";

    fn claims<'a>(policy: &'a str, origin: &str) -> TokenClaims<'a> {
        TokenClaims {
            policy,
            origin: origin.to_string(),
        }
    }

    /// Round trip: a minted token verifies under the same key and returns
    /// both claims — including policy names containing dots, which the
    /// right-split parse must not truncate, and origins of every shape the
    /// gateway mints (dotted IPv4, bracketed IPv6, a bare hostname).
    #[test]
    fn mint_verify_round_trip() {
        let key = TokenKey::random();
        for name in ["localhost", "public_demo", "a.b.c", "x"] {
            for origin in [
                "http://127.0.0.1:6342",
                "http://[::1]:6342",
                "http://myhost:80",
                "http://10.0.0.5:8080",
            ] {
                let token = key.mint(name, origin);
                assert_eq!(
                    key.verify(&token),
                    Ok(claims(name, origin)),
                    "token: {token}"
                );
            }
        }
    }

    /// Expiry: valid at the expiry instant, rejected one second after.
    #[test]
    fn expired_tokens_are_rejected() {
        let key = TokenKey::random();
        let token = key.sign("demo", ORIGIN, 1_000_000);
        assert_eq!(key.verify_at(&token, 1_000_000), Ok(claims("demo", ORIGIN)));
        assert_eq!(key.verify_at(&token, 999_999), Ok(claims("demo", ORIGIN)));
        assert_eq!(key.verify_at(&token, 1_000_001), Err(TokenError::Expired));
    }

    /// The pre-origin three-segment format is malformed, never a partial
    /// parse: its expiry lands in the origin slot and its name in the
    /// expiry slot.
    #[test]
    fn legacy_three_segment_tokens_are_malformed() {
        let key = TokenKey::random();
        let token = format!("demo.1000000.{}", "ab".repeat(32));
        assert_eq!(key.verify_at(&token, 1_000_000), Err(TokenError::Malformed));
    }

    /// Garbage and truncated tokens are malformed, not panics; tampering
    /// with any authenticated part (name, expiry, tag) is bad-hmac; and a
    /// token from a different key never verifies.
    #[test]
    fn forged_and_broken_tokens_are_rejected() {
        let key = TokenKey::random();
        let now = 1_000_000;
        for garbage in [
            "",
            ".",
            "..",
            "...",
            "no-dots-at-all",
            "one.dot",
            "name.notanumber.abcdef",
            "name.12345.zz-not-hex",
            ".12345.abcdef", // empty policy name
            "name.notanumber.aHR0cA.abcdef",
            "name.12345.aHR0cA.zz-not-hex",
            ".12345.aHR0cA.abcdef",   // empty policy name
            "name.12345..abcdef",     // empty origin
            "name.12345.!!!!.abcdef", // origin not base64url
            "name.12345._w.abcdef",   // origin not UTF-8 (0xff)
        ] {
            assert_eq!(
                key.verify_at(garbage, now),
                Err(TokenError::Malformed),
                "garbage: {garbage:?}"
            );
        }

        let token = key.sign("demo", ORIGIN, now + 100);
        // Truncated tag: still parses as hex (even length) -> bad-hmac;
        // odd-length truncation -> malformed. Both must be rejected.
        assert!(key.verify_at(&token[..token.len() - 2], now).is_err());
        assert!(key.verify_at(&token[..token.len() - 1], now).is_err());

        // Tampered name and tampered expiry both break the HMAC.
        let tampered_name = token.replacen("demo", "root", 1);
        assert_eq!(key.verify_at(&tampered_name, now), Err(TokenError::BadHmac));
        let tampered_expiry = token.replacen(
            &format!(".{}", now + 100),
            &format!(".{}", now + 9_999_999),
            1,
        );
        assert_eq!(
            key.verify_at(&tampered_expiry, now),
            Err(TokenError::BadHmac)
        );

        // A re-pointed origin (the claim the UI routes SSR calls by) breaks
        // the HMAC just like the name and the expiry do.
        let other_origin = URL_SAFE_NO_PAD.encode(b"http://127.0.0.1:1");
        let tampered_origin = token.replacen(&URL_SAFE_NO_PAD.encode(ORIGIN), &other_origin, 1);
        assert_ne!(tampered_origin, token);
        assert_eq!(
            key.verify_at(&tampered_origin, now),
            Err(TokenError::BadHmac)
        );

        // Bit flip in the tag.
        let mut flipped = token.clone().into_bytes();
        let last = flipped.len() - 1;
        flipped[last] = if flipped[last] == b'0' { b'1' } else { b'0' };
        let flipped = String::from_utf8(flipped).unwrap();
        assert_eq!(key.verify_at(&flipped, now), Err(TokenError::BadHmac));

        // A different key rejects everything the first key minted.
        let other = TokenKey::random();
        assert_eq!(other.verify_at(&token, now), Err(TokenError::BadHmac));
    }

    /// The configured-key path: a 64-hex-char key parses and produces
    /// deterministic tokens (two instances from the same hex agree), and
    /// invalid hex / wrong lengths fail with a clear error.
    #[test]
    fn configured_key_parses_and_is_deterministic() {
        let hex_key = "aa".repeat(32);
        let load = |key_line: &str| {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("gw.toml");
            std::fs::write(
                &path,
                format!(
                    r#"
[server]
host = "127.0.0.1"
port = 9155
{key_line}

[upstreams.ui]
base_url = "http://127.0.0.1:6339"

[upstreams.api]
base_url = "http://127.0.0.1:6342"
"#
                ),
            )
            .unwrap();
            Settings::load(Some(path)).unwrap()
        };

        let settings = load(&format!("policy_token_key = \"{hex_key}\""));
        let key_a = TokenKey::from_settings(&settings).unwrap();
        let key_b = TokenKey::from_settings(&settings).unwrap();
        let token = key_a.sign("demo", ORIGIN, 42);
        assert_eq!(key_b.verify_at(&token, 42), Ok(claims("demo", ORIGIN)));

        // Unset -> random per boot: two keys disagree.
        let settings = load("");
        let key_a = TokenKey::from_settings(&settings).unwrap();
        let key_b = TokenKey::from_settings(&settings).unwrap();
        let token = key_a.sign("demo", ORIGIN, 42);
        assert_eq!(key_b.verify_at(&token, 42), Err(TokenError::BadHmac));

        // Bad hex and wrong length are startup errors.
        let settings = load("policy_token_key = \"not hex\"");
        assert!(TokenKey::from_settings(&settings).is_err());
        let settings = load("policy_token_key = \"aabb\"");
        let err = TokenKey::from_settings(&settings)
            .err()
            .expect("2-byte key must fail");
        assert!(format!("{err:#}").contains("32 bytes"), "{err:#}");
    }
}
