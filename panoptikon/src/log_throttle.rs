//! Rate limiting for log lines that repeat once per request or per item, so
//! an outage costs a few lines instead of one per request.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::Instant;

/// How long a throttled line stays quiet after it is logged.
pub(crate) const LOG_REPEAT_WINDOW: Duration = Duration::from_secs(10);

/// Keys one throttle tracks at once; past this, lines log unthrottled.
const MAX_KEYS: usize = 256;

/// One repeating log line, throttled per key so distinct errors stay
/// visible. The first occurrence of a key logs in full and opens a window;
/// occurrences inside it are only counted. The count is reported by a timer
/// when the window closes, or by the next occurrence after it when no timer
/// ran (no runtime, or the runtime that held it is gone).
#[derive(Debug, Clone)]
pub(crate) struct LogThrottle {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    /// Names the line in the summary.
    what: String,
    level: tracing::Level,
    window: Duration,
    windows: Mutex<HashMap<String, Window>>,
}

#[derive(Debug)]
struct Window {
    opened: Instant,
    suppressed: u64,
}

impl LogThrottle {
    pub(crate) fn new(what: impl Into<String>, level: tracing::Level) -> Self {
        Self::with_window(what, level, LOG_REPEAT_WINDOW)
    }

    pub(crate) fn with_window(
        what: impl Into<String>,
        level: tracing::Level,
        window: Duration,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                what: what.into(),
                level,
                window,
                windows: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Whether this occurrence of the line should be logged.
    pub(crate) fn admit(&self) -> bool {
        self.admit_for("")
    }

    /// Whether this occurrence of the line, for `key`, should be logged.
    pub(crate) fn admit_for(&self, key: &str) -> bool {
        let now = Instant::now();
        let lapsed = {
            let mut windows = self.inner.lock();
            match windows.get_mut(key) {
                Some(window) if now.duration_since(window.opened) < self.inner.window => {
                    window.suppressed += 1;
                    return false;
                }
                Some(window) => {
                    window.opened = now;
                    std::mem::take(&mut window.suppressed)
                }
                None => {
                    if windows.len() >= MAX_KEYS {
                        windows.retain(|_, window| {
                            now.duration_since(window.opened) < self.inner.window
                        });
                        if windows.len() >= MAX_KEYS {
                            return true;
                        }
                    }
                    windows.insert(
                        key.to_owned(),
                        Window {
                            opened: now,
                            suppressed: 0,
                        },
                    );
                    0
                }
            }
        };
        self.inner.report(key, lapsed);
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let (inner, key) = (Arc::clone(&self.inner), key.to_owned());
            runtime.spawn(async move {
                tokio::time::sleep(inner.window).await;
                inner.close(&key, now);
            });
        }
        true
    }
}

impl Inner {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Window>> {
        self.windows
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Ends the window `key` opened at `opened`, if it is still that one, and
    /// logs what it suppressed; returns that count.
    fn close(&self, key: &str, opened: Instant) -> u64 {
        let suppressed = {
            let mut windows = self.lock();
            if windows
                .get(key)
                .is_none_or(|window| window.opened != opened)
            {
                return 0;
            }
            windows.remove(key).map_or(0, |window| window.suppressed)
        };
        self.report(key, suppressed);
        suppressed
    }

    fn report(&self, key: &str, suppressed: u64) {
        if suppressed == 0 {
            return;
        }
        let what = self.what.as_str();
        let window_secs = self.window.as_secs();
        let key = if key.is_empty() {
            String::new()
        } else {
            format!(" ({key})")
        };
        if self.level == tracing::Level::ERROR {
            tracing::error!("{what}{key}: {suppressed} more in the last {window_secs} s");
        } else {
            tracing::warn!("{what}{key}: {suppressed} more in the last {window_secs} s");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One line per window plus one count of the rest, and the next window
    /// opens on its own.
    #[tokio::test(start_paused = true)]
    async fn the_first_line_of_a_window_is_logged_and_the_rest_are_counted() {
        let throttle = LogThrottle::with_window("x", tracing::Level::WARN, Duration::from_secs(10));
        assert!(throttle.admit());
        assert!((0..5).all(|_| !throttle.admit()));
        let opened = throttle.inner.lock()[""].opened;
        assert_eq!(throttle.inner.close("", opened), 5, "the window's count");
        assert!(throttle.admit(), "a closed window reopens");
        assert!(!throttle.admit());
        tokio::time::sleep(Duration::from_secs(11)).await;
        assert!(throttle.admit(), "the window closes by itself");
    }

    /// Keys have their own windows.
    #[tokio::test(start_paused = true)]
    async fn distinct_keys_are_not_suppressed_by_each_other() {
        let throttle = LogThrottle::new("x", tracing::Level::WARN);
        assert!(throttle.admit_for("a"));
        assert!(throttle.admit_for("b"));
        assert!(!throttle.admit_for("a") && !throttle.admit_for("b"));
    }

    /// A window whose timer died with its runtime still ends on time: the
    /// next occurrence after it is logged.
    #[test]
    fn a_window_outlives_the_runtime_that_opened_it_by_no_more_than_its_length() {
        let throttle =
            LogThrottle::with_window("x", tracing::Level::WARN, Duration::from_millis(50));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        runtime.block_on(async {
            assert!(throttle.admit());
            assert!(!throttle.admit());
        });
        drop(runtime);
        std::thread::sleep(Duration::from_millis(100));
        assert!(throttle.admit(), "no runtime left to close it");
        assert!(!throttle.admit(), "and a new window opened");
    }
}
