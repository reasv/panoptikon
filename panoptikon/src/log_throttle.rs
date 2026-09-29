//! Rate limiting for log lines that repeat once per request or per item, so
//! an outage costs a few lines instead of one per request.

use std::sync::{Arc, Mutex};
use std::time::Duration;

/// How long a throttled line stays quiet after it is logged.
pub(crate) const LOG_REPEAT_WINDOW: Duration = Duration::from_secs(10);

/// One repeating log line. The first occurrence logs in full and opens a
/// window; occurrences inside it are only counted, and when it closes one
/// summary line reports the count. Without a tokio runtime nothing is
/// suppressed.
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
    state: Mutex<State>,
}

#[derive(Debug, Default)]
struct State {
    open: bool,
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
                state: Mutex::new(State::default()),
            }),
        }
    }

    /// Whether this occurrence should be logged.
    pub(crate) fn admit(&self) -> bool {
        let mut state = self.inner.lock();
        if state.open {
            state.suppressed += 1;
            return false;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return true;
        };
        state.open = true;
        let inner = Arc::clone(&self.inner);
        runtime.spawn(async move {
            tokio::time::sleep(inner.window).await;
            inner.close();
        });
        true
    }
}

impl Inner {
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Ends the window and logs what it suppressed; returns that count.
    fn close(&self) -> u64 {
        let suppressed = {
            let mut state = self.lock();
            state.open = false;
            std::mem::take(&mut state.suppressed)
        };
        if suppressed > 0 {
            let window_secs = self.window.as_secs();
            let what = self.what.as_str();
            if self.level == tracing::Level::ERROR {
                tracing::error!("{what}: {suppressed} more in the last {window_secs} s");
            } else {
                tracing::warn!("{what}: {suppressed} more in the last {window_secs} s");
            }
        }
        suppressed
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
        assert_eq!(throttle.inner.close(), 5, "the window's count");
        assert!(throttle.admit(), "a closed window reopens");
        assert!(!throttle.admit());
        tokio::time::sleep(Duration::from_secs(11)).await;
        assert!(throttle.admit(), "the window closes by itself");
    }

    #[test]
    fn nothing_is_suppressed_without_a_runtime() {
        let throttle = LogThrottle::new("x", tracing::Level::WARN);
        assert!(throttle.admit() && throttle.admit());
    }
}
