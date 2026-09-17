//! The process's open-file-descriptor budget (`RLIMIT_NOFILE`).
//!
//! Local inference is served over loopback HTTP by the same process that
//! calls it, so one in-flight predict costs *two* sockets in one descriptor
//! table. So the soft limit is raised to the hard limit once at startup
//! ([`raise_soft_limit_at_startup`]) — free capacity nothing has to be
//! configured to grant — and `jobs::extraction` bounds its in-flight unit
//! budget by whatever survives that raise ([`soft_nofile_limit`]), so a host
//! whose *hard* limit is also small cannot be talked into exhausting its
//! table by an inference server's desired-in-flight figure.
//!
//! See docs/batch-calibration-run1-report.md, finding F6.

use std::sync::OnceLock;

/// What [`soft_nofile_limit`] reports where there is no per-process limit to
/// read. Windows has no `RLIMIT_NOFILE`, so a very large budget makes the
/// descriptor term of the in-flight ceiling non-binding there.
pub const NOFILE_LIMIT_UNKNOWN: u64 = u64::MAX;

/// Ceiling on what the startup raise will ask for. The hard limit can be
/// `RLIM_INFINITY`, which is meaningless (there is still a global
/// `fs.nr_open`) and rejected outright on some systems.
#[cfg(any(unix, test))]
const NOFILE_RAISE_CAP: u64 = 1_048_576;

/// Fallback target tried when the ambitious one is refused. macOS rejects a
/// `RLIMIT_NOFILE` above `kern.maxfilesperproc` even when the hard limit reads
/// as unlimited; rather than read that sysctl, retry once below it.
#[cfg(any(unix, test))]
const NOFILE_FALLBACK_TARGET: u64 = 10_240;

/// Outcome of the one-time startup raise, kept so it can be logged once
/// logging is configured: the raise itself happens before the config that
/// decides where logs go has been read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NofileRaise {
    /// No `RLIMIT_NOFILE` here; nothing was attempted. Windows only, so a
    /// Unix build never constructs it but still matches on it.
    #[cfg_attr(unix, allow(dead_code))]
    Unsupported,
    /// The soft limit already met the target: a no-op, not a failure.
    AlreadyRaised { soft: u64 },
    /// The soft limit was raised from `from` to `to`.
    Raised { from: u64, to: u64 },
    /// Every target was refused; the process keeps `soft`. Not fatal: the
    /// in-flight ceiling clamps itself to the surviving limit.
    Failed {
        soft: u64,
        wanted: u64,
        error: String,
    },
    /// The limit could not even be read. Treated like a failed raise.
    Unreadable { error: String },
}

static STARTUP_RAISE: OnceLock<NofileRaise> = OnceLock::new();

/// Raise the soft `RLIMIT_NOFILE` to the hard limit and remember the outcome
/// for [`log_startup_raise`]. Call exactly once, as early in `main` as
/// possible: before descriptors are handed out, and before the tokio runtime
/// is built so every thread and child process inherits it. A second call is
/// ignored.
pub fn raise_soft_limit_at_startup() {
    let outcome = raise_soft_limit();
    let _ = STARTUP_RAISE.set(outcome);
}

/// Log what the startup raise did. Call once logging is initialized.
pub fn log_startup_raise() {
    match STARTUP_RAISE.get() {
        None | Some(NofileRaise::Unsupported) => {}
        Some(NofileRaise::AlreadyRaised { soft }) => {
            tracing::info!(
                soft_nofile = soft,
                "open file descriptor limit is already at its maximum"
            );
        }
        Some(NofileRaise::Raised { from, to }) => {
            tracing::info!(
                soft_nofile_before = from,
                soft_nofile_after = to,
                "raised the open file descriptor soft limit to the hard limit"
            );
        }
        Some(NofileRaise::Failed {
            soft,
            wanted,
            error,
        }) => {
            tracing::warn!(
                soft_nofile = soft,
                wanted,
                %error,
                "could not raise the open file descriptor soft limit; \
                 continuing with the current limit, which also bounds how \
                 much inference work a job keeps in flight"
            );
        }
        Some(NofileRaise::Unreadable { error }) => {
            tracing::warn!(
                %error,
                "could not read the open file descriptor limit; continuing \
                 without raising it"
            );
        }
    }
}

/// The process's current soft `RLIMIT_NOFILE`. Read live rather than cached,
/// so a limit changed under us (`prlimit`) is honoured. [`NOFILE_LIMIT_UNKNOWN`]
/// when the platform has no such limit or it cannot be read.
pub fn soft_nofile_limit() -> u64 {
    #[cfg(unix)]
    {
        match get_nofile() {
            Ok((soft, _hard)) => soft,
            Err(_) => NOFILE_LIMIT_UNKNOWN,
        }
    }
    #[cfg(not(unix))]
    {
        NOFILE_LIMIT_UNKNOWN
    }
}

#[cfg(unix)]
fn raise_soft_limit() -> NofileRaise {
    raise_soft_limit_with(get_nofile, set_soft_nofile)
}

#[cfg(not(unix))]
fn raise_soft_limit() -> NofileRaise {
    NofileRaise::Unsupported
}

/// `(soft, hard)` from `getrlimit(RLIMIT_NOFILE)`. The casts are redundant
/// here but not everywhere: `rlim_t` is signed on some BSDs.
#[cfg(unix)]
#[allow(clippy::unnecessary_cast)]
fn get_nofile() -> std::io::Result<(u64, u64)> {
    // SAFETY: `getrlimit` writes into a fully owned, correctly typed struct.
    let mut limit = unsafe { std::mem::zeroed::<libc::rlimit>() };
    let rc = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok((limit.rlim_cur as u64, limit.rlim_max as u64))
}

#[cfg(unix)]
fn set_soft_nofile(soft: u64) -> std::io::Result<()> {
    let (_, hard) = get_nofile()?;
    let limit = libc::rlimit {
        rlim_cur: soft as libc::rlim_t,
        rlim_max: hard as libc::rlim_t,
    };
    // SAFETY: `setrlimit` reads a fully initialized, correctly typed struct.
    let rc = unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limit) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

/// The target the raise asks for, or `None` when the soft limit already meets
/// it. Split from the syscalls so it is testable without changing the test
/// process's own limits.
#[cfg(any(unix, test))]
fn raise_target(soft: u64, hard: u64) -> Option<u64> {
    let target = hard.min(NOFILE_RAISE_CAP);
    (target > soft).then_some(target)
}

/// The raise, with the two syscalls injected. Never returns an error: a
/// process that cannot raise its limit keeps running under the one it has.
#[cfg(any(unix, test))]
fn raise_soft_limit_with<G, S>(get: G, mut set: S) -> NofileRaise
where
    G: Fn() -> std::io::Result<(u64, u64)>,
    S: FnMut(u64) -> std::io::Result<()>,
{
    let (soft, hard) = match get() {
        Ok(pair) => pair,
        Err(error) => {
            return NofileRaise::Unreadable {
                error: error.to_string(),
            };
        }
    };
    let Some(target) = raise_target(soft, hard) else {
        return NofileRaise::AlreadyRaised { soft };
    };
    let mut last_error = match set(target) {
        Ok(()) => {
            return NofileRaise::Raised {
                from: soft,
                to: target,
            };
        }
        Err(error) => error,
    };
    // See NOFILE_FALLBACK_TARGET for why a second, lower target is tried.
    if target > NOFILE_FALLBACK_TARGET && NOFILE_FALLBACK_TARGET > soft {
        match set(NOFILE_FALLBACK_TARGET) {
            Ok(()) => {
                return NofileRaise::Raised {
                    from: soft,
                    to: NOFILE_FALLBACK_TARGET,
                };
            }
            Err(error) => last_error = error,
        }
    }
    NofileRaise::Failed {
        soft,
        wanted: target,
        error: last_error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    fn io_error(kind: std::io::ErrorKind) -> std::io::Error {
        std::io::Error::new(kind, "test")
    }

    /// The ordinary case: a small soft limit and a large hard one — what a
    /// login shell and a containerd container both hand the process.
    #[test]
    fn a_low_soft_limit_is_raised_to_the_hard_limit() {
        let asked = RefCell::new(Vec::new());
        let outcome = raise_soft_limit_with(
            || Ok((1024, 524_288)),
            |soft| {
                asked.borrow_mut().push(soft);
                Ok(())
            },
        );
        assert_eq!(
            outcome,
            NofileRaise::Raised {
                from: 1024,
                to: 524_288
            }
        );
        assert_eq!(*asked.borrow(), vec![524_288]);
    }

    /// soft == hard is a no-op, not a failed raise: nothing is asked for and
    /// the outcome logs at INFO rather than WARN.
    #[test]
    fn an_already_maximal_soft_limit_is_a_no_op() {
        let called = RefCell::new(false);
        let outcome = raise_soft_limit_with(
            || Ok((524_288, 524_288)),
            |_| {
                *called.borrow_mut() = true;
                Ok(())
            },
        );
        assert_eq!(outcome, NofileRaise::AlreadyRaised { soft: 524_288 });
        assert!(!*called.borrow(), "no setrlimit for a limit already met");
    }

    /// An unlimited hard limit is not asked for verbatim: the cap keeps the
    /// request to a number the kernel will accept.
    #[test]
    fn an_unlimited_hard_limit_is_capped() {
        assert_eq!(raise_target(1024, u64::MAX), Some(NOFILE_RAISE_CAP));
        // And a soft limit already above the cap is left alone rather than
        // being *lowered* to it.
        assert_eq!(raise_target(u64::MAX, u64::MAX), None);
        assert_eq!(raise_target(NOFILE_RAISE_CAP * 4, u64::MAX), None);
    }

    /// macOS shape: the hard limit reads as unlimited but the kernel refuses
    /// anything above `kern.maxfilesperproc`. The fallback target lands.
    #[test]
    fn a_refused_target_falls_back_to_a_modest_one() {
        let asked = RefCell::new(Vec::new());
        let outcome = raise_soft_limit_with(
            || Ok((256, u64::MAX)),
            |soft| {
                asked.borrow_mut().push(soft);
                if soft > NOFILE_FALLBACK_TARGET {
                    Err(io_error(std::io::ErrorKind::InvalidInput))
                } else {
                    Ok(())
                }
            },
        );
        assert_eq!(
            outcome,
            NofileRaise::Raised {
                from: 256,
                to: NOFILE_FALLBACK_TARGET
            }
        );
        assert_eq!(
            *asked.borrow(),
            vec![NOFILE_RAISE_CAP, NOFILE_FALLBACK_TARGET]
        );
    }

    /// The fallback is a *raise* that asks for less, never a lowering. A
    /// macOS-shaped host whose soft limit already sits above
    /// [`NOFILE_FALLBACK_TARGET`] (`launchctl limit maxfiles` raised, hard
    /// unlimited, the kernel still refusing the cap) must be left where it
    /// is: asking for 10 240 there would cost the process descriptors it
    /// already had.
    #[test]
    fn the_fallback_never_lowers_an_already_high_soft_limit() {
        let asked = RefCell::new(Vec::new());
        let outcome = raise_soft_limit_with(
            || Ok((20_000, u64::MAX)),
            |soft| {
                asked.borrow_mut().push(soft);
                Err(io_error(std::io::ErrorKind::InvalidInput))
            },
        );
        assert!(
            matches!(outcome, NofileRaise::Failed { soft: 20_000, .. }),
            "expected the existing limit to be kept, got {outcome:?}"
        );
        assert_eq!(
            *asked.borrow(),
            vec![NOFILE_RAISE_CAP],
            "the fallback must not be attempted below the current soft limit"
        );
        // And a hard limit somehow *below* the soft one asks for nothing at
        // all rather than shrinking the process to it.
        assert_eq!(raise_target(100_000, 1024), None);
    }

    /// A raise that fails outright is reported, not propagated: startup
    /// continues under the limit the process already has.
    #[test]
    fn a_failing_raise_is_tolerated() {
        let outcome = raise_soft_limit_with(
            || Ok((1024, 524_288)),
            |_| Err(io_error(std::io::ErrorKind::PermissionDenied)),
        );
        match outcome {
            NofileRaise::Failed { soft, wanted, .. } => {
                assert_eq!(soft, 1024);
                assert_eq!(wanted, 524_288);
            }
            other => panic!("expected a tolerated failure, got {other:?}"),
        }
        // So is a limit that cannot be read at all.
        let outcome =
            raise_soft_limit_with(|| Err(io_error(std::io::ErrorKind::NotFound)), |_| Ok(()));
        assert!(matches!(outcome, NofileRaise::Unreadable { .. }));
    }

    /// The live reader answers something usable on every platform: a real
    /// limit on Unix, the "no limit to read" sentinel elsewhere. The
    /// in-flight ceiling divides by this, so a zero would be a disaster.
    #[test]
    fn the_live_soft_limit_is_plausible() {
        let soft = soft_nofile_limit();
        assert!(soft > 0, "a soft limit of zero would starve every socket");
        #[cfg(unix)]
        {
            let (live, hard) = get_nofile().expect("getrlimit on a Unix host");
            assert_eq!(soft, live);
            assert!(live <= hard, "soft {live} above hard {hard}");
        }
    }
}
