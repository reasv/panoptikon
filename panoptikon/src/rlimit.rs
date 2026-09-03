//! The process's open-file-descriptor budget (`RLIMIT_NOFILE`).
//!
//! Two things in the gateway care about it, both because of the same
//! architectural fact: **local inference is served over loopback HTTP by the
//! same process that calls it**, so one in-flight predict costs *two* sockets
//! in one descriptor table (the client end and the accepted server end) on
//! top of the databases, the listeners, the worker pipes and the log files.
//!
//! - This module raises the soft limit to the hard limit once at startup
//!   ([`raise_soft_limit_at_startup`]). A bare Linux login shell usually
//!   starts at soft 1024 / hard 524 288, and a container started by
//!   containerd gets exactly the same pair from the default OCI spec, so the
//!   raise is free capacity that nothing else has to be configured to grant.
//! - `jobs::extraction` bounds its in-flight unit budget by whatever soft
//!   limit survives that raise ([`soft_nofile_limit`]), so a host whose
//!   *hard* limit is also small cannot be talked into exhausting its
//!   descriptor table by an inference server's desired-in-flight figure.
//!
//! The motivating failure (test protocol §8 G7, Phase 6 finding F6): in the
//! shipped Docker image at soft 1024, a 2000-item extraction job drove the
//! gateway to 983 sockets, `accept` began failing with `EMFILE`, SQLite could
//! not open its files, and the job ended with 1849 items unprocessed. The
//! same job on a build whose in-flight window was fixed at 64 items peaked at
//! 177 descriptors and finished.

use std::sync::OnceLock;

/// What [`soft_nofile_limit`] reports on a platform with no per-process
/// descriptor limit to read.
///
/// Windows has no `RLIMIT_NOFILE`: handles are bounded by kernel pool memory,
/// not by a per-process rlimit, and the C runtime's `_setmaxstdio` limit
/// applies to CRT `FILE*` streams rather than to sockets. Reporting a very
/// large budget makes the descriptor term of the in-flight ceiling
/// unconditionally non-binding there, which is the honest answer: on that
/// platform the other two terms are the only ones that mean anything.
pub const NOFILE_LIMIT_UNKNOWN: u64 = u64::MAX;

/// Ceiling on what the startup raise will ask for.
///
/// The hard limit is normally a real number (524 288 on this host and in a
/// containerd container), but it can be `RLIM_INFINITY`, and asking for
/// infinity is both meaningless — the kernel still has a global
/// `fs.nr_open` — and rejected outright on some systems. A million
/// descriptors is far above anything the gateway can plausibly want and stays
/// under the usual `fs.nr_open` of 1 048 576.
#[cfg(any(unix, test))]
const NOFILE_RAISE_CAP: u64 = 1_048_576;

/// Fallback target tried when the ambitious one is refused.
///
/// macOS rejects a `RLIMIT_NOFILE` above `kern.maxfilesperproc` with
/// `EINVAL` even when the hard limit reads as unlimited, and that sysctl is
/// commonly 24 576 or 61 440. Rather than read the sysctl, retry once with a
/// figure below every value it takes in practice: 10 240 is still ten times
/// the usual starting soft limit and covers ~5000 in-flight predicts.
#[cfg(any(unix, test))]
const NOFILE_FALLBACK_TARGET: u64 = 10_240;

/// Outcome of the one-time startup raise, kept so it can be logged after
/// logging is configured (the raise itself happens before the runtime, and
/// therefore before the config that decides where logs go has been read).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NofileRaise {
    /// No `RLIMIT_NOFILE` on this platform; nothing was attempted. Windows
    /// only, so a Unix build never constructs it — and still matches on it,
    /// which is why it is allowed rather than `cfg`-ed away.
    #[cfg_attr(unix, allow(dead_code))]
    Unsupported,
    /// The soft limit already met the target: a no-op, not a failure.
    AlreadyRaised { soft: u64 },
    /// The soft limit was raised from `from` to `to`.
    Raised { from: u64, to: u64 },
    /// Every target was refused; the process keeps `soft`. Not fatal: the
    /// in-flight ceiling reads the surviving limit and clamps itself to it.
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
/// for [`log_startup_raise`].
///
/// Call exactly once, as early in `main` as possible: raising the limit is
/// only useful before descriptors start being handed out, and doing it before
/// the tokio runtime is built means every thread and every child process
/// inherits the raised limit (rlimits are per-process and inherited across
/// `fork`/`exec`, so the inference workers and the UI server get it too).
/// A second call is ignored, keeping the first outcome.
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

/// The process's current soft `RLIMIT_NOFILE`, i.e. how many descriptors it
/// may hold open at once.
///
/// Read live rather than cached: it is consulted once per extraction job, the
/// syscall is trivial, and a limit changed out from under us (`prlimit` on a
/// running process) is then honoured. [`NOFILE_LIMIT_UNKNOWN`] when the
/// platform has no such limit or it cannot be read.
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

/// `(soft, hard)` from `getrlimit(RLIMIT_NOFILE)`.
///
/// The casts are not redundant on every target even though they are on this
/// one: `rlim_t` is `u64` on Linux and macOS but a signed 64-bit integer on
/// some BSDs, so the conversion has to be written as a cast rather than a
/// `From`/`TryFrom` that only exists on half of them.
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
/// it. Split out from the syscalls so the policy can be tested without
/// changing the test process's own limits.
#[cfg(any(unix, test))]
fn raise_target(soft: u64, hard: u64) -> Option<u64> {
    let target = hard.min(NOFILE_RAISE_CAP);
    (target > soft).then_some(target)
}

/// The raise, with the two syscalls injected.
///
/// Never returns an error: a process that cannot raise its limit is expected
/// to keep running under the limit it has, and the in-flight ceiling reads
/// the surviving figure.
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
    // See NOFILE_FALLBACK_TARGET: macOS refuses a limit above
    // `kern.maxfilesperproc` regardless of what the hard limit says.
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
