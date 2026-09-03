//! Child-process lifetime plumbing shared by inferio worker supervision and
//! the UI server: a kill-on-close job object wrapper (Windows), console
//! detachment, and the Unix counterparts (PR_SET_PDEATHSIG, process-group
//! SIGKILL). On Windows every descendant of the job-assigned process is
//! terminated when the guard drops, covering both multi-process trees
//! (Chromium) and launchers whose real payload detaches from the spawned
//! process (msedge.exe); also used by the HTML-thumbnail browser path. On
//! Unix the job-object role is split: `die_with_parent` ties the direct
//! child to gateway death via the kernel, and `kill_process_group` reaps
//! the child's descendants on the explicit kill paths.
//!
//! Because that Linux tie is to the forking **thread** and not to the
//! process (see [`die_with_parent`]), every armed spawn is funnelled through
//! one permanently alive thread: [`spawn_supervised`] and
//! [`spawn_supervised_tokio`]. That is the module's central invariant — a
//! `die_with_parent` command spawned from anywhere else is a worker that
//! dies when whichever thread happened to fork it goes away.

/// The spawn-configuration surface both `std::process::Command` and
/// `tokio::process::Command` expose under different names. It exists so the
/// two policies below (console detachment, parent-death) have exactly one
/// implementation each: the blocking transcode runner spawns std children
/// while every other supervised child is a tokio one, and a second copy of
/// either policy would be a platform-specific divergence waiting to happen.
pub(crate) trait SpawnCommand {
    #[cfg(windows)]
    fn set_creation_flags(&mut self, flags: u32);
    #[cfg(unix)]
    fn set_process_group(&mut self, pgid: i32);
    /// # Safety
    /// The hook runs between fork and exec in the child, so it may only call
    /// async-signal-safe functions.
    #[cfg(target_os = "linux")]
    unsafe fn set_pre_exec(
        &mut self,
        hook: Box<dyn FnMut() -> std::io::Result<()> + Send + Sync + 'static>,
    );
}

impl SpawnCommand for std::process::Command {
    #[cfg(windows)]
    fn set_creation_flags(&mut self, flags: u32) {
        use std::os::windows::process::CommandExt;
        self.creation_flags(flags);
    }
    #[cfg(unix)]
    fn set_process_group(&mut self, pgid: i32) {
        use std::os::unix::process::CommandExt;
        self.process_group(pgid);
    }
    #[cfg(target_os = "linux")]
    unsafe fn set_pre_exec(
        &mut self,
        hook: Box<dyn FnMut() -> std::io::Result<()> + Send + Sync + 'static>,
    ) {
        use std::os::unix::process::CommandExt;
        unsafe {
            self.pre_exec(hook);
        }
    }
}

impl SpawnCommand for tokio::process::Command {
    #[cfg(windows)]
    fn set_creation_flags(&mut self, flags: u32) {
        self.creation_flags(flags);
    }
    #[cfg(unix)]
    fn set_process_group(&mut self, pgid: i32) {
        self.process_group(pgid);
    }
    #[cfg(target_os = "linux")]
    unsafe fn set_pre_exec(
        &mut self,
        hook: Box<dyn FnMut() -> std::io::Result<()> + Send + Sync + 'static>,
    ) {
        unsafe {
            self.pre_exec(hook);
        }
    }
}

/// Keep console signals for the gateway alone: a Ctrl-C that reached the
/// children directly would kill them before the supervisor is told to stop,
/// logging spurious "exited unexpectedly" noise mid-shutdown and skipping
/// graceful teardown. Same treatment the Python router gave its children
/// (CREATE_NEW_PROCESS_GROUP on Windows, setsid on Unix); shutdown delivery
/// is unaffected — supervisors stop children via their own ladders
/// (TerminateProcess/SIGKILL and the job object), never console signals.
pub(crate) fn detach_from_console<C: SpawnCommand>(command: &mut C) {
    #[cfg(windows)]
    {
        use windows_sys::Win32::System::Threading::{CREATE_NEW_PROCESS_GROUP, CREATE_NO_WINDOW};
        command.set_creation_flags(CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW);
    }
    #[cfg(unix)]
    {
        command.set_process_group(0);
    }
    #[cfg(not(any(windows, unix)))]
    {
        let _ = command;
    }
}

/// Make the child die when the gateway does, even when no gateway code runs
/// (second-Ctrl-C `process::exit`, the hard-exit timer, an external SIGKILL
/// or OOM kill of the gateway — none of which run destructors, so
/// `kill_on_drop` never fires). Windows is a no-op: the kill-on-close job
/// object already makes worker death a kernel-enforced consequence of
/// gateway death. On Linux the equivalent is PR_SET_PDEATHSIG, and its
/// scope is the trap this function is documented around: **the kernel
/// delivers the SIGKILL when the forking thread exits, not when the forking
/// process does** (`man 2 prctl`). A command armed here must therefore be
/// spawned from a thread that cannot exit before the gateway does, which is
/// what [`spawn_supervised`]/[`spawn_supervised_tokio`] are for — every
/// caller of this function must use one of them.
///
/// This used to say the arming was "safe because spawns happen on tokio
/// core worker threads, which live until the runtime goes down". That
/// premise was false: tokio launches its multi-thread workers *through the
/// blocking pool*, so any `block_in_place` on such a thread demotes it to an
/// ordinary pooled thread with a 10 s idle keep-alive. The load-path host
/// probe did exactly that milliseconds before forking a worker, and the
/// kernel then SIGKILLed a perfectly healthy worker ~10 s later — measured
/// 8/8, 1–3 ms after the forking thread's `exit(0)` (finding F11).
///
/// The fork→prctl gap
/// is closed by re-checking the parent after arming: if the gateway died in
/// between, the signal never armed, so the child exits itself.
/// macOS has no PR_SET_PDEATHSIG equivalent (prctl is Linux-only), so there
/// this is a no-op: `kill_process_group` still covers every orderly shutdown
/// path, and only a gateway death where no gateway code runs can leave the
/// child behind.
pub(crate) fn die_with_parent<C: SpawnCommand>(command: &mut C) {
    #[cfg(target_os = "linux")]
    {
        let gateway = std::process::id() as libc::pid_t;
        // SAFETY: runs between fork and exec in the child; prctl, getppid,
        // and _exit are async-signal-safe.
        unsafe {
            command.set_pre_exec(Box::new(move || {
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                if libc::getppid() != gateway {
                    libc::_exit(127);
                }
                Ok(())
            }));
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = command;
    }
}

/// Work handed to the spawner thread. Each job carries its own reply
/// channel, so one queue serves both the std and the tokio flavour.
type SpawnJob = Box<dyn FnOnce() + Send + 'static>;

/// The one thread every [`die_with_parent`]-armed child is forked from.
///
/// Started on first use and never joined, never told to stop and never
/// allowed to return: it is the thread whose lifetime PR_SET_PDEATHSIG ties
/// the children to, so "it exits" and "every worker dies" are the same
/// sentence. Nothing runs on it but `Command::spawn` — no user code, no
/// blocking I/O — so a job cannot wedge the queue behind it.
static SPAWNER: std::sync::OnceLock<std::sync::Mutex<std::sync::mpsc::Sender<SpawnJob>>> =
    std::sync::OnceLock::new();

/// Hand one job to the spawner thread, starting it if this is the first.
fn submit(job: SpawnJob) -> std::io::Result<()> {
    let sender = SPAWNER.get_or_init(|| {
        let (tx, rx) = std::sync::mpsc::channel::<SpawnJob>();
        std::thread::Builder::new()
            .name("panoptikon-spawner".to_owned())
            // Nothing but forks runs here, and this is also the stack the
            // pre-exec hook runs on in the child, so keep it small.
            .stack_size(512 * 1024)
            .spawn(move || {
                // `for` ends only if every sender is dropped, and the one
                // sender lives in a `static` — so this loop never ends.
                for job in rx {
                    job();
                }
            })
            .expect("the process spawner thread must start");
        std::sync::Mutex::new(tx)
    });
    let sender = sender.lock().unwrap_or_else(|err| err.into_inner());
    sender.send(job).map_err(|_| {
        std::io::Error::other("the process spawner thread is gone; cannot spawn children")
    })
}

/// Spawn a `std` child from the permanent spawner thread. Blocks the caller
/// for the length of one `fork`+`exec`; use it for every command armed with
/// [`die_with_parent`] (the blocking transcode runner's ffmpeg children).
pub(crate) fn spawn_supervised(
    command: std::process::Command,
) -> std::io::Result<std::process::Child> {
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    submit(Box::new(move || {
        let mut command = command;
        let _ = tx.send(command.spawn());
    }))?;
    rx.recv()
        .unwrap_or_else(|_| Err(std::io::Error::other("the process spawner thread died")))
}

/// Spawn a `tokio` child from the permanent spawner thread — the async
/// counterpart of [`spawn_supervised`], and the path every inferio worker
/// takes.
///
/// The child is created inside `Handle::enter()` of the **caller's** runtime
/// rather than one captured when the spawner started: `tokio::process`
/// registers the child with the runtime's signal driver (SIGCHLD) and its
/// pipes with the I/O driver at spawn time, and it has to be the same
/// runtime that later `wait()`s on it — otherwise the reaping machinery
/// belongs to a runtime nobody is driving. Handing the handle over per call
/// also keeps the spawner correct in tests, where every `#[tokio::test]`
/// builds and drops a runtime of its own.
///
/// # Panics
/// Called outside a tokio runtime (`Handle::current`).
pub(crate) async fn spawn_supervised_tokio(
    command: tokio::process::Command,
) -> std::io::Result<tokio::process::Child> {
    let handle = tokio::runtime::Handle::current();
    let (tx, rx) = tokio::sync::oneshot::channel();
    submit(Box::new(move || {
        let mut command = command;
        // Held across the send so that a child nobody is waiting for any
        // more (a cancelled caller) is also *dropped* in runtime context.
        let _guard = handle.enter();
        let _ = tx.send(command.spawn());
    }))?;
    rx.await
        .unwrap_or_else(|_| Err(std::io::Error::other("the process spawner thread died")))
}

/// SIGKILL the child's whole process group. The spawn made the child its
/// own group leader (`detach_from_console`), so this reaps descendants
/// (dataloader workers and the like) that a plain child kill would orphan —
/// the Unix stand-in for the job object's kill-the-tree semantics, minus
/// processes that left the group. No-op once the child has been reaped
/// (`id()` is `None`; an unreaped exited child stays a zombie, so its pid —
/// and group id — cannot be recycled out from under us). Windows: no-op,
/// the job object covers the tree.
pub(crate) fn kill_process_group(child: &tokio::process::Child) {
    kill_process_group_pid(child.id());
}

/// [`kill_process_group`] by pid, for callers holding a `std` child (whose
/// `id()` is not invalidated by reaping — only call this while the child is
/// still unreaped, or the pid may have been handed to someone else).
pub(crate) fn kill_process_group_pid(pid: Option<u32>) {
    #[cfg(unix)]
    if let Some(pid) = pid {
        // Errors (ESRCH: group already gone) are irrelevant by design.
        unsafe {
            libc::kill(-(pid as libc::pid_t), libc::SIGKILL);
        }
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
    }
}

pub(crate) struct JobGuard {
    #[cfg(windows)]
    _job: Option<windows_job::Job>,
}

impl JobGuard {
    /// Closes the job object, reaping any grandchildren still inside it.
    /// Spelled as a method rather than `drop(guard)` because on non-Windows
    /// targets the guard carries nothing and has no `Drop` impl.
    pub(crate) fn release(self) {}

    pub(crate) fn assign(child: &std::process::Child) -> JobGuard {
        #[cfg(windows)]
        {
            use std::os::windows::io::AsRawHandle;
            Self::from_raw_handle(child.as_raw_handle())
        }
        #[cfg(not(windows))]
        {
            let _ = child;
            JobGuard {}
        }
    }

    /// Assign a tokio child. On Windows the raw handle is only available
    /// while the child has not yet been reaped; a `None` handle degrades to
    /// an unarmed guard with a warning, mirroring job-creation failure.
    pub(crate) fn assign_tokio(child: &tokio::process::Child) -> JobGuard {
        #[cfg(windows)]
        {
            match child.raw_handle() {
                Some(handle) => Self::from_raw_handle(handle),
                None => {
                    tracing::warn!(
                        "child already reaped; no job object assigned to its process tree"
                    );
                    JobGuard { _job: None }
                }
            }
        }
        #[cfg(not(windows))]
        {
            let _ = child;
            JobGuard {}
        }
    }

    #[cfg(windows)]
    fn from_raw_handle(handle: std::os::windows::io::RawHandle) -> JobGuard {
        let job = windows_job::Job::assign_handle(handle);
        if job.is_none() {
            tracing::warn!(
                "failed to create job object; child process tree may outlive the gateway"
            );
        }
        JobGuard { _job: job }
    }
}

#[cfg(windows)]
mod windows_job {
    use windows_sys::Win32::Foundation::{CloseHandle, HANDLE};
    use windows_sys::Win32::System::JobObjects::{
        AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
        JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
        SetInformationJobObject,
    };

    pub(super) struct Job(HANDLE);

    // The handle is used only to close the job object exactly once; the
    // kernel object itself is thread-safe.
    unsafe impl Send for Job {}

    impl Job {
        /// Children the process spawned before this call are not captured
        /// (std cannot spawn suspended), but launchers need far longer to
        /// start their payload than this takes to run.
        pub(super) fn assign_handle(process: std::os::windows::io::RawHandle) -> Option<Job> {
            unsafe {
                let handle = CreateJobObjectW(std::ptr::null(), std::ptr::null());
                if handle.is_null() {
                    return None;
                }
                // Owns the handle from here on, so early returns close it.
                let job = Job(handle);
                let mut info: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = std::mem::zeroed();
                info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
                if SetInformationJobObject(
                    handle,
                    JobObjectExtendedLimitInformation,
                    (&info as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
                    std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
                ) == 0
                {
                    return None;
                }
                if AssignProcessToJobObject(handle, process as HANDLE) == 0 {
                    return None;
                }
                Some(job)
            }
        }
    }

    impl Drop for Job {
        fn drop(&mut self) {
            // Kill-on-close terminates every process still in the job.
            unsafe {
                CloseHandle(self.0);
            }
        }
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use std::os::unix::process::ExitStatusExt;
    use std::process::{Command, ExitStatus, Stdio};
    use std::time::{Duration, Instant};

    /// A long-lived child armed exactly the way every supervised spawn is.
    fn armed_sleeper() -> Command {
        let mut command = Command::new("sleep");
        command
            .arg("30")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        detach_from_console(&mut command);
        die_with_parent(&mut command);
        command
    }

    /// Poll for the child's exit up to `within`. `None` = it is still
    /// running, which is the assertion the spawner test makes.
    fn wait_for_exit(child: &mut std::process::Child, within: Duration) -> Option<ExitStatus> {
        let deadline = Instant::now() + within;
        loop {
            if let Ok(Some(status)) = child.try_wait() {
                return Some(status);
            }
            if Instant::now() >= deadline {
                return None;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// The negative control, and the whole reason the spawner exists (F11):
    /// PR_SET_PDEATHSIG is armed against the forking **thread**, so a child
    /// forked on a thread that then exits is SIGKILLed by the kernel while
    /// the process that "owns" it is perfectly alive. Tokio demotes and
    /// retires runtime threads (`block_in_place` → blocking pool → 10 s
    /// keep-alive), which is how healthy inferio workers were being killed.
    #[test]
    fn a_child_forked_on_a_thread_that_exits_is_killed_by_the_kernel() {
        let mut child = std::thread::spawn(|| armed_sleeper().spawn().expect("spawn sleep"))
            .join()
            .expect("the forking thread finished");
        let status = wait_for_exit(&mut child, Duration::from_secs(5))
            .expect("the kernel killed it when its forking thread exited");
        assert_eq!(
            status.signal(),
            Some(libc::SIGKILL),
            "PR_SET_PDEATHSIG delivers SIGKILL: {status}"
        );
    }

    /// And the fix: routed through the spawner, the same command outlives
    /// the thread that asked for it, because the thread it was forked from
    /// is the one that never exits.
    #[test]
    fn a_child_forked_by_the_spawner_outlives_the_requesting_thread() {
        let mut child =
            std::thread::spawn(|| spawn_supervised(armed_sleeper()).expect("spawn sleep"))
                .join()
                .expect("the requesting thread finished");
        let died = wait_for_exit(&mut child, Duration::from_millis(750));
        let alive = died.is_none();
        // Reaped either way, so a failure does not leak a `sleep`.
        let _ = child.kill();
        let _ = child.wait();
        assert!(
            alive,
            "the child must survive its requester: {died:?} (spawner thread gone?)"
        );
    }
}
