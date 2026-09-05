//! Child-process lifetime plumbing shared by inferio worker supervision, the
//! UI server and the HTML-thumbnail browser path: a kill-on-close job object
//! (Windows), console detachment, and the Unix counterparts.
//!
//! On Windows the job object terminates every descendant when the guard drops.
//! On Unix that role is split: [`die_with_parent`] ties the child to gateway
//! death through the kernel, [`kill_process_group`] reaps its descendants.
//!
//! **Central invariant:** the Linux tie is to the forking *thread*, not the
//! process, so every armed spawn goes through one permanently alive thread
//! ([`spawn_supervised`], [`spawn_supervised_tokio`]).

/// The spawn-configuration surface `std::process::Command` and
/// `tokio::process::Command` expose under different names, so the two policies
/// below have one implementation each.
pub(crate) trait SpawnCommand {
    #[cfg(windows)]
    fn set_creation_flags(&mut self, flags: u32);
    #[cfg(unix)]
    fn set_process_group(&mut self, pgid: i32);
    /// # Safety
    /// Post-fork in the child: only async-signal-safe calls.
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

/// Keep console signals for the gateway alone (`CREATE_NEW_PROCESS_GROUP` on
/// Windows, `setsid` on Unix): a Ctrl-C reaching the children directly would
/// kill them before the supervisor is told to stop. Shutdown is unaffected —
/// supervisors stop children through their own ladders.
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
/// (second-Ctrl-C `process::exit`, the hard-exit timer, an external SIGKILL or
/// OOM kill — none of which run destructors, so `kill_on_drop` never fires).
///
/// **The kernel delivers the SIGKILL when the forking *thread* exits, not when
/// the forking process does** (`man 2 prctl`), so every caller must go through
/// [`spawn_supervised`] or [`spawn_supervised_tokio`]. Tokio threads do not
/// qualify: a `block_in_place` demotes one into the blocking pool, with a 10 s
/// idle keep-alive. See docs/batch-calibration-run1-report.md, finding F11.
///
/// The fork-to-prctl gap is closed by re-checking the parent after arming.
/// Windows is a no-op (the job object already covers it) and so is macOS
/// (`prctl` is Linux-only), where `kill_process_group` covers every orderly
/// shutdown path.
pub(crate) fn die_with_parent<C: SpawnCommand>(command: &mut C) {
    #[cfg(target_os = "linux")]
    {
        let gateway = std::process::id() as libc::pid_t;
        // SAFETY: post-fork in the child; prctl/getppid/_exit are
        // async-signal-safe.
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

/// Work handed to the spawner thread. One queue, two flavours: each job
/// carries its own reply channel.
type SpawnJob = Box<dyn FnOnce() + Send + 'static>;

/// The one thread every [`die_with_parent`]-armed child is forked from. Never
/// joined, never told to stop and never allowed to return: its lifetime is
/// what PR_SET_PDEATHSIG ties the children to. Nothing runs on it but
/// `Command::spawn`, and a job that unwinds is caught.
static SPAWNER: std::sync::OnceLock<std::sync::Mutex<std::sync::mpsc::Sender<SpawnJob>>> =
    std::sync::OnceLock::new();

/// Hand one job to the spawner thread, starting it if this is the first.
fn submit(job: SpawnJob) -> std::io::Result<()> {
    let sender = SPAWNER.get_or_init(|| {
        let (tx, rx) = std::sync::mpsc::channel::<SpawnJob>();
        std::thread::Builder::new()
            .name("panoptikon-spawner".to_owned())
            // Also the stack the child's pre-exec hook runs on: keep small.
            .stack_size(512 * 1024)
            .spawn(move || {
                // The one sender lives in a `static`, so this never ends.
                for job in rx {
                    // Nor does a panicking job end it: an unwind here would
                    // SIGKILL every child ever forked from this thread. Jobs
                    // are not panic-free — the tokio flavour panics when the
                    // current runtime has no I/O driver. Contained: the reply
                    // channel dies with the job, so its caller gets a spawn
                    // error and every other child keeps its parent thread.
                    if let Err(payload) =
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(job))
                    {
                        let reason = payload
                            .downcast_ref::<&str>()
                            .copied()
                            .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
                            .unwrap_or("a non-string panic payload");
                        tracing::error!(
                            "a supervised spawn panicked ({reason}); its caller sees a failed \
                             spawn, and the spawner thread survives so that no already-forked \
                             child is killed by the kernel"
                        );
                    }
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

/// What both supervised spawns report when the spawner thread answered
/// nothing at all.
const NO_ANSWER: &str =
    "the supervised spawn produced no answer (it panicked, or the spawner thread is gone)";

/// Spawn a `std` child from the permanent spawner thread, blocking the caller
/// for one `fork`+`exec`. For every std command armed with
/// [`die_with_parent`].
pub(crate) fn spawn_supervised(
    command: std::process::Command,
) -> std::io::Result<std::process::Child> {
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    submit(Box::new(move || {
        let mut command = command;
        // No cancelled-caller case: this receiver is waited on below.
        let _ = tx.send(command.spawn());
    }))?;
    rx.recv()
        .unwrap_or_else(|_| Err(std::io::Error::other(NO_ANSWER)))
}

/// Spawn a `tokio` child from the permanent spawner thread — the async
/// counterpart of [`spawn_supervised`], and every inferio worker's path.
///
/// The child is created inside `Handle::enter()` of the **caller's** runtime,
/// not one captured when the spawner started: `tokio::process` registers the
/// child with the runtime's SIGCHLD and I/O drivers at spawn time, and it must
/// be the same runtime that later `wait()`s on it. Per-call handles also keep
/// the spawner correct in tests, where each `#[tokio::test]` has its own.
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
        // Held across the send, so an abandoned child is also dropped here.
        let _guard = handle.enter();
        if let Err(Ok(mut child)) = tx.send(command.spawn()) {
            // The caller went away between the submit and the reply, so
            // nothing will supervise this child, and it is tied to a thread
            // that never exits. Killed here rather than left to the command's
            // own `kill_on_drop`; group first, so a child that already forked
            // helpers takes them with it.
            kill_process_group(&child);
            let _ = child.start_kill();
        }
    }))?;
    rx.await
        .unwrap_or_else(|_| Err(std::io::Error::other(NO_ANSWER)))
}

/// SIGKILL the child's whole process group. The spawn made the child its own
/// group leader (`detach_from_console`), so this reaps descendants a plain
/// child kill would orphan. No-op once the child has been reaped (`id()` is
/// `None`; an unreaped exited child stays a zombie, so its pid cannot be
/// recycled out from under us). Windows: the job object covers it.
pub(crate) fn kill_process_group(child: &tokio::process::Child) {
    kill_process_group_pid(child.id());
}

/// [`kill_process_group`] by pid, for callers holding a `std` child, whose
/// `id()` survives reaping — so only call it while the child is unreaped, or
/// the pid may already belong to someone else.
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
    /// Close the job object, reaping any grandchildren still inside it. A
    /// method rather than `drop(guard)`, because on non-Windows targets the
    /// guard carries nothing and has no `Drop` impl.
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
    /// before the child is reaped; `None` degrades to an unarmed guard with a
    /// warning, mirroring job-creation failure.
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

    // Used only to close the job object once, and it is thread-safe.
    unsafe impl Send for Job {}

    impl Job {
        /// Children spawned before this call are not captured (std cannot
        /// spawn suspended), but launchers are far slower than this is.
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

    /// Poll for the child's exit up to `within`; `None` means still running.
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

    /// The negative control, and the reason the spawner exists: a child forked
    /// on a thread that then exits is SIGKILLed while its owning process is
    /// perfectly alive.
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

    /// And the fix: through the spawner, it outlives the thread that asked.
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

    /// One panicking job must not end the thread, because ending it is how
    /// every child forked from it dies — and the tokio flavour panics when the
    /// current runtime has no I/O driver.
    #[test]
    fn a_panicking_job_does_not_take_the_spawner_with_it() {
        // Forked before the panic; its survival is the assertion.
        let mut before = spawn_supervised(armed_sleeper()).expect("spawn sleep");

        let hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let (ran, unwound) = std::sync::mpsc::channel::<()>();
        submit(Box::new(move || {
            // Dropped by the unwind, which is how this test waits for it.
            let _ran = ran;
            panic!("a spawn job panicked");
        }))
        .expect("the job was queued");
        assert!(
            unwound.recv().is_err(),
            "the job ran and unwound without answering"
        );
        std::panic::set_hook(hook);

        // The spawner still answers, and the pre-panic child still has its
        // parent.
        let mut after = spawn_supervised(armed_sleeper()).expect("the spawner survived the panic");
        let died = wait_for_exit(&mut before, Duration::from_millis(250));
        for child in [&mut before, &mut after] {
            let _ = child.kill();
            let _ = child.wait();
        }
        assert!(
            died.is_none(),
            "the panic retired the forking thread and the kernel killed a child: {died:?}"
        );
    }

    /// A requester cancelled while awaiting its child must not leave it
    /// running: nobody supervises it, and its PDEATHSIG thread never exits.
    /// The child here would create a marker one second in.
    #[tokio::test]
    async fn a_cancelled_requester_leaves_no_child_behind() {
        let marker =
            std::env::temp_dir().join(format!("panoptikon-spawner-cancel-{}", std::process::id()));
        let _ = std::fs::remove_file(&marker);
        let mut command = tokio::process::Command::new("sh");
        command
            .arg("-c")
            .arg(format!("sleep 1; : > '{}'", marker.display()))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        detach_from_console(&mut command);
        die_with_parent(&mut command);

        // Occupy the spawner first, so the reply cannot beat the cancel.
        let (release, held) = std::sync::mpsc::channel::<()>();
        submit(Box::new(move || {
            let _ = held.recv_timeout(Duration::from_secs(5));
        }))
        .expect("the gate job was queued");

        // One poll submits the job; the timeout then drops the caller.
        let cancelled = tokio::time::timeout(Duration::ZERO, spawn_supervised_tokio(command)).await;
        assert!(
            cancelled.is_err(),
            "the requester has to be gone before its child arrives for this test to mean anything"
        );
        let _ = release.send(());

        tokio::time::sleep(Duration::from_millis(1_500)).await;
        let stranded = marker.exists();
        let _ = std::fs::remove_file(&marker);
        assert!(
            !stranded,
            "the abandoned child ran to completion: {}",
            marker.display()
        );
    }
}
