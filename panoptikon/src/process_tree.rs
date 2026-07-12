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

/// Keep console signals for the gateway alone: a Ctrl-C that reached the
/// children directly would kill them before the supervisor is told to stop,
/// logging spurious "exited unexpectedly" noise mid-shutdown and skipping
/// graceful teardown. Same treatment the Python router gave its children
/// (CREATE_NEW_PROCESS_GROUP on Windows, setsid on Unix); shutdown delivery
/// is unaffected — supervisors stop children via their own ladders
/// (TerminateProcess/SIGKILL and the job object), never console signals.
pub(crate) fn detach_from_console(command: &mut tokio::process::Command) {
    #[cfg(windows)]
    {
        use windows_sys::Win32::System::Threading::{CREATE_NEW_PROCESS_GROUP, CREATE_NO_WINDOW};
        command.creation_flags(CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW);
    }
    #[cfg(unix)]
    {
        command.process_group(0);
    }
}

/// Make the child die when the gateway does, even when no gateway code runs
/// (second-Ctrl-C `process::exit`, the hard-exit timer, an external SIGKILL
/// or OOM kill of the gateway — none of which run destructors, so
/// `kill_on_drop` never fires). Windows is a no-op: the kill-on-close job
/// object already makes worker death a kernel-enforced consequence of
/// gateway death. On Linux the equivalent is PR_SET_PDEATHSIG: the kernel
/// delivers SIGKILL to the child when the spawning *thread* dies — safe
/// here because spawns happen on tokio core worker threads, which live
/// until the runtime (and thus the process) goes down. The fork→prctl gap
/// is closed by re-checking the parent after arming: if the gateway died in
/// between, the signal never armed, so the child exits itself.
/// macOS has no PR_SET_PDEATHSIG equivalent (prctl is Linux-only), so there
/// this is a no-op: `kill_process_group` still covers every orderly shutdown
/// path, and only a gateway death where no gateway code runs can leave the
/// child behind.
pub(crate) fn die_with_parent(command: &mut tokio::process::Command) {
    #[cfg(target_os = "linux")]
    {
        let gateway = std::process::id() as libc::pid_t;
        // SAFETY: runs between fork and exec in the child; prctl, getppid,
        // and _exit are async-signal-safe.
        unsafe {
            command.pre_exec(move || {
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                if libc::getppid() != gateway {
                    libc::_exit(127);
                }
                Ok(())
            });
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = command;
    }
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
    #[cfg(unix)]
    if let Some(pid) = child.id() {
        // Errors (ESRCH: group already gone) are irrelevant by design.
        unsafe {
            libc::kill(-(pid as libc::pid_t), libc::SIGKILL);
        }
    }
    #[cfg(not(unix))]
    {
        let _ = child;
    }
}

pub(crate) struct JobGuard {
    #[cfg(windows)]
    _job: Option<windows_job::Job>,
}

impl JobGuard {
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
