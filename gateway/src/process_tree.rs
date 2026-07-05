//! Kill-on-close job object wrapper. On Windows, every descendant of the
//! assigned process is terminated when the guard drops, covering both
//! multi-process trees (Chromium) and launchers whose real payload detaches
//! from the spawned process (msedge.exe). Used by the HTML-thumbnail browser
//! path and by inferio worker supervision. On other platforms this is a no-op
//! and `Child::kill` remains the only cleanup.

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
