//! Group commit for the extraction job's index writes.
//!
//! The writer's cost is commits, not rows: on the measured 8 000-item tagging
//! job `COMMIT` was 85.6% of its 186s, growing from 9ms to 35ms per item as
//! the b-trees and the FTS index filled, against 3.1ms of row work per item.
//! One transaction per item therefore turns a deep inference window into a
//! serial tail — 1 573 items still queued when the last frame came back, 48.6s
//! of it.
//!
//! So writes are coalesced here, on the classic group-commit shape: the first
//! submitter flushes immediately, and everything that arrives while that group
//! is in the writer forms the next one. Nothing waits on a timer — a group
//! closes the moment the previous commit returns — so an idle writer keeps
//! today's latency and a busy one groups exactly as deeply as the backlog it
//! has, with no tuning knob to get wrong.

use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex, OnceLock};

use tokio::sync::{Mutex, oneshot};

use crate::api_error::ApiError;
use crate::db::index_writer::{IndexDbWriterMessage, OutputWriteUnit, call_index_db_writer};

type ApiResult<T> = std::result::Result<T, ApiError>;

/// The most items one transaction takes. It bounds the dirty pages a single
/// group holds and the work one rollback throws away; 256 is the deepest
/// window measured on the calibration legs, not a ceiling the sizer has.
const MAX_GROUP_ITEMS: usize = 256;

static BATCHERS: OnceLock<Mutex<HashMap<String, Arc<Batcher>>>> = OnceLock::new();

struct Batcher {
    index_db: String,
    queue: StdMutex<Queue>,
}

impl Batcher {
    /// The queue lock, poison tolerant. A flush task that panicked while
    /// holding it leaves the queue to [`FlushGuard`]; propagating the poison
    /// instead would turn every later write into a panic of its own.
    fn lock(&self) -> std::sync::MutexGuard<'_, Queue> {
        self.queue.lock().unwrap_or_else(|err| err.into_inner())
    }
}

#[derive(Default)]
struct Queue {
    pending: Vec<Submission>,
    /// True while a flush task is running. It keeps draining until the queue
    /// is empty, so a submitter that finds it set only has to wait.
    flushing: bool,
    /// Woken when the queue next goes idle — shutdown waits on these, so the
    /// writer barrier that follows really is behind every submitted write.
    idle_waiters: Vec<oneshot::Sender<()>>,
}

struct Submission {
    unit: OutputWriteUnit,
    reply: oneshot::Sender<ApiResult<()>>,
}

/// Queues one completed item's index write and waits for its own result.
///
/// The result is this item's alone: a group in which some other item failed
/// still returns `Ok` here, and the failure is reported to whoever submitted
/// it. Only losing the writer itself fails everyone in the group.
pub(crate) async fn write_output(index_db: &str, unit: OutputWriteUnit) -> ApiResult<()> {
    let rx = submit_output(index_db, unit).await;
    rx.await
        .unwrap_or_else(|_| Err(ApiError::internal("Index DB writer dropped response")))
}

/// Queues the write and hands back the channel its result will arrive on.
async fn submit_output(index_db: &str, unit: OutputWriteUnit) -> oneshot::Receiver<ApiResult<()>> {
    let batcher = batcher_for(index_db).await;
    let (reply, rx) = oneshot::channel();
    let flush = {
        let mut queue = batcher.lock();
        queue.pending.push(Submission { unit, reply });
        if queue.flushing {
            false
        } else {
            queue.flushing = true;
            true
        }
    };
    if flush {
        // Detached, so cancelling a submitter (an aborted job drops its item
        // tasks) can never strand the queue with `flushing` stuck true.
        tokio::spawn(flush_groups(batcher));
    }
    rx
}

/// Waits for every write already queued on every batcher to have been given
/// to the index writer. Shutdown calls this before the writer's own barrier,
/// which otherwise proves nothing about submissions still sitting here.
pub(crate) async fn drain_all_batchers() {
    let Some(batchers) = BATCHERS.get() else {
        return;
    };
    let all: Vec<Arc<Batcher>> = batchers.lock().await.values().cloned().collect();
    for batcher in all {
        let wait = {
            let mut queue = batcher.lock();
            if !queue.flushing && queue.pending.is_empty() {
                continue;
            }
            let (tx, rx) = oneshot::channel();
            queue.idle_waiters.push(tx);
            rx
        };
        let _ = wait.await;
    }
}

async fn batcher_for(index_db: &str) -> Arc<Batcher> {
    let batchers = BATCHERS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = batchers.lock().await;
    guard
        .entry(index_db.to_string())
        .or_insert_with(|| {
            Arc::new(Batcher {
                index_db: index_db.to_string(),
                queue: StdMutex::new(Queue::default()),
            })
        })
        .clone()
}

/// Runs when the flush task ends *without* having drained the queue — a panic
/// inside a group's write. It frees `flushing` so the next submitter spawns a
/// task, tells the writes still queued, and releases the shutdown drain.
struct FlushGuard {
    batcher: Arc<Batcher>,
    /// Set by the normal ending, which clears `flushing` itself under the
    /// lock that saw the queue empty; the guard must not touch a queue a
    /// later submitter may already own.
    drained: bool,
}

impl Drop for FlushGuard {
    fn drop(&mut self) {
        if self.drained {
            return;
        }
        let (stranded, waiters) = {
            let mut queue = self.batcher.lock();
            queue.flushing = false;
            (
                std::mem::take(&mut queue.pending),
                std::mem::take(&mut queue.idle_waiters),
            )
        };
        tracing::error!(
            index_db = self.batcher.index_db,
            queued = stranded.len(),
            "the index write batch task ended early; failing the writes it left queued"
        );
        fail_group(
            stranded.into_iter().map(|s| s.reply).collect(),
            &ApiError::internal("Index DB write batch failed"),
        );
        for waiter in waiters {
            let _ = waiter.send(());
        }
    }
}

async fn flush_groups(batcher: Arc<Batcher>) {
    let mut guard = FlushGuard {
        batcher: batcher.clone(),
        drained: false,
    };
    let idle = loop {
        let group = {
            let mut queue = batcher.lock();
            if queue.pending.is_empty() {
                // Cleared under the same lock that found the queue empty, so
                // a write arriving now spawns its own task rather than being
                // left to one that is on its way out.
                queue.flushing = false;
                guard.drained = true;
                break std::mem::take(&mut queue.idle_waiters);
            }
            let take = queue.pending.len().min(MAX_GROUP_ITEMS);
            queue.pending.drain(..take).collect::<Vec<_>>()
        };
        write_group(&batcher.index_db, group).await;
    };
    for waiter in idle {
        let _ = waiter.send(());
    }
}

/// Test-only: the index DB whose next group's write panics, so the flush
/// task's guard can be exercised. Named, so a panic armed by one test cannot
/// land in another running beside it.
#[cfg(test)]
static PANIC_ON_DB: StdMutex<Option<String>> = StdMutex::new(None);

#[cfg(test)]
fn take_armed_panic(index_db: &str) -> bool {
    let mut armed = PANIC_ON_DB.lock().unwrap_or_else(|err| err.into_inner());
    if armed.as_deref() == Some(index_db) {
        *armed = None;
        return true;
    }
    false
}

async fn write_group(index_db: &str, group: Vec<Submission>) {
    #[cfg(test)]
    if take_armed_panic(index_db) {
        // After a yield, so the test can queue writes behind the group that
        // is about to die and see what the guard does with them.
        tokio::task::yield_now().await;
        panic!("injected index write batch panic");
    }
    let mut units = Vec::with_capacity(group.len());
    let mut replies = Vec::with_capacity(group.len());
    for submission in group {
        units.push(submission.unit);
        replies.push(submission.reply);
    }
    let result = call_index_db_writer(index_db, |reply| IndexDbWriterMessage::WriteOutputs {
        units: units.clone(),
        reply,
    })
    .await;

    match result {
        // One result per unit, in the order they were sent.
        Ok(results) if results.len() == replies.len() => {
            for (reply, result) in replies.into_iter().zip(results) {
                let _ = reply.send(result);
            }
        }
        Ok(_) => fail_group(
            replies,
            &ApiError::internal("Index DB writer returned a mismatched group"),
        ),
        // Every error that reaches here is the writer's, not an item's, so
        // every submitter in the group gets the same one.
        Err(err) => fail_group(replies, &err),
    }
}

fn fail_group(replies: Vec<oneshot::Sender<ApiResult<()>>>, err: &ApiError) {
    for reply in replies {
        let _ = reply.send(Err(err.clone()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::index_writer::{extraction_test_db, tag_unit};
    use crate::test_utils::test_data_dir;

    async fn tag_links(index_db: &str) -> i64 {
        let mut conn = crate::db::open_index_db_read_no_user_data(index_db)
            .await
            .unwrap();
        sqlx::query_scalar("SELECT COUNT(*) FROM tags_items")
            .fetch_one(&mut conn)
            .await
            .unwrap()
    }

    // Shutdown aborts the job's tasks, so writes can be sitting here with
    // nobody awaiting them. The drain has to put them in the writer before
    // the writer's own barrier runs, or that barrier proves nothing.
    #[tokio::test]
    async fn the_shutdown_drain_commits_what_is_still_queued() {
        let _test_env = test_data_dir();
        let (index_db, job_id) = extraction_test_db(3).await;

        // Submitted and then abandoned, exactly as an aborted job leaves them.
        for sha in ["sha0", "sha1", "sha2"] {
            let _abandoned = submit_output(&index_db, tag_unit(job_id, sha, &["cat"])).await;
        }
        drain_all_batchers().await;

        assert_eq!(
            tag_links(&index_db).await,
            3,
            "the drain returned before the queued writes had committed"
        );
    }

    // A panic in the flush task must not wedge the queue: `flushing` goes
    // back to false, the writes it left queued are told, and the next write
    // still goes through.
    #[tokio::test]
    async fn a_panicking_flush_frees_the_queue_and_fails_its_writes() {
        let _test_env = test_data_dir();
        let (index_db, job_id) = extraction_test_db(3).await;
        *PANIC_ON_DB.lock().unwrap() = Some(index_db.clone());

        let first = submit_output(&index_db, tag_unit(job_id, "sha0", &["cat"])).await;
        // The flush task now holds the first group and is at the injection's
        // yield, so these two land in `pending` behind it.
        tokio::task::yield_now().await;
        let second = submit_output(&index_db, tag_unit(job_id, "sha1", &["hat"])).await;
        let third = submit_output(&index_db, tag_unit(job_id, "sha2", &["bat"])).await;

        assert!(
            first.await.is_err(),
            "the panicking group's own submitter loses its reply channel"
        );
        for rx in [second, third] {
            let answered = rx.await.expect("the guard answers the writes it strands");
            assert!(answered.is_err(), "a stranded write is failed, not lost");
        }
        assert_eq!(
            tag_links(&index_db).await,
            0,
            "the panicking group wrote nothing"
        );

        assert!(
            write_output(&index_db, tag_unit(job_id, "sha0", &["cat"]))
                .await
                .is_ok(),
            "the queue keeps working after a flush task dies"
        );
        assert_eq!(tag_links(&index_db).await, 1);
    }

    // Each submitter gets its own verdict, not the group's: the item that
    // could not be written fails, and its neighbours in the same transaction
    // still report success.
    #[tokio::test]
    async fn every_submitter_gets_its_own_result() {
        let _test_env = test_data_dir();
        let (index_db, job_id) = extraction_test_db(2).await;

        let (first, poisoned, second) = tokio::join!(
            write_output(&index_db, tag_unit(job_id, "sha0", &["cat"])),
            write_output(&index_db, tag_unit(job_id, "missing", &["ghost"])),
            write_output(&index_db, tag_unit(job_id, "sha1", &["hat"])),
        );
        assert!(first.is_ok());
        assert!(
            poisoned.is_err(),
            "the unwritable item must fail on its own"
        );
        assert!(second.is_ok());

        let mut conn = crate::db::open_index_db_read_no_user_data(&index_db)
            .await
            .unwrap();
        let written: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM tags_items")
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert_eq!(written, 2, "both writable items landed");
    }
}
