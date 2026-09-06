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

/// The most items one transaction takes. 256 is the deepest window the batch
/// sizer builds, so a full window commits together; the cap keeps a single
/// group's dirty pages, and what one rollback throws away, bounded.
const MAX_GROUP_ITEMS: usize = 256;

static BATCHERS: OnceLock<Mutex<HashMap<String, Arc<Batcher>>>> = OnceLock::new();

struct Batcher {
    index_db: String,
    queue: StdMutex<Queue>,
}

#[derive(Default)]
struct Queue {
    pending: Vec<Submission>,
    /// True while a flush task is running. It keeps draining until the queue
    /// is empty, so a submitter that finds it set only has to wait.
    flushing: bool,
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
    let batcher = batcher_for(index_db).await;
    let (reply, rx) = oneshot::channel();
    let flush = {
        let mut queue = batcher.queue.lock().expect("output batch queue poisoned");
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
    rx.await
        .unwrap_or_else(|_| Err(ApiError::internal("Index DB writer dropped response")))
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

async fn flush_groups(batcher: Arc<Batcher>) {
    loop {
        let group = {
            let mut queue = batcher.queue.lock().expect("output batch queue poisoned");
            if queue.pending.is_empty() {
                queue.flushing = false;
                return;
            }
            let take = queue.pending.len().min(MAX_GROUP_ITEMS);
            queue.pending.drain(..take).collect::<Vec<_>>()
        };
        write_group(&batcher.index_db, group).await;
    }
}

async fn write_group(index_db: &str, group: Vec<Submission>) {
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
        Ok(_) => fail_group(replies, "Index DB writer returned a mismatched group"),
        // `ApiError` is not cloneable, and every error that reaches here is
        // the transaction's, not an item's: the detail is the same for all.
        Err(err) => fail_group(replies, err.detail()),
    }
}

fn fail_group(replies: Vec<oneshot::Sender<ApiResult<()>>>, detail: &str) {
    for reply in replies {
        let _ = reply.send(Err(ApiError::internal(detail.to_string())));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::index_writer::{extraction_test_db, tag_unit};
    use crate::test_utils::test_data_dir;

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
