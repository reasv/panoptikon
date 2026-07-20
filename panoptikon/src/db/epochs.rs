//! Process-local epoch counters for search-result cache invalidation.
//!
//! One counter per index DB (bumped by the index write actor after every
//! committed transaction) and one per user data DB (bumped whenever a
//! `UserDataWrite` connection is released). Cache entries record the values
//! they were built under and are re-validated on read; a mismatch means the
//! underlying data may have changed since the entry was stored.
//!
//! The counters live here in global maps — not inside the writer actor,
//! which spins down when idle — and reset with the process, together with
//! the (also in-memory) cache they guard.
//!
//! Counters are keyed by DB *name* and never reset while the process
//! lives. Any future flow that swaps a database file behind an unchanged
//! name (delete/rename/restore alongside `invalidate_read_pools`) must
//! also bump the epoch or call `search_cache::clear` for that name, or
//! entries built against the old file will still validate.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

type EpochMap = Mutex<HashMap<String, Arc<AtomicU64>>>;

static INDEX_EPOCHS: OnceLock<EpochMap> = OnceLock::new();
static USER_DATA_EPOCHS: OnceLock<EpochMap> = OnceLock::new();

fn counter(map: &'static OnceLock<EpochMap>, db: &str) -> Arc<AtomicU64> {
    let map = map.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = map.lock().expect("epoch map poisoned");
    Arc::clone(
        guard
            .entry(db.to_string())
            .or_insert_with(|| Arc::new(AtomicU64::new(0))),
    )
}

pub(crate) fn index_epoch(index_db: &str) -> u64 {
    counter(&INDEX_EPOCHS, index_db).load(Ordering::Acquire)
}

pub(crate) fn bump_index_epoch(index_db: &str) {
    counter(&INDEX_EPOCHS, index_db).fetch_add(1, Ordering::AcqRel);
}

pub(crate) fn user_data_epoch(user_data_db: &str) -> u64 {
    counter(&USER_DATA_EPOCHS, user_data_db).load(Ordering::Acquire)
}

pub(crate) fn bump_user_data_epoch(user_data_db: &str) {
    counter(&USER_DATA_EPOCHS, user_data_db).fetch_add(1, Ordering::AcqRel);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epochs_start_at_zero_and_bump_independently() {
        assert_eq!(index_epoch("epoch-test-a"), 0);
        assert_eq!(user_data_epoch("epoch-test-a"), 0);
        bump_index_epoch("epoch-test-a");
        bump_index_epoch("epoch-test-a");
        bump_user_data_epoch("epoch-test-a");
        assert_eq!(index_epoch("epoch-test-a"), 2);
        assert_eq!(user_data_epoch("epoch-test-a"), 1);
        assert_eq!(index_epoch("epoch-test-b"), 0);
    }
}
