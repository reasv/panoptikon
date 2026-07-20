//! Custom SQLite scalar functions, registered on every connection.
//!
//! Registration goes through `sqlite3_auto_extension`, the same mechanism
//! sqlite-vec uses: an auto-extension's init runs on every database
//! connection opened *afterwards*, so read pools, write connections,
//! migrations and test harnesses are all covered from a single call site.
//! The alternative — a `after_connect` hook — would have to be replicated at
//! every pool/connection construction site and would silently miss the ones
//! the test harnesses build.
//!
//! Because registration only affects later connections, call
//! [`ensure_sqlite_extensions`] before opening anything.

use std::ffi::c_char;
use std::os::raw::c_int;
use std::sync::OnceLock;

use libsqlite3_sys::{
    SQLITE_DETERMINISTIC, SQLITE_NULL, SQLITE_OK, SQLITE_UTF8, sqlite3, sqlite3_api_routines,
    sqlite3_auto_extension, sqlite3_context, sqlite3_create_function_v2, sqlite3_result_int64,
    sqlite3_result_null, sqlite3_value, sqlite3_value_int64, sqlite3_value_type,
};
use sqlite_vec::sqlite3_vec_init;

use crate::api_error::ApiError;

/// splitmix64's finalizer: a full-avalanche 64-bit mixer.
fn mix64(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Map a row identity and a seed onto a pseudorandom value. Ordering by this
/// is a deterministic permutation of the result set, reproducible from the
/// seed alone — which is what makes seeded random ordering pageable and
/// cacheable (see `docs/seeded-random-order-design.md`).
///
/// The seed is mixed before being combined so that adjacent seeds (1, 2, 3 —
/// exactly what a naive minter produces) give uncorrelated orderings rather
/// than near-identical ones.
///
/// Deliberately implemented in Rust rather than as a SQL expression: SQLite
/// silently promotes integer arithmetic to REAL on overflow, so a
/// SQL-expressed 64-bit mixer would lose precision and clump. Native
/// wrapping arithmetic sidesteps that entirely.
pub(crate) fn pk_mix(id: i64, seed: i64) -> i64 {
    mix64((id as u64) ^ mix64(seed as u64)) as i64
}

/// SQLite binding for [`pk_mix`]. NULL in either argument yields NULL; in
/// practice neither is ever NULL (the id is a primary key, the seed is a
/// bound parameter).
unsafe extern "C" fn pk_mix_scalar(
    ctx: *mut sqlite3_context,
    argc: c_int,
    argv: *mut *mut sqlite3_value,
) {
    unsafe {
        if argc != 2 {
            sqlite3_result_null(ctx);
            return;
        }
        let id_value = *argv.offset(0);
        let seed_value = *argv.offset(1);
        if sqlite3_value_type(id_value) == SQLITE_NULL
            || sqlite3_value_type(seed_value) == SQLITE_NULL
        {
            sqlite3_result_null(ctx);
            return;
        }
        let id = sqlite3_value_int64(id_value);
        let seed = sqlite3_value_int64(seed_value);
        sqlite3_result_int64(ctx, pk_mix(id, seed));
    }
}

/// Auto-extension entry point registering `pk_mix` on a fresh connection.
///
/// `SQLITE_DETERMINISTIC` is accurate — the result depends only on the
/// arguments — and lets SQLite reason about the expression normally.
unsafe extern "C" fn init_custom_functions(
    db: *mut sqlite3,
    _err_msg: *mut *mut c_char,
    _api: *const sqlite3_api_routines,
) -> c_int {
    unsafe {
        sqlite3_create_function_v2(
            db,
            c"pk_mix".as_ptr(),
            2,
            SQLITE_UTF8 | SQLITE_DETERMINISTIC,
            std::ptr::null_mut(),
            Some(pk_mix_scalar),
            None,
            None,
            None,
        )
    }
}

/// Register every SQLite extension and custom function the app relies on.
/// Idempotent, and cheap after the first call.
pub(crate) fn ensure_sqlite_extensions() -> Result<(), ApiError> {
    static EXT_LOADED: OnceLock<()> = OnceLock::new();
    if EXT_LOADED.get().is_some() {
        return Ok(());
    }

    let status =
        unsafe { sqlite3_auto_extension(Some(std::mem::transmute(sqlite3_vec_init as *const ()))) };
    if status != SQLITE_OK {
        tracing::error!(status, "failed to register sqlite-vec extension");
        return Err(ApiError::internal("Failed to load sqlite-vec extension"));
    }

    let status = unsafe {
        sqlite3_auto_extension(Some(std::mem::transmute(init_custom_functions as *const ())))
    };
    if status != SQLITE_OK {
        tracing::error!(status, "failed to register custom SQL functions");
        return Err(ApiError::internal("Failed to register custom SQL functions"));
    }

    let _ = EXT_LOADED.set(());
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use sqlx::{Connection, Row, SqliteConnection};

    use super::{ensure_sqlite_extensions, pk_mix};

    /// The registration path is the part that can silently fail: the Rust
    /// function can be perfect while the auto-extension never reaches a
    /// connection. Open a connection *after* registering and call `pk_mix`
    /// through SQLite, asserting it agrees with the Rust implementation.
    #[tokio::test]
    async fn pk_mix_is_callable_from_sqlite() {
        ensure_sqlite_extensions().expect("failed to register SQLite extensions");
        let mut conn = SqliteConnection::connect("sqlite::memory:")
            .await
            .expect("failed to open in-memory database");

        for (id, seed) in [(1_i64, 42_i64), (0, 0), (12_345, -7), (i64::MAX, i64::MIN)] {
            let row = sqlx::query("SELECT pk_mix(?, ?) AS mixed")
                .bind(id)
                .bind(seed)
                .fetch_one(&mut conn)
                .await
                .expect("pk_mix is not registered on this connection");
            let mixed: i64 = row.try_get("mixed").expect("pk_mix returned a non-integer");
            assert_eq!(mixed, pk_mix(id, seed), "id={id} seed={seed}");
        }

        // NULL in either argument yields NULL rather than an error.
        let row = sqlx::query("SELECT pk_mix(NULL, 1) IS NULL AS is_null")
            .fetch_one(&mut conn)
            .await
            .expect("pk_mix rejected a NULL argument");
        let is_null: i64 = row.try_get("is_null").expect("unexpected result shape");
        assert_eq!(is_null, 1);
    }

    /// Ordering by `pk_mix` must be a stable permutation *inside SQLite*, not
    /// just in Rust — this is the property seeded random ordering sells.
    #[tokio::test]
    async fn ordering_by_pk_mix_is_stable_across_executions() {
        ensure_sqlite_extensions().expect("failed to register SQLite extensions");
        let mut conn = SqliteConnection::connect("sqlite::memory:")
            .await
            .expect("failed to open in-memory database");
        sqlx::query("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .execute(&mut conn)
            .await
            .expect("failed to create table");
        for id in 1..=200_i64 {
            sqlx::query("INSERT INTO t (id) VALUES (?)")
                .bind(id)
                .execute(&mut conn)
                .await
                .expect("failed to insert");
        }

        async fn ordered(conn: &mut SqliteConnection, seed: i64) -> Vec<i64> {
            let rows = sqlx::query("SELECT id FROM t ORDER BY pk_mix(id, ?), id")
                .bind(seed)
                .fetch_all(conn)
                .await
                .expect("failed to order by pk_mix");
            rows.iter()
                .map(|row| row.get::<i64, _>("id"))
                .collect::<Vec<i64>>()
        }

        let first = ordered(&mut conn, 99).await;
        let second = ordered(&mut conn, 99).await;
        let other_seed = ordered(&mut conn, 100).await;

        assert_eq!(first, second, "same seed must reproduce the order");
        assert_ne!(first, other_seed, "a different seed must reorder");
        assert_eq!(first.len(), 200);
        assert_eq!(
            first.iter().collect::<HashSet<_>>().len(),
            200,
            "the ordering must be a permutation, not a resample"
        );
    }

    #[test]
    fn pk_mix_is_deterministic() {
        for id in [0_i64, 1, 2, 17, 9_999, i64::MAX, i64::MIN] {
            for seed in [0_i64, 1, 42, -1, i64::MAX] {
                assert_eq!(pk_mix(id, seed), pk_mix(id, seed));
            }
        }
    }

    /// The property the ordering relies on: a seed permutes the id space, and
    /// two different seeds permute it differently.
    #[test]
    fn different_seeds_produce_different_orderings() {
        let ids: Vec<i64> = (1..=500).collect();
        let order_for = |seed: i64| {
            let mut ordered = ids.clone();
            ordered.sort_by_key(|&id| pk_mix(id, seed));
            ordered
        };

        let a = order_for(1);
        let b = order_for(2);
        assert_eq!(a, order_for(1), "same seed must reproduce the order");
        assert_ne!(a, b, "adjacent seeds must not correlate");
        assert_ne!(a, ids, "the order must not be the identity");
    }

    /// Collisions are astronomically unlikely but would silently reintroduce
    /// tie instability, so assert the mixer is injective over a realistic id
    /// range rather than assuming it.
    #[test]
    fn pk_mix_is_collision_free_over_a_large_id_range() {
        let seed = 0x5EED;
        let mixed: HashSet<i64> = (1..=100_000_i64).map(|id| pk_mix(id, seed)).collect();
        assert_eq!(mixed.len(), 100_000);
    }

    /// Sanity check that the output is spread across the range rather than
    /// clustered — a mixer that failed this would produce visibly striped
    /// "random" orderings.
    #[test]
    fn pk_mix_spreads_across_buckets() {
        let seed = 7;
        let buckets = 16_usize;
        let mut counts = vec![0_usize; buckets];
        let total = 100_000_i64;
        for id in 1..=total {
            // Bucket by the top bits, where a weak mixer clusters worst.
            let bucket = ((pk_mix(id, seed) as u64) >> 60) as usize;
            counts[bucket] += 1;
        }
        let expected = total as usize / buckets;
        for (bucket, count) in counts.iter().enumerate() {
            let deviation = (*count as f64 - expected as f64).abs() / expected as f64;
            assert!(
                deviation < 0.1,
                "bucket {bucket} held {count}, expected ~{expected}"
            );
        }
    }
}
