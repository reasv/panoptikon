//! Which index database a pinboard belongs to: the stamped rows and the
//! match rule that reads them.
//!
//! A board is **associated with the current index database** iff any of:
//!
//! - **(a)** a stamped row's `db_uuid` is the current database's identity;
//! - **(b)** a stamped row was written by *this* instance, names the current
//!   database, and its `db_uuid` belongs to no existing local database (the
//!   rebuilt-from-TOML case: a remake mints a fresh UUID, so the stamp can
//!   only be carried across by name — and only for stamps this instance
//!   wrote, or a user_data database shared between two instances that both
//!   call their database `default` would adopt the other's boards);
//! - **(c)** every item on the head version exists in the current database
//!   (100% overlap is always valid, whoever authored the board).
//!
//! Partial overlap is deliberately never a membership signal: rot is
//! inevitable and unrelated databases share incidental images. It is
//! reported (`present_count`) for display only.
//!
//! Associations are hints, never authority — every automatic verdict has a
//! manual fix path.

use std::collections::HashMap;

use sqlx::Row;

use crate::api_error::ApiError;

use super::identity::current_index_db_uuid;
use super::instance_id::instance_uuid;
use super::local_dbs::{LocalDbIdentities, canonical_index_db_name, local_index_db_identities};

type ApiResult<T> = std::result::Result<T, ApiError>;

/// Boards per stamp-row query. The library list is unpaginated, so the id
/// list is however many boards the user has; chunking keeps it clear of
/// SQLite's bound-parameter ceiling without a second query shape.
const STAMP_QUERY_CHUNK: usize = 500;

fn internal(context: &'static str) -> impl FnOnce(sqlx::Error) -> ApiError {
    move |err| {
        tracing::error!(error = %err, context, "pinboard association query failed");
        ApiError::internal(context)
    }
}

/// One stored stamp row. `db_uuid` and `instance_uuid` are server-side
/// matching keys and never leave the process.
struct StampRow {
    pinboard_id: i64,
    db_uuid: String,
    db_name: String,
    instance_uuid: String,
    last_stamped: i64,
}

/// A stamp row as the API reports it: the database's name as of the stamp
/// (the residual hint that labels a link whose database is gone), when it was
/// stamped, and whether it is the database currently being viewed.
pub(crate) struct PinboardDatabase {
    pub name: String,
    pub last_stamped: i64,
    pub associated: bool,
}

/// One board's verdict. The default is what a board with no stamps and no
/// overlap gets.
#[derive(Default)]
pub(crate) struct PinboardAssociation {
    pub associated: bool,
    /// Newest stamp first — the client's owning-database badge and opens-in
    /// link take the most recently stamped row whose name resolves locally.
    pub databases: Vec<PinboardDatabase>,
}

/// The head-version overlap counts clause (c) reads, as the listing queries
/// already computed them.
pub(crate) struct BoardOverlap {
    pub pinboard_id: i64,
    pub present_count: i64,
    pub item_count: i64,
}

/// Everything the match rule needs that is constant across one request.
///
/// Built once per request on purpose: the current database's identity is a
/// query, the instance identity takes a lock, and the local identity set can
/// probe database files.
pub(crate) struct AssociationContext {
    /// The current index database's identity — clause (a)'s key. `None` for a
    /// database that predates the identity migration.
    current_db_uuid: Option<String>,
    /// This instance's identity, or `None` when it could not be obtained (a
    /// read-only deployment, say), which simply switches clause (b) off.
    instance_uuid: Option<String>,
    /// The index database being viewed, under its folder's own spelling —
    /// clause (b)'s name compare, and (from the next step) what a stamp
    /// records as `db_name`.
    index_db: String,
    /// What the local index databases claim, for clause (b)'s gate.
    local: LocalDbIdentities,
}

impl AssociationContext {
    /// `index_db` is the name the request runs under; it is canonicalized to
    /// the folder's spelling, because the configured default is trusted
    /// unchecked and a case-only difference would otherwise split clause (b)'s
    /// name comparison in two.
    pub(crate) async fn load(conn: &mut sqlx::SqliteConnection, index_db: &str) -> Self {
        Self {
            current_db_uuid: current_index_db_uuid(conn).await,
            instance_uuid: instance_uuid().map(str::to_string),
            index_db: canonical_index_db_name(index_db),
            local: local_index_db_identities().await,
        }
    }

    /// Clauses (a) and (b) for one stamp row — the per-row verdict the API's
    /// `databases` array carries.
    fn stamp_is_current(&self, stamp: &StampRow) -> bool {
        // (a) The identity itself. Survives a folder rename, which is why it
        // is the primary key of the whole scheme.
        if self.current_db_uuid.as_deref() == Some(stamp.db_uuid.as_str()) {
            return true;
        }
        // (b) The name fallback. `Some(_) == Some(_)` only: an instance with
        // no identity matches nothing, and no row is ever written with an
        // empty sentinel (which would compare equal across instances).
        self.instance_uuid.as_deref() == Some(stamp.instance_uuid.as_str())
            && stamp.db_name == self.index_db
            && self.local.is_dangling(&stamp.db_uuid)
    }

    /// A context that matches nothing by stamp, for tests and for callers
    /// that only need the overlap clause.
    #[cfg(test)]
    pub(crate) fn for_tests(
        current_db_uuid: Option<&str>,
        instance_uuid: Option<&str>,
        index_db: &str,
        local: LocalDbIdentities,
    ) -> Self {
        Self {
            current_db_uuid: current_db_uuid.map(str::to_string),
            instance_uuid: instance_uuid.map(str::to_string),
            index_db: index_db.to_string(),
            local,
        }
    }
}

/// The association verdict for each board in `boards`, keyed by pinboard id.
///
/// One stamp-row query for the whole list, grouped in Rust — the library list
/// is unpaginated, so a per-board query would be one round trip per card.
pub(crate) async fn load_associations(
    conn: &mut sqlx::SqliteConnection,
    ctx: &AssociationContext,
    boards: &[BoardOverlap],
) -> ApiResult<HashMap<i64, PinboardAssociation>> {
    let mut stamps = fetch_stamps(
        conn,
        &boards
            .iter()
            .map(|board| board.pinboard_id)
            .collect::<Vec<_>>(),
    )
    .await?;

    let mut associations = HashMap::with_capacity(boards.len());
    for board in boards {
        let rows = stamps.remove(&board.pinboard_id).unwrap_or_default();
        let databases: Vec<PinboardDatabase> = rows
            .into_iter()
            .map(|stamp| PinboardDatabase {
                associated: ctx.stamp_is_current(&stamp),
                name: stamp.db_name,
                last_stamped: stamp.last_stamped,
            })
            .collect();
        // Clause (c): the `item_count > 0` guard is what keeps `0 == 0` from
        // admitting every empty board in every database.
        let full_overlap = board.item_count > 0 && board.present_count == board.item_count;
        associations.insert(
            board.pinboard_id,
            PinboardAssociation {
                associated: full_overlap || databases.iter().any(|db| db.associated),
                databases,
            },
        );
    }
    Ok(associations)
}

/// Every stamp row for `pinboard_ids`, newest stamp first within each board.
async fn fetch_stamps(
    conn: &mut sqlx::SqliteConnection,
    pinboard_ids: &[i64],
) -> ApiResult<HashMap<i64, Vec<StampRow>>> {
    let mut grouped: HashMap<i64, Vec<StampRow>> = HashMap::new();
    for chunk in pinboard_ids.chunks(STAMP_QUERY_CHUNK) {
        let placeholders = std::iter::repeat_n("?", chunk.len())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            r#"
            SELECT pinboard_id, db_uuid, db_name, instance_uuid, last_stamped
            FROM user_data.pinboard_databases
            WHERE pinboard_id IN ({placeholders})
            ORDER BY last_stamped DESC, db_name ASC
            "#
        );
        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()));
        for id in chunk {
            query = query.bind(id);
        }
        let rows = query
            .fetch_all(&mut *conn)
            .await
            .map_err(internal("Failed to read pinboard databases"))?;
        for row in rows {
            let stamp = StampRow {
                pinboard_id: row
                    .try_get("pinboard_id")
                    .map_err(internal("Failed to read pinboard databases"))?,
                db_uuid: row
                    .try_get("db_uuid")
                    .map_err(internal("Failed to read pinboard databases"))?,
                db_name: row
                    .try_get("db_name")
                    .map_err(internal("Failed to read pinboard databases"))?,
                instance_uuid: row
                    .try_get("instance_uuid")
                    .map_err(internal("Failed to read pinboard databases"))?,
                last_stamped: row
                    .try_get("last_stamped")
                    .map_err(internal("Failed to read pinboard databases"))?,
            };
            grouped.entry(stamp.pinboard_id).or_default().push(stamp);
        }
    }
    Ok(grouped)
}

/// Writes a stamp row directly: the write points land in the next step, and
/// the rule has to be exercised against every shape of stored row (here and
/// in the API tests that drive the endpoints' association fields).
#[cfg(test)]
pub(crate) async fn stamp_for_tests(
    conn: &mut sqlx::SqliteConnection,
    pinboard_id: i64,
    db_uuid: &str,
    db_name: &str,
    instance_uuid: &str,
    last_stamped: i64,
) {
    sqlx::query(
        r#"
        INSERT INTO user_data.pinboard_databases (
            pinboard_id, db_uuid, db_name, instance_uuid, last_stamped
        )
        VALUES (?, ?, ?, ?, ?)
        "#,
    )
    .bind(pinboard_id)
    .bind(db_uuid)
    .bind(db_name)
    .bind(instance_uuid)
    .bind(last_stamped)
    .execute(conn)
    .await
    .unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrations::setup_test_databases;

    const T0: i64 = 1_800_000_000;

    use super::stamp_for_tests as stamp;

    /// A board with no overlap at all, so only the stamp clauses can admit it.
    fn rotted(pinboard_id: i64) -> BoardOverlap {
        BoardOverlap {
            pinboard_id,
            present_count: 0,
            item_count: 4,
        }
    }

    async fn verdict(
        conn: &mut sqlx::SqliteConnection,
        ctx: &AssociationContext,
        board: BoardOverlap,
    ) -> PinboardAssociation {
        let pinboard_id = board.pinboard_id;
        load_associations(conn, ctx, &[board])
            .await
            .unwrap()
            .remove(&pinboard_id)
            .unwrap()
    }

    // Clause (a): the identity match, whatever the name says. A stamp naming
    // a different database still matches when the UUID is this database's —
    // that is what makes an association survive a folder rename.
    #[tokio::test]
    async fn identity_match_survives_a_rename() {
        let mut dbs = setup_test_databases().await;
        stamp(&mut dbs.index_conn, 1, "uuid_here", "old-name", "inst", T0).await;

        let ctx = AssociationContext::for_tests(
            Some("uuid_here"),
            Some("inst"),
            "new-name",
            LocalDbIdentities::for_tests(&[("new-name", "uuid_here")], false),
        );
        let verdict = verdict(&mut dbs.index_conn, &ctx, rotted(1)).await;
        assert!(verdict.associated);
        assert_eq!(verdict.databases.len(), 1);
        assert!(verdict.databases[0].associated);
        assert_eq!(verdict.databases[0].name, "old-name");
        assert_eq!(verdict.databases[0].last_stamped, T0);
    }

    // Clause (b), walkthrough 1: two instances sharing one user_data
    // database, both calling their index database `default`. The other
    // instance's stamp dangles here and names `default` — and must still be
    // refused, which is the entire reason the instance identity exists.
    #[tokio::test]
    async fn a_foreign_instances_stamp_is_refused() {
        let mut dbs = setup_test_databases().await;
        stamp(
            &mut dbs.index_conn,
            1,
            "uuid_a",
            "default",
            "instance_a",
            T0,
        )
        .await;

        let ctx = AssociationContext::for_tests(
            Some("uuid_b"),
            Some("instance_b"),
            "default",
            LocalDbIdentities::for_tests(&[("default", "uuid_b")], false),
        );
        let verdict = verdict(&mut dbs.index_conn, &ctx, rotted(1)).await;
        assert!(!verdict.associated);
        assert!(!verdict.databases[0].associated);
    }

    // Clause (b), walkthrough 2: the case it exists for. The same instance
    // deleted and remade `default` from its TOML, which minted a fresh UUID,
    // so the old stamp dangles — and the board comes back.
    #[tokio::test]
    async fn a_rebuilt_database_readopts_its_boards() {
        let mut dbs = setup_test_databases().await;
        stamp(&mut dbs.index_conn, 1, "uuid_old", "default", "inst", T0).await;

        let ctx = AssociationContext::for_tests(
            Some("uuid_new"),
            Some("inst"),
            "default",
            LocalDbIdentities::for_tests(&[("default", "uuid_new")], false),
        );
        let readopted = verdict(&mut dbs.index_conn, &ctx, rotted(1)).await;
        assert!(readopted.associated);
        assert!(readopted.databases[0].associated);

        // A different database selected: the name no longer matches.
        let elsewhere = AssociationContext::for_tests(
            Some("uuid_other"),
            Some("inst"),
            "photos",
            LocalDbIdentities::for_tests(&[("photos", "uuid_other")], false),
        );
        assert!(
            !verdict(&mut dbs.index_conn, &elsewhere, rotted(1))
                .await
                .associated
        );
    }

    // Clause (b), walkthrough 3: rename-reuse. "photos" became "phone" and a
    // new database took the name "photos". The stamp's UUID is alive under
    // "phone", so the gate refuses the name fallback and the board follows
    // its UUID instead.
    #[tokio::test]
    async fn rename_reuse_does_not_hand_boards_to_the_new_namesake() {
        let mut dbs = setup_test_databases().await;
        stamp(&mut dbs.index_conn, 1, "uuid_p", "photos", "inst", T0).await;
        let local =
            || LocalDbIdentities::for_tests(&[("phone", "uuid_p"), ("photos", "uuid_x")], false);

        // The new "photos" must not adopt it...
        let namesake =
            AssociationContext::for_tests(Some("uuid_x"), Some("inst"), "photos", local());
        assert!(
            !verdict(&mut dbs.index_conn, &namesake, rotted(1))
                .await
                .associated
        );

        // ...while the renamed original still owns it, by clause (a).
        let renamed = AssociationContext::for_tests(Some("uuid_p"), Some("inst"), "phone", local());
        assert!(
            verdict(&mut dbs.index_conn, &renamed, rotted(1))
                .await
                .associated
        );
    }

    // The gate fails closed: with a database that could not be interrogated
    // in the folder listing, "this UUID lives nowhere here" is unprovable, so
    // the name fallback is refused. Clause (a) is unaffected by it.
    #[tokio::test]
    async fn an_unknown_probe_refuses_the_name_fallback() {
        let mut dbs = setup_test_databases().await;
        stamp(&mut dbs.index_conn, 1, "uuid_old", "default", "inst", T0).await;
        stamp(&mut dbs.index_conn, 2, "uuid_new", "default", "inst", T0).await;

        let ctx = AssociationContext::for_tests(
            Some("uuid_new"),
            Some("inst"),
            "default",
            LocalDbIdentities::for_tests(&[("default", "uuid_new")], true),
        );
        assert!(
            !verdict(&mut dbs.index_conn, &ctx, rotted(1))
                .await
                .associated
        );
        assert!(
            verdict(&mut dbs.index_conn, &ctx, rotted(2))
                .await
                .associated
        );
    }

    // No instance identity (a read-only deployment, an unwritable data
    // folder) switches clause (b) off entirely, rather than matching every
    // same-named database everywhere.
    #[tokio::test]
    async fn without_an_instance_identity_the_name_fallback_never_fires() {
        let mut dbs = setup_test_databases().await;
        stamp(&mut dbs.index_conn, 1, "uuid_old", "default", "inst", T0).await;

        let ctx = AssociationContext::for_tests(
            Some("uuid_new"),
            None,
            "default",
            LocalDbIdentities::for_tests(&[("default", "uuid_new")], false),
        );
        assert!(
            !verdict(&mut dbs.index_conn, &ctx, rotted(1))
                .await
                .associated
        );
    }

    // Clause (c): 100% overlap admits a board nothing stamped, partial
    // overlap never does, and an empty board is not "fully present".
    #[tokio::test]
    async fn full_overlap_admits_and_partial_never_does() {
        let mut dbs = setup_test_databases().await;
        let ctx = AssociationContext::for_tests(
            Some("uuid_here"),
            Some("inst"),
            "default",
            LocalDbIdentities::for_tests(&[("default", "uuid_here")], false),
        );

        let cases = [
            (4, 4, true),
            (3, 4, false),
            (0, 4, false),
            // Every empty board would otherwise be associated everywhere.
            (0, 0, false),
        ];
        for (present_count, item_count, expected) in cases {
            let board = BoardOverlap {
                pinboard_id: 1,
                present_count,
                item_count,
            };
            assert_eq!(
                verdict(&mut dbs.index_conn, &ctx, board).await.associated,
                expected,
                "{present_count}/{item_count}"
            );
        }
    }

    // Several stamps on one board: each row carries its own verdict, the
    // board is associated if any of them matches, and the rows come back
    // newest first (the client's opens-in link takes the first that resolves).
    #[tokio::test]
    async fn stamp_rows_carry_per_row_verdicts_newest_first() {
        let mut dbs = setup_test_databases().await;
        stamp(&mut dbs.index_conn, 1, "uuid_a", "archive", "inst", T0).await;
        stamp(&mut dbs.index_conn, 1, "uuid_b", "current", "inst", T0 + 10).await;

        let ctx = AssociationContext::for_tests(
            Some("uuid_b"),
            Some("inst"),
            "current",
            LocalDbIdentities::for_tests(&[("current", "uuid_b"), ("archive", "uuid_a")], false),
        );
        let verdict = verdict(&mut dbs.index_conn, &ctx, rotted(1)).await;
        assert!(verdict.associated);
        let names: Vec<&str> = verdict
            .databases
            .iter()
            .map(|db| db.name.as_str())
            .collect();
        assert_eq!(names, vec!["current", "archive"]);
        assert_eq!(
            verdict
                .databases
                .iter()
                .map(|db| db.associated)
                .collect::<Vec<_>>(),
            vec![true, false]
        );
    }

    // A board nobody stamped and nothing overlaps is simply not associated,
    // and it still gets an entry (the caller maps every board it asked for).
    #[tokio::test]
    async fn boards_without_stamps_are_not_associated() {
        let mut dbs = setup_test_databases().await;
        let ctx = AssociationContext::for_tests(
            Some("uuid_here"),
            Some("inst"),
            "default",
            LocalDbIdentities::for_tests(&[("default", "uuid_here")], false),
        );
        let verdict = verdict(&mut dbs.index_conn, &ctx, rotted(7)).await;
        assert!(!verdict.associated);
        assert!(verdict.databases.is_empty());
    }
}
