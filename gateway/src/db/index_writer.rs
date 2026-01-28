use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::OnceLock,
    time::{Duration, Instant},
};

use ractor::{Actor, ActorProcessingErr, ActorRef};
use ractor::concurrency::Duration as RactorDuration;
use sqlx::SqliteConnection;
use tokio::sync::{Mutex, oneshot};

use crate::api_error::ApiError;
use crate::db::{
    file_scans::{
        add_file_scan,
        close_file_scan,
        delete_unavailable_files,
        mark_unavailable_files,
        update_file_scan,
        FileScanUpdate,
    },
    files::{
        delete_files_not_allowed_stub,
        delete_items_without_files,
        delete_file_by_path,
        delete_item_if_orphan,
        rename_file_path,
        set_blurhash,
        update_file_data,
        FileScanData,
        FileUpsertResult,
    },
    folders::{
        add_folder_to_database,
        delete_files_not_under_included_folders,
        delete_files_under_excluded_folders,
        delete_folders_not_in_list,
    },
    open_index_db_read_no_user_data,
    open_index_db_write_no_user_data,
    storage::{
        delete_orphaned_frames,
        delete_orphaned_thumbnails,
        store_frames,
        store_thumbnails,
        StoredImage,
    },
    extraction_log::delete_data_job_by_log_id,
};
use crate::db::connection::index_storage_paths_unchecked;

type ApiResult<T> = std::result::Result<T, ApiError>;
type Reply<T> = oneshot::Sender<ApiResult<T>>;
type DbFuture<'a, T> = Pin<Box<dyn Future<Output = ApiResult<T>> + Send + 'a>>;
const HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(300);
const IDLE_TIMEOUT: Duration = Duration::from_secs(300);
const CALL_RETRY_ATTEMPTS: usize = 2;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct IndexDbKey {
    index_db: String,
}

pub(crate) enum IndexDbWriterMessage {
    AddFileScan {
        scan_time: String,
        path: String,
        reply: Reply<i64>,
    },
    UpdateFileScan {
        scan_id: i64,
        update: FileScanUpdate,
        reply: Reply<()>,
    },
    CloseFileScan {
        scan_id: i64,
        end_time: String,
        reply: Reply<()>,
    },
    MarkUnavailableFiles {
        scan_id: i64,
        path: String,
        reply: Reply<(i64, i64)>,
    },
    UpdateFileData {
        time_added: String,
        scan_id: i64,
        data: FileScanData,
        reply: Reply<FileUpsertResult>,
    },
    StoreThumbnails {
        sha256: String,
        mime_type: String,
        process_version: i64,
        thumbnails: Vec<StoredImage>,
        reply: Reply<()>,
    },
    StoreFrames {
        sha256: String,
        mime_type: String,
        process_version: i64,
        frames: Vec<StoredImage>,
        reply: Reply<()>,
    },
    RenameFilePath {
        old_path: String,
        new_path: String,
        scan_id: i64,
        last_modified: String,
        reply: Reply<bool>,
    },
    DeleteFileByPath {
        path: String,
        reply: Reply<u64>,
    },
    DeleteItemIfOrphan {
        item_id: i64,
        reply: Reply<bool>,
    },
    SetBlurhash {
        sha256: String,
        blurhash: String,
        reply: Reply<()>,
    },
    DeleteUnavailableFiles {
        reply: Reply<u64>,
    },
    DeleteItemsWithoutFiles {
        batch_size: i64,
        reply: Reply<u64>,
    },
    DeleteFilesNotAllowed {
        reply: Reply<u64>,
    },
    DeleteOrphanedFrames {
        reply: Reply<u64>,
    },
    DeleteOrphanedThumbnails {
        reply: Reply<u64>,
    },
    DeleteJobData {
        log_id: i64,
        reply: Reply<()>,
    },
    AddFolderToDatabase {
        time_added: String,
        path: String,
        included: bool,
        reply: Reply<bool>,
    },
    DeleteFoldersNotInList {
        folder_paths: Vec<String>,
        included: bool,
        reply: Reply<u64>,
    },
    DeleteFilesUnderExcludedFolders {
        reply: Reply<u64>,
    },
    DeleteFilesNotUnderIncludedFolders {
        reply: Reply<u64>,
    },
    IdleCheck,
}

pub(crate) struct IndexDbWriter;

pub(crate) struct IndexDbWriterArgs {
    pub index_db: String,
    pub idle_timeout: Duration,
}

pub(crate) struct IndexDbWriterState {
    index_db: String,
    idle_timeout: Duration,
    last_used: Option<Instant>,
    conn: Option<SqliteConnection>,
}

impl IndexDbWriterState {
    async fn ensure_conn(&mut self) -> ApiResult<&mut SqliteConnection> {
        if self.conn.is_none() {
            let conn = open_index_db_write_no_user_data(&self.index_db).await?;
            self.conn = Some(conn);
        }
        Ok(self.conn.as_mut().expect("connection missing"))
    }

    async fn with_transaction<T, F>(&mut self, op: F) -> ApiResult<T>
    where
        F: for<'a> FnOnce(&'a mut SqliteConnection) -> DbFuture<'a, T>,
    {
        let mut drop_conn = false;
        let result = {
            let conn = self.ensure_conn().await?;
            if let Err(err) = begin_tx(conn).await {
                drop_conn = true;
                Err(err)
            } else {
                let result = op(conn).await;
                match result {
                    Ok(value) => {
                        if let Err(err) = commit_tx(conn).await {
                            drop_conn = true;
                            Err(err)
                        } else {
                            Ok(value)
                        }
                    }
                    Err(err) => {
                        if let Err(rb_err) = rollback_tx(conn).await {
                            drop_conn = true;
                            tracing::error!(error = ?rb_err, "failed to rollback transaction");
                        }
                        Err(err)
                    }
                }
            }
        };

        if drop_conn {
            self.conn = None;
            self.last_used = None;
        } else if self.conn.is_some() {
            self.last_used = Some(Instant::now());
        }

        result
    }
}

impl Actor for IndexDbWriter {
    type Msg = IndexDbWriterMessage;
    type State = IndexDbWriterState;
    type Arguments = IndexDbWriterArgs;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let _ = myself.send_interval(
            RactorDuration::from_secs(args.idle_timeout.as_secs()),
            || IndexDbWriterMessage::IdleCheck,
        );
        Ok(IndexDbWriterState {
            index_db: args.index_db,
            idle_timeout: args.idle_timeout,
            last_used: None,
            conn: None,
        })
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            IndexDbWriterMessage::IdleCheck => {
                if let (Some(last_used), Some(_)) = (state.last_used, state.conn.as_ref()) {
                    if last_used.elapsed() >= state.idle_timeout {
                        state.conn = None;
                        state.last_used = None;
                        tracing::info!(
                            index_db = %state.index_db,
                            "index db writer connection closed after idle timeout"
                        );
                    }
                }
            }
            IndexDbWriterMessage::AddFileScan {
                scan_time,
                path,
                reply,
            } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move { add_file_scan(conn, &scan_time, &path).await })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::UpdateFileScan {
                scan_id,
                update,
                reply,
            } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move { update_file_scan(conn, scan_id, update).await })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::CloseFileScan {
                scan_id,
                end_time,
                reply,
            } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move { close_file_scan(conn, scan_id, &end_time).await })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::MarkUnavailableFiles {
                scan_id,
                path,
                reply,
            } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move { mark_unavailable_files(conn, scan_id, &path).await })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::UpdateFileData {
                time_added,
                scan_id,
                data,
                reply,
            } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move {
                            update_file_data(conn, &time_added, scan_id, &data).await
                        })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::StoreThumbnails {
                sha256,
                mime_type,
                process_version,
                thumbnails,
                reply,
            } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move {
                            store_thumbnails(
                                conn,
                                &sha256,
                                &mime_type,
                                process_version,
                                &thumbnails,
                            )
                            .await
                        })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::StoreFrames {
                sha256,
                mime_type,
                process_version,
                frames,
                reply,
            } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move {
                            store_frames(
                                conn,
                                &sha256,
                                &mime_type,
                                process_version,
                                &frames,
                            )
                            .await
                        })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::RenameFilePath {
                old_path,
                new_path,
                scan_id,
                last_modified,
                reply,
            } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move {
                            rename_file_path(conn, &old_path, &new_path, scan_id, &last_modified)
                                .await
                        })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::DeleteFileByPath { path, reply } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move { delete_file_by_path(conn, &path).await })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::DeleteItemIfOrphan { item_id, reply } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move { delete_item_if_orphan(conn, item_id).await })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::SetBlurhash {
                sha256,
                blurhash,
                reply,
            } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move { set_blurhash(conn, &sha256, &blurhash).await })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::DeleteUnavailableFiles { reply } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move { delete_unavailable_files(conn).await })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::DeleteItemsWithoutFiles { batch_size, reply } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move { delete_items_without_files(conn, batch_size).await })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::DeleteFilesNotAllowed { reply } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move { delete_files_not_allowed_stub(conn).await })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::DeleteOrphanedFrames { reply } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move { delete_orphaned_frames(conn).await })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::DeleteOrphanedThumbnails { reply } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move { delete_orphaned_thumbnails(conn).await })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::DeleteJobData { log_id, reply } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move { delete_data_job_by_log_id(conn, log_id).await })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::AddFolderToDatabase {
                time_added,
                path,
                included,
                reply,
            } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move {
                            add_folder_to_database(conn, &time_added, &path, included).await
                        })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::DeleteFoldersNotInList {
                folder_paths,
                included,
                reply,
            } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move {
                            delete_folders_not_in_list(conn, &folder_paths, included).await
                        })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::DeleteFilesUnderExcludedFolders { reply } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move { delete_files_under_excluded_folders(conn).await })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::DeleteFilesNotUnderIncludedFolders { reply } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move {
                            delete_files_not_under_included_folders(conn).await
                        })
                    })
                    .await;
                let _ = reply.send(result);
            }
        }
        Ok(())
    }
}

pub(crate) enum IndexDbSupervisorMessage {
    GetWriter {
        index_db: String,
        force_new: bool,
        reply: Reply<ActorRef<IndexDbWriterMessage>>,
    },
    HealthCheck,
}

pub(crate) struct IndexDbSupervisor;

pub(crate) struct IndexDbSupervisorArgs {
    pub health_interval: Duration,
    pub idle_timeout: Duration,
}

pub(crate) struct IndexDbSupervisorState {
    writers: HashMap<IndexDbKey, ActorRef<IndexDbWriterMessage>>,
    idle_timeout: Duration,
}

impl Actor for IndexDbSupervisor {
    type Msg = IndexDbSupervisorMessage;
    type State = IndexDbSupervisorState;
    type Arguments = IndexDbSupervisorArgs;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let _ = myself.send_interval(
            RactorDuration::from_secs(args.health_interval.as_secs()),
            || IndexDbSupervisorMessage::HealthCheck,
        );
        Ok(IndexDbSupervisorState {
            writers: HashMap::new(),
            idle_timeout: args.idle_timeout,
        })
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            IndexDbSupervisorMessage::GetWriter {
                index_db,
                force_new,
                reply,
            } => {
                let key = IndexDbKey {
                    index_db: index_db.clone(),
                };
                if let Some(existing) = state.writers.get(&key) {
                    if force_new {
                        // Only force a respawn if the existing writer is actually dead.
                        if existing.send_message(IndexDbWriterMessage::IdleCheck).is_err() {
                            if let Some(existing) = state.writers.remove(&key) {
                                existing.stop(None);
                            }
                        } else {
                            let _ = reply.send(Ok(existing.clone()));
                            return Ok(());
                        }
                    } else {
                        let _ = reply.send(Ok(existing.clone()));
                        return Ok(());
                    }
                }

                let writer = spawn_writer(&index_db, state.idle_timeout).await;
                match writer {
                    Ok(writer) => {
                        state.writers.insert(key, writer.clone());
                        let _ = reply.send(Ok(writer));
                    }
                    Err(err) => {
                        let _ = reply.send(Err(err));
                    }
                }
            }
            IndexDbSupervisorMessage::HealthCheck => {
                let mut to_remove = Vec::new();
                for (key, writer) in state.writers.iter() {
                    let paths = index_storage_paths_unchecked(&key.index_db);
                    if !paths.index_db_file.exists() || !paths.storage_db_file.exists() {
                        tracing::warn!(
                            index_db = %key.index_db,
                            "index db files missing during health check"
                        );
                        to_remove.push(key.clone());
                        continue;
                    }

                    match ping_db(&key.index_db).await {
                        Ok(()) => {}
                        Err(err) => {
                            tracing::warn!(
                                error = ?err,
                                index_db = %key.index_db,
                                "index db health check failed"
                            );
                            to_remove.push(key.clone());
                        }
                    }

                    if !to_remove.contains(key) {
                        if writer.send_message(IndexDbWriterMessage::IdleCheck).is_err() {
                            to_remove.push(key.clone());
                        }
                    }
                }

                for key in to_remove {
                    if let Some(writer) = state.writers.remove(&key) {
                        writer.stop(None);
                    }
                }
            }
        }
        Ok(())
    }
}

static SUPERVISOR: OnceLock<Mutex<Option<ActorRef<IndexDbSupervisorMessage>>>> = OnceLock::new();

pub(crate) async fn get_index_db_writer(
    index_db: &str,
) -> ApiResult<ActorRef<IndexDbWriterMessage>> {
    get_index_db_writer_inner(index_db, false).await
}

async fn get_index_db_writer_fresh(
    index_db: &str,
) -> ApiResult<ActorRef<IndexDbWriterMessage>> {
    get_index_db_writer_inner(index_db, true).await
}

async fn get_index_db_writer_inner(
    index_db: &str,
    force_new: bool,
) -> ApiResult<ActorRef<IndexDbWriterMessage>> {
    for attempt in 0..CALL_RETRY_ATTEMPTS {
        let supervisor = if attempt == 0 {
            ensure_supervisor().await?
        } else {
            replace_supervisor().await?
        };
        let (reply, rx) = oneshot::channel();
        if supervisor
            .send_message(IndexDbSupervisorMessage::GetWriter {
                index_db: index_db.to_string(),
                force_new,
                reply,
            })
            .is_err()
        {
            continue;
        }
        return rx
            .await
            .map_err(|_| ApiError::internal("Index DB supervisor dropped response"))?;
    }
    Err(ApiError::internal("Index DB supervisor unavailable"))
}

/// Sends a request to the writer with a single retry on writer death.
/// The builder may be called more than once; use Arc/cloneable payloads if needed.
pub(crate) async fn call_index_db_writer<T, F>(
    index_db: &str,
    mut build: F,
) -> ApiResult<T>
where
    F: FnMut(Reply<T>) -> IndexDbWriterMessage,
{
    let mut last_err = None;
    for attempt in 0..CALL_RETRY_ATTEMPTS {
        let writer = if attempt == 0 {
            get_index_db_writer(index_db).await?
        } else {
            get_index_db_writer_fresh(index_db).await?
        };
        let (reply, rx) = oneshot::channel();
        let msg = build(reply);
        if writer.send_message(msg).is_err() {
            last_err = Some(ApiError::internal("Index DB writer unavailable"));
            continue;
        }

        match rx.await {
            Ok(result) => return result,
            Err(_) => {
                last_err = Some(ApiError::internal("Index DB writer dropped response"));
            }
        }
    }

    Err(last_err.unwrap_or_else(|| {
        ApiError::internal("Index DB writer unavailable")
    }))
}

async fn ensure_supervisor() -> ApiResult<ActorRef<IndexDbSupervisorMessage>> {
    let cell = SUPERVISOR.get_or_init(|| Mutex::new(None));
    let mut guard = cell.lock().await;
    if let Some(actor) = guard.as_ref() {
        return Ok(actor.clone());
    }
    let actor = spawn_supervisor().await?;
    *guard = Some(actor.clone());
    Ok(actor)
}

async fn replace_supervisor() -> ApiResult<ActorRef<IndexDbSupervisorMessage>> {
    let cell = SUPERVISOR.get_or_init(|| Mutex::new(None));
    let mut guard = cell.lock().await;
    let actor = spawn_supervisor().await?;
    *guard = Some(actor.clone());
    Ok(actor)
}

async fn spawn_supervisor() -> ApiResult<ActorRef<IndexDbSupervisorMessage>> {
    let args = IndexDbSupervisorArgs {
        health_interval: HEALTH_CHECK_INTERVAL,
        idle_timeout: IDLE_TIMEOUT,
    };
    let (actor, _handle) = Actor::spawn(None, IndexDbSupervisor, args)
    .await
    .map_err(|err| {
        tracing::error!(error = ?err, "failed to start index db supervisor");
        ApiError::internal("Failed to start index DB supervisor")
    })?;
    Ok(actor)
}

async fn spawn_writer(
    index_db: &str,
    idle_timeout: Duration,
) -> ApiResult<ActorRef<IndexDbWriterMessage>> {
    let name = format!("index-db-writer-{}", sanitize_name(index_db));
    let args = IndexDbWriterArgs {
        index_db: index_db.to_string(),
        idle_timeout,
    };
    let (actor, _handle) = Actor::spawn(Some(name), IndexDbWriter, args)
        .await
        .map_err(|err| {
            tracing::error!(error = ?err, index_db, "failed to start index db writer");
            ApiError::internal("Failed to start index DB writer")
        })?;
    Ok(actor)
}

fn sanitize_name(input: &str) -> String {
    input
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect()
}

async fn ping_db(index_db: &str) -> ApiResult<()> {
    let mut conn = open_index_db_read_no_user_data(index_db).await?;
    sqlx::query("SELECT 1")
        .execute(&mut conn)
        .await
        .map_err(|err| {
            tracing::error!(error = ?err, "failed to ping index db");
            ApiError::internal("Failed to ping index database")
        })?;
    Ok(())
}

async fn begin_tx(conn: &mut SqliteConnection) -> ApiResult<()> {
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *conn)
        .await
        .map_err(|err| {
            tracing::error!(error = ?err, "failed to begin transaction");
            ApiError::internal("Failed to begin transaction")
        })?;
    Ok(())
}

async fn commit_tx(conn: &mut SqliteConnection) -> ApiResult<()> {
    sqlx::query("COMMIT")
        .execute(&mut *conn)
        .await
        .map_err(|err| {
            tracing::error!(error = ?err, "failed to commit transaction");
            ApiError::internal("Failed to commit transaction")
        })?;
    Ok(())
}

async fn rollback_tx(conn: &mut SqliteConnection) -> ApiResult<()> {
    sqlx::query("ROLLBACK")
        .execute(&mut *conn)
        .await
        .map_err(|err| {
            tracing::error!(error = ?err, "failed to rollback transaction");
            ApiError::internal("Failed to rollback transaction")
        })?;
    Ok(())
}
