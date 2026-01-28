
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use notify::{Event, EventKind, PollWatcher, RecommendedWatcher, RecursiveMode, Watcher};
use notify::event::{ModifyKind, RenameMode};
use ractor::factory::{
    Factory, FactoryArguments, FactoryMessage, Job, JobOptions, Worker, WorkerBuilder,
    queues, routing,
};
use ractor::{Actor, ActorProcessingErr, ActorRef};
use ractor::concurrency::Duration as RactorDuration;
use tokio::sync::{OnceCell, oneshot};

use crate::api_error::ApiError;
use crate::db::{
    file_scans::{
        FileScanUpdate, get_open_file_scan_id,
    },
    files::{
        FileDeleteInfo, FileUpsertResult, count_files_for_item, get_file_delete_info,
    },
    index_writer::{call_index_db_writer, IndexDbWriterMessage},
    open_index_db_read,
    storage::{has_frame, has_thumbnail},
    system_config::{SystemConfig, SystemConfigStore},
};
use crate::db::files::has_blurhash;
use crate::jobs::files::{
    FileProcessError, PreparedFile, ScanOptions, build_extension_set,
    build_file_scan_data, check_folder_validity, current_iso_timestamp, deduplicate_paths,
    get_last_modified_time_and_size, has_allowed_extension, is_excluded, is_hidden_or_temp,
    normalize_path, process_file,
};

type ApiResult<T> = Result<T, ApiError>;

const CONTINUOUS_PATH_SENTINEL: &str = "<continuous>";
const SUPERVISOR_RESYNC_INTERVAL: Duration = Duration::from_secs(300);

#[derive(Clone)]
struct FileWork {
    path: PathBuf,
    config: Arc<SystemConfig>,
    epoch: u64,
    scan_time: String,
    reply_to: ActorRef<ContinuousScanMessage>,
}

struct ContinuousWorker;
impl Worker for ContinuousWorker {
    type Key = ();
    type Message = FileWork;
    type Arguments = ();
    type State = ();

    async fn pre_start(
        &self,
        _wid: ractor::factory::WorkerId,
        _factory: &ActorRef<FactoryMessage<Self::Key, Self::Message>>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(())
    }

    async fn handle(
        &self,
        _wid: ractor::factory::WorkerId,
        _factory: &ActorRef<FactoryMessage<Self::Key, Self::Message>>,
        job: Job<Self::Key, Self::Message>,
        _state: &mut Self::State,
    ) -> Result<Self::Key, ActorProcessingErr> {
        let FileWork {
            path,
            config,
            epoch,
            scan_time,
            reply_to,
        } = job.msg;

        let result = tokio::task::spawn_blocking(move || process_file(path, &config))
            .await
            .map_err(|err| FileProcessError::Worker(err.to_string()))
            .and_then(|res| res);

        let _ = reply_to.cast(ContinuousScanMessage::WorkerResult {
            epoch,
            scan_time,
            result,
        });
        Ok(job.key)
    }
}

struct ContinuousWorkerBuilder;

impl WorkerBuilder<ContinuousWorker, ()> for ContinuousWorkerBuilder {
    fn build(&mut self, _wid: usize) -> (ContinuousWorker, ()) {
        (ContinuousWorker, ())
    }
}

#[derive(Debug)]
pub(crate) enum FsEvent {
    Create(PathBuf),
    Modify(PathBuf),
    Remove(PathBuf),
    Rename { from: PathBuf, to: PathBuf },
    Overflow,
}

pub(crate) enum ContinuousScanMessage {
    Pause { reply: oneshot::Sender<()> },
    Resume,
    UpdateConfig { config: SystemConfig },
    FsEvent(FsEvent),
    WorkerResult {
        epoch: u64,
        scan_time: String,
        result: Result<PreparedFile, FileProcessError>,
    },
    Shutdown,
}

pub(crate) struct ContinuousScanActor;

pub(crate) struct ContinuousScanActorArgs {
    pub index_db: String,
    pub user_data_db: String,
    pub data_dir: PathBuf,
    pub enable_watcher: bool,
}

struct ScanStats {
    new_items: i64,
    unchanged_files: i64,
    new_files: i64,
    modified_files: i64,
    marked_unavailable: i64,
    errors: i64,
    total_available: i64,
    false_changes: i64,
    metadata_time: f64,
    hashing_time: f64,
    thumbgen_time: f64,
    blurhash_time: f64,
}

impl ScanStats {
    fn new() -> Self {
        Self {
            new_items: 0,
            unchanged_files: 0,
            new_files: 0,
            modified_files: 0,
            marked_unavailable: 0,
            errors: 0,
            total_available: 0,
            false_changes: 0,
            metadata_time: 0.0,
            hashing_time: 0.0,
            thumbgen_time: 0.0,
            blurhash_time: 0.0,
        }
    }
}

enum WatcherHandle {
    Recommended(RecommendedWatcher),
    Poll(PollWatcher),
}

pub(crate) struct ContinuousScanState {
    index_db: String,
    user_data_db: String,
    config_store: SystemConfigStore,
    config: SystemConfig,
    included_roots: Vec<PathBuf>,
    excluded_roots: Vec<PathBuf>,
    allowed_extensions: HashSet<String>,
    scan_id: Option<i64>,
    scan_time: Option<String>,
    stats: ScanStats,
    epoch: u64,
    paused: bool,
    paused_for_job: bool,
    actor_ref: ActorRef<ContinuousScanMessage>,
    factory: ActorRef<FactoryMessage<(), FileWork>>,
    factory_handle: Option<ractor::concurrency::JoinHandle<()>>,
    watcher: Option<WatcherHandle>,
    enable_watcher: bool,
}
impl ContinuousScanState {
    fn reset_stats(&mut self) {
        self.stats = ScanStats::new();
    }

    fn refresh_roots(&mut self) {
        let mut included = self.config.included_folders.clone();
        included.retain(|folder| check_folder_validity(folder));
        let deduped = deduplicate_paths(&included);
        self.included_roots = deduped
            .iter()
            .map(|path| normalize_path(path, true))
            .collect();

        self.excluded_roots = self
            .config
            .excluded_folders
            .iter()
            .map(|path| normalize_path(path, true))
            .collect();
        self.allowed_extensions = build_extension_set(&self.config);
    }

    async fn start_scan(&mut self) -> ApiResult<()> {
        let scan_time = current_iso_timestamp();
        let scan_id = call_index_db_writer(&self.index_db, |reply| {
            IndexDbWriterMessage::AddFileScan {
                scan_time: scan_time.clone(),
                path: CONTINUOUS_PATH_SENTINEL.to_string(),
                reply,
            }
        })
        .await?;
        self.scan_id = Some(scan_id);
        self.scan_time = Some(scan_time);
        self.reset_stats();
        Ok(())
    }

    async fn close_scan(&mut self) -> ApiResult<()> {
        let Some(scan_id) = self.scan_id.take() else {
            return Ok(());
        };
        let end_time = current_iso_timestamp();
        let update = FileScanUpdate {
            end_time,
            new_items: self.stats.new_items,
            unchanged_files: self.stats.unchanged_files,
            new_files: self.stats.new_files,
            modified_files: self.stats.modified_files,
            marked_unavailable: self.stats.marked_unavailable,
            errors: self.stats.errors,
            total_available: self.stats.total_available,
            false_changes: self.stats.false_changes,
            metadata_time: self.stats.metadata_time,
            hashing_time: self.stats.hashing_time,
            thumbgen_time: self.stats.thumbgen_time,
            blurhash_time: self.stats.blurhash_time,
        };
        call_index_db_writer(&self.index_db, |reply| {
            IndexDbWriterMessage::UpdateFileScan {
                scan_id,
                update: update.clone(),
                reply,
            }
        })
        .await?;
        self.scan_time = None;
        self.reset_stats();
        Ok(())
    }

    async fn close_stale_scan(&self) -> ApiResult<()> {
        let mut conn = open_index_db_read(&self.index_db, &self.user_data_db).await?;
        if let Some(scan_id) = get_open_file_scan_id(&mut conn, CONTINUOUS_PATH_SENTINEL).await? {
            let end_time = current_iso_timestamp();
            call_index_db_writer(&self.index_db, |reply| {
                IndexDbWriterMessage::CloseFileScan {
                    scan_id,
                    end_time: end_time.clone(),
                    reply,
                }
            })
            .await?;
        }
        Ok(())
    }

    fn should_process_path(&self, path: &Path) -> bool {
        if self.included_roots.is_empty() {
            return false;
        }
        if is_hidden_or_temp(path) {
            return false;
        }
        if !has_allowed_extension(path, &self.allowed_extensions) {
            return false;
        }
        let is_included = self.included_roots.iter().any(|root| path.starts_with(root));
        if !is_included {
            return false;
        }
        if is_excluded(path, &self.excluded_roots) {
            return false;
        }
        true
    }

    async fn handle_remove(&mut self, path: PathBuf) -> ApiResult<()> {
        if self.paused {
            return Ok(());
        }
        if !self.config.remove_unavailable_files {
            return Ok(());
        }
        if path.exists() {
            return Ok(());
        }
        if !self.should_process_path(&path) {
            return Ok(());
        }
        let mut conn = open_index_db_read(&self.index_db, &self.user_data_db).await?;
        let Some(FileDeleteInfo {
            item_id,
            scan_id,
            ..
        }) = get_file_delete_info(&mut conn, path.to_string_lossy().as_ref()).await?
        else {
            return Ok(());
        };

        let current_scan = self.scan_id.unwrap_or_default();
        let safe_delete = scan_id == current_scan
            || count_files_for_item(&mut conn, item_id).await? > 1;
        if !safe_delete {
            return Ok(());
        }

        let _ = call_index_db_writer(&self.index_db, |reply| {
            IndexDbWriterMessage::DeleteFileByPath {
                path: path.to_string_lossy().to_string(),
                reply,
            }
        })
        .await?;
        let _ = call_index_db_writer(&self.index_db, |reply| {
            IndexDbWriterMessage::DeleteItemIfOrphan { item_id, reply }
        })
        .await?;
        Ok(())
    }

    async fn handle_rename(&mut self, from: PathBuf, to: PathBuf) -> ApiResult<()> {
        if self.paused {
            return Ok(());
        }
        if !self.should_process_path(&to) {
            return Ok(());
        }
        if !to.exists() {
            return Ok(());
        }
        let scan_id = match self.scan_id {
            Some(scan_id) => scan_id,
            None => return Ok(()),
        };
        let last_modified = match get_last_modified_time_and_size(&to) {
            Ok((time, _)) => time,
            Err(_) => return Ok(()),
        };
        let renamed = call_index_db_writer(&self.index_db, |reply| {
            IndexDbWriterMessage::RenameFilePath {
                old_path: from.to_string_lossy().to_string(),
                new_path: to.to_string_lossy().to_string(),
                scan_id,
                last_modified: last_modified.clone(),
                reply,
            }
        })
        .await?;
        if renamed {
            self.stats.unchanged_files += 1;
            self.stats.total_available += 1;
            return Ok(());
        }

        self.dispatch_path(to);
        Ok(())
    }

    fn dispatch_path(&self, path: PathBuf) {
        if self.paused {
            return;
        }
        if !self.should_process_path(&path) {
            return;
        }
        if let Ok(metadata) = std::fs::metadata(&path) {
            if !metadata.is_file() {
                return;
            }
        } else {
            return;
        }
        let scan_time = match &self.scan_time {
            Some(value) => value.clone(),
            None => current_iso_timestamp(),
        };
        let msg = FileWork {
            path,
            config: Arc::new(self.config.clone()),
            epoch: self.epoch,
            scan_time,
            reply_to: self.actor_ref.clone(),
        };
        let _ = self.factory.cast(FactoryMessage::Dispatch(Job {
            key: (),
            msg,
            options: JobOptions::default(),
            accepted: None,
        }));
    }
}
impl ContinuousScanActor {
    async fn build_factory(
        worker_count: usize,
    ) -> Result<(ActorRef<FactoryMessage<(), FileWork>>, ractor::concurrency::JoinHandle<()>), ActorProcessingErr> {
        let factory_def = Factory::<
            (),
            FileWork,
            (),
            ContinuousWorker,
            routing::QueuerRouting<(), FileWork>,
            queues::DefaultQueue<(), FileWork>,
        >::default();

        let args = FactoryArguments::builder()
            .worker_builder(Box::new(ContinuousWorkerBuilder))
            .queue(Default::default())
            .router(Default::default())
            .num_initial_workers(worker_count)
            .build();

        let (factory, handle) = Actor::spawn(None, factory_def, args)
            .await
            .map_err(|err| ActorProcessingErr::from(format!("factory spawn failed: {err:?}")))?;
        Ok((factory, handle))
    }

    fn map_event(event: Event) -> Vec<FsEvent> {
        match event.kind {
            EventKind::Create(_) => event.paths.into_iter().map(FsEvent::Create).collect(),
            EventKind::Modify(ModifyKind::Name(RenameMode::Both)) => {
                if event.paths.len() >= 2 {
                    vec![FsEvent::Rename {
                        from: event.paths[0].clone(),
                        to: event.paths[1].clone(),
                    }]
                } else {
                    Vec::new()
                }
            }
            EventKind::Modify(ModifyKind::Name(RenameMode::From)) => event
                .paths
                .into_iter()
                .map(FsEvent::Remove)
                .collect(),
            EventKind::Modify(ModifyKind::Name(RenameMode::To)) => event
                .paths
                .into_iter()
                .map(FsEvent::Create)
                .collect(),
            EventKind::Modify(_) => event.paths.into_iter().map(FsEvent::Modify).collect(),
            EventKind::Remove(_) => event.paths.into_iter().map(FsEvent::Remove).collect(),
            EventKind::Other => vec![FsEvent::Overflow],
            _ => Vec::new(),
        }
    }
}

impl Actor for ContinuousScanActor {
    type Msg = ContinuousScanMessage;
    type State = ContinuousScanState;
    type Arguments = ContinuousScanActorArgs;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let config_store = SystemConfigStore::new(args.data_dir);
        let config = config_store
            .load(&args.index_db)
            .map_err(|err| ActorProcessingErr::from(format!("{err:?}")))?;

        let options = ScanOptions::default();
        let (factory, handle) = Self::build_factory(options.worker_count).await?;

        let mut state = ContinuousScanState {
            index_db: args.index_db,
            user_data_db: args.user_data_db,
            config_store,
            config,
            included_roots: Vec::new(),
            excluded_roots: Vec::new(),
            allowed_extensions: HashSet::new(),
            scan_id: None,
            scan_time: None,
            stats: ScanStats::new(),
            epoch: 0,
            paused: false,
            paused_for_job: false,
            actor_ref: myself.clone(),
            factory,
            factory_handle: Some(handle),
            watcher: None,
            enable_watcher: args.enable_watcher,
        };

        state.refresh_roots();
        let _ = state.close_stale_scan().await;
        if state.config.continuous_filescan {
            let _ = state.start_scan().await;
            if state.enable_watcher {
                let watcher = start_watcher(
                    myself.clone(),
                    &state.included_roots,
                    state.config.continuous_filescan_poll_interval_secs,
                );
                state.watcher = watcher.ok();
            }
        } else {
            state.paused = true;
        }

        Ok(state)
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            ContinuousScanMessage::Pause { reply } => {
                state.paused = true;
                state.paused_for_job = true;
                state.epoch = state.epoch.wrapping_add(1);
                state.watcher = None;
                let _ = state.close_scan().await;
                let _ = reply.send(());
            }
            ContinuousScanMessage::Resume => {
                state.paused_for_job = false;
                state.config = match state.config_store.load(&state.index_db) {
                    Ok(config) => config,
                    Err(err) => {
                        tracing::error!(error = ?err, "failed to reload continuous scan config");
                        return Ok(());
                    }
                };
                state.refresh_roots();
                if !state.config.continuous_filescan {
                    state.paused = true;
                    return Ok(());
                }
                state.paused = false;
                state.epoch = state.epoch.wrapping_add(1);
                let _ = state.start_scan().await;
                if state.enable_watcher {
                    let watcher = start_watcher(
                        myself.clone(),
                        &state.included_roots,
                        state.config.continuous_filescan_poll_interval_secs,
                    );
                    state.watcher = watcher.ok();
                }
            }
            ContinuousScanMessage::UpdateConfig { config } => {
                let was_enabled = state.config.continuous_filescan;
                let now_enabled = config.continuous_filescan;
                state.config = config;
                state.refresh_roots();
                if !was_enabled && now_enabled {
                    if !state.paused_for_job {
                        state.paused = false;
                        state.epoch = state.epoch.wrapping_add(1);
                        let _ = state.start_scan().await;
                        if state.enable_watcher {
                            let watcher = start_watcher(
                                myself.clone(),
                                &state.included_roots,
                                state.config.continuous_filescan_poll_interval_secs,
                            );
                            state.watcher = watcher.ok();
                        }
                    }
                } else if was_enabled && !now_enabled {
                    state.paused = true;
                    state.epoch = state.epoch.wrapping_add(1);
                    state.watcher = None;
                    let _ = state.close_scan().await;
                }
            }
            ContinuousScanMessage::FsEvent(event) => {
                if state.paused {
                    return Ok(());
                }
                match event {
                    FsEvent::Create(path) => state.dispatch_path(path),
                    FsEvent::Modify(path) => state.dispatch_path(path),
                    FsEvent::Remove(path) => {
                        let _ = state.handle_remove(path).await;
                    }
                    FsEvent::Rename { from, to } => {
                        let _ = state.handle_rename(from, to).await;
                    }
                    FsEvent::Overflow => {
                        tracing::warn!(
                            index_db = %state.index_db,
                            "continuous scan watcher overflow or unknown event"
                        );
                    }
                }
            }
            ContinuousScanMessage::WorkerResult {
                epoch,
                scan_time,
                result,
            } => {
                if state.paused || epoch != state.epoch {
                    return Ok(());
                }
                let processed = match result {
                    Ok(processed) => processed,
                    Err(_) => {
                        state.stats.errors += 1;
                        return Ok(());
                    }
                };

                state.stats.hashing_time += processed.hash_time;
                state.stats.metadata_time += processed.metadata_time;
                state.stats.thumbgen_time += processed.thumb_time;
                state.stats.blurhash_time += processed.blurhash_time;

                let mut conn = match open_index_db_read(&state.index_db, &state.user_data_db).await {
                    Ok(conn) => conn,
                    Err(err) => {
                        tracing::error!(error = ?err, "failed to open read connection");
                        state.stats.errors += 1;
                        return Ok(());
                    }
                };
                let file_data = match build_file_scan_data(&mut conn, processed, &scan_time).await {
                    Ok(data) => data,
                    Err(err) => {
                        tracing::error!(error = ?err, "failed to build file scan data");
                        state.stats.errors += 1;
                        return Ok(());
                    }
                };

                let false_change = file_data.new_file_hash == false && file_data.new_file_timestamp;
                if false_change {
                    state.stats.false_changes += 1;
                }

                if !file_data.thumbnails.is_empty() {
                    if let Ok(mut thumb_conn) =
                        open_index_db_read(&state.index_db, &state.user_data_db).await
                    {
                        if let Ok(has_thumb) = has_thumbnail(&mut thumb_conn, &file_data.sha256, 1).await {
                            if !has_thumb {
                                let _ = call_index_db_writer(&state.index_db, |reply| {
                                    IndexDbWriterMessage::StoreThumbnails {
                                        sha256: file_data.sha256.clone(),
                                        mime_type: file_data.mime_type.clone(),
                                        process_version: 1,
                                        thumbnails: file_data.thumbnails.clone(),
                                        reply,
                                    }
                                })
                                .await;
                            }
                        }
                    }
                }

                if !file_data.frames.is_empty() {
                    if let Ok(mut frame_conn) =
                        open_index_db_read(&state.index_db, &state.user_data_db).await
                    {
                        if let Ok(has_frame) = has_frame(&mut frame_conn, &file_data.sha256, 1).await {
                            if !has_frame {
                                let _ = call_index_db_writer(&state.index_db, |reply| {
                                    IndexDbWriterMessage::StoreFrames {
                                        sha256: file_data.sha256.clone(),
                                        mime_type: file_data.mime_type.clone(),
                                        process_version: 1,
                                        frames: file_data.frames.clone(),
                                        reply,
                                    }
                                })
                                .await;
                            }
                        }
                    }
                }

                if let Some(blurhash) = &file_data.blurhash {
                    if let Ok(mut blur_conn) =
                        open_index_db_read(&state.index_db, &state.user_data_db).await
                    {
                        if let Ok(has_value) = has_blurhash(&mut blur_conn, &file_data.sha256).await {
                            if !has_value {
                                let _ = call_index_db_writer(&state.index_db, |reply| {
                                    IndexDbWriterMessage::SetBlurhash {
                                        sha256: file_data.sha256.clone(),
                                        blurhash: blurhash.clone(),
                                        reply,
                                    }
                                })
                                .await;
                            }
                        }
                    }
                }

                let update_result = call_index_db_writer(&state.index_db, |reply| {
                    IndexDbWriterMessage::UpdateFileData {
                        time_added: file_data.time_added.clone(),
                        scan_id: state.scan_id.unwrap_or_default(),
                        data: file_data.data.clone(),
                        reply,
                    }
                })
                .await;

                match update_result {
                    Ok(FileUpsertResult {
                        item_inserted,
                        file_updated,
                        file_deleted,
                        file_inserted,
                    }) => {
                        if item_inserted {
                            state.stats.new_items += 1;
                        }
                        if file_updated {
                            state.stats.unchanged_files += 1;
                        } else if file_deleted {
                            state.stats.modified_files += 1;
                        } else if file_inserted {
                            state.stats.new_files += 1;
                        }
                        state.stats.total_available += 1;
                    }
                    Err(err) => {
                        tracing::error!(error = ?err, "failed to update file data");
                        state.stats.errors += 1;
                    }
                }
            }
            ContinuousScanMessage::Shutdown => {
                let _ = state.close_scan().await;
                myself.stop(None);
            }
        }
        Ok(())
    }

    async fn post_stop(
        &self,
        _myself: ActorRef<Self::Msg>,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        let _ = state.close_scan().await;
        state.watcher = None;
        if let Some(handle) = state.factory_handle.take() {
            state.factory.stop(None);
            let _ = handle.await;
        }
        Ok(())
    }
}
fn start_watcher(
    actor: ActorRef<ContinuousScanMessage>,
    roots: &[PathBuf],
    poll_interval_secs: Option<u64>,
) -> Result<WatcherHandle, notify::Error> {
    let handler = move |res| match res {
        Ok(event) => {
            for mapped in ContinuousScanActor::map_event(event) {
                let _ = actor.cast(ContinuousScanMessage::FsEvent(mapped));
            }
        }
        Err(err) => {
            tracing::error!(error = ?err, "continuous scan watcher error");
        }
    };

    if let Some(interval) = poll_interval_secs {
        let config = notify::Config::default().with_poll_interval(Duration::from_secs(interval));
        let mut watcher = PollWatcher::new(handler, config)?;
        for root in roots {
            watcher.watch(root, RecursiveMode::Recursive)?;
        }
        return Ok(WatcherHandle::Poll(watcher));
    }

    let mut watcher = RecommendedWatcher::new(handler, notify::Config::default())?;
    for root in roots {
        watcher.watch(root, RecursiveMode::Recursive)?;
    }

    Ok(WatcherHandle::Recommended(watcher))
}

pub(crate) enum ContinuousScanSupervisorMessage {
    ResyncFromDisk,
    ConfigChanged { index_db: String },
    PauseForJob {
        index_db: String,
        reply: oneshot::Sender<()>,
    },
    ResumeAfterJob { index_db: String },
}

pub(crate) struct ContinuousScanSupervisor;

pub(crate) struct ContinuousScanSupervisorArgs {
    pub data_dir: PathBuf,
}

pub(crate) struct ContinuousScanSupervisorState {
    data_dir: PathBuf,
    config_store: SystemConfigStore,
    actors: HashMap<String, ActorRef<ContinuousScanMessage>>,
    watcher: Option<RecommendedWatcher>,
}

impl Actor for ContinuousScanSupervisor {
    type Msg = ContinuousScanSupervisorMessage;
    type State = ContinuousScanSupervisorState;
    type Arguments = ContinuousScanSupervisorArgs;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let config_store = SystemConfigStore::new(args.data_dir.clone());
        let watcher = start_supervisor_watcher(myself.clone(), &args.data_dir).ok();
        let mut state = ContinuousScanSupervisorState {
            data_dir: args.data_dir,
            config_store,
            actors: HashMap::new(),
            watcher,
        };
        let _ = myself.send_interval(
            RactorDuration::from_secs(SUPERVISOR_RESYNC_INTERVAL.as_secs()),
            || ContinuousScanSupervisorMessage::ResyncFromDisk,
        );
        let _ = resync_from_disk(&mut state).await;
        Ok(state)
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            ContinuousScanSupervisorMessage::ResyncFromDisk => {
                let _ = resync_from_disk(state).await;
            }
            ContinuousScanSupervisorMessage::ConfigChanged { index_db } => {
                let _ = sync_single_db(state, &index_db).await;
            }
            ContinuousScanSupervisorMessage::PauseForJob { index_db, reply } => {
                if let Some(actor) = state.actors.get(&index_db) {
                    let (tx, rx) = oneshot::channel();
                    let _ = actor.cast(ContinuousScanMessage::Pause { reply: tx });
                    let _ = rx.await;
                }
                let _ = reply.send(());
            }
            ContinuousScanSupervisorMessage::ResumeAfterJob { index_db } => {
                if let Some(actor) = state.actors.get(&index_db) {
                    let _ = actor.cast(ContinuousScanMessage::Resume);
                } else {
                    let _ = sync_single_db(state, &index_db).await;
                }
            }
        }
        Ok(())
    }
}

async fn resync_from_disk(state: &mut ContinuousScanSupervisorState) -> ApiResult<()> {
    let index_dir = state.data_dir.join("index");
    let mut desired = HashMap::new();
    if index_dir.is_dir() {
        for entry in std::fs::read_dir(&index_dir).map_err(|err| {
            tracing::error!(error = %err, path = %index_dir.display(), "failed to read index dir");
            ApiError::internal("Failed to read index directory")
        })? {
            let entry = entry.map_err(|err| {
                tracing::error!(error = %err, "failed to read index dir entry");
                ApiError::internal("Failed to read index directory")
            })?;
            if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                continue;
            }
            let index_db = entry.file_name().to_string_lossy().to_string();
            let index_db_file = entry.path().join("index.db");
            if !index_db_file.is_file() {
                continue;
            }
            let config = state.config_store.load(&index_db)?;
            if config.continuous_filescan {
                desired.insert(index_db, config);
            }
        }
    }

    let mut to_stop = Vec::new();
    for existing in state.actors.keys() {
        if !desired.contains_key(existing) {
            to_stop.push(existing.clone());
        }
    }
    for name in to_stop {
        if let Some(actor) = state.actors.remove(&name) {
            actor.stop(None);
        }
    }

    for (index_db, config) in desired {
        if let Some(actor) = state.actors.get(&index_db) {
            let _ = actor.cast(ContinuousScanMessage::UpdateConfig { config });
            continue;
        }
        let args = ContinuousScanActorArgs {
            index_db: index_db.clone(),
            user_data_db: index_db.clone(),
            data_dir: state.data_dir.clone(),
            enable_watcher: true,
        };
        let (actor, _handle) = Actor::spawn(
            Some(format!("continuous-scan-{index_db}")),
            ContinuousScanActor,
            args,
        )
        .await
        .map_err(|err| ApiError::internal(format!("Failed to spawn continuous scan: {err:?}")))?;
        let _ = actor.cast(ContinuousScanMessage::UpdateConfig { config });
        state.actors.insert(index_db, actor);
    }

    Ok(())
}

async fn sync_single_db(
    state: &mut ContinuousScanSupervisorState,
    index_db: &str,
) -> ApiResult<()> {
    let config = state.config_store.load(index_db)?;
    let index_db_file = state
        .data_dir
        .join("index")
        .join(index_db)
        .join("index.db");
    if !index_db_file.is_file() {
        if let Some(actor) = state.actors.remove(index_db) {
            actor.stop(None);
        }
        return Ok(());
    }
    if !config.continuous_filescan {
        if let Some(actor) = state.actors.remove(index_db) {
            actor.stop(None);
        }
        return Ok(());
    }

    if let Some(actor) = state.actors.get(index_db) {
        let _ = actor.cast(ContinuousScanMessage::UpdateConfig { config });
        return Ok(());
    }

    let args = ContinuousScanActorArgs {
        index_db: index_db.to_string(),
        user_data_db: index_db.to_string(),
        data_dir: state.data_dir.clone(),
        enable_watcher: true,
    };
    let (actor, _handle) = Actor::spawn(
        Some(format!("continuous-scan-{index_db}")),
        ContinuousScanActor,
        args,
    )
    .await
    .map_err(|err| ApiError::internal(format!("Failed to spawn continuous scan: {err:?}")))?;
    let _ = actor.cast(ContinuousScanMessage::UpdateConfig { config });
    state.actors.insert(index_db.to_string(), actor);
    Ok(())
}

fn start_supervisor_watcher(
    actor: ActorRef<ContinuousScanSupervisorMessage>,
    data_dir: &Path,
) -> Result<RecommendedWatcher, notify::Error> {
    let watch_root = data_dir.join("index");
    let _ = std::fs::create_dir_all(&watch_root);
    let mut watcher = RecommendedWatcher::new(
        move |res| match res {
            Ok(_event) => {
                let _ = actor.cast(ContinuousScanSupervisorMessage::ResyncFromDisk);
            }
            Err(err) => {
                tracing::error!(error = ?err, "continuous scan supervisor watcher error");
            }
        },
        notify::Config::default(),
    )?;
    watcher.watch(&watch_root, RecursiveMode::Recursive)?;
    Ok(watcher)
}

static SUPERVISOR: OnceCell<ActorRef<ContinuousScanSupervisorMessage>> = OnceCell::const_new();

pub(crate) async fn ensure_continuous_supervisor() -> ApiResult<ActorRef<ContinuousScanSupervisorMessage>> {
    SUPERVISOR
        .get_or_try_init(|| async {
            let data_dir = std::env::var("DATA_FOLDER").unwrap_or_else(|_| "data".to_string());
            let (actor, _handle) = Actor::spawn(
                Some("continuous-scan-supervisor".to_string()),
                ContinuousScanSupervisor,
                ContinuousScanSupervisorArgs {
                    data_dir: PathBuf::from(data_dir),
                },
            )
            .await
            .map_err(|err| {
                tracing::error!(error = ?err, "failed to start continuous scan supervisor");
                ApiError::internal("Failed to start continuous scan supervisor")
            })?;
            Ok(actor)
        })
        .await
        .map(Clone::clone)
}

pub(crate) async fn notify_config_change(index_db: &str) -> ApiResult<()> {
    let supervisor = ensure_continuous_supervisor().await?;
    supervisor
        .cast(ContinuousScanSupervisorMessage::ConfigChanged {
            index_db: index_db.to_string(),
        })
        .map_err(|_| ApiError::internal("Failed to notify continuous scan supervisor"))?;
    Ok(())
}

pub(crate) async fn pause_for_job(index_db: &str) -> ApiResult<()> {
    let supervisor = ensure_continuous_supervisor().await?;
    let (reply, rx) = oneshot::channel();
    supervisor
        .cast(ContinuousScanSupervisorMessage::PauseForJob {
            index_db: index_db.to_string(),
            reply,
        })
        .map_err(|_| ApiError::internal("Failed to pause continuous scan"))?;
    let _ = rx.await;
    Ok(())
}

pub(crate) async fn resume_after_job(index_db: &str) -> ApiResult<()> {
    let supervisor = ensure_continuous_supervisor().await?;
    supervisor
        .cast(ContinuousScanSupervisorMessage::ResumeAfterJob {
            index_db: index_db.to_string(),
        })
        .map_err(|_| ApiError::internal("Failed to resume continuous scan"))?;
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrations::migrate_databases_on_disk;
    use image::{ImageBuffer, Rgb};
    use ractor::Actor;
    use std::sync::atomic::{AtomicU64, Ordering};
    use crate::test_utils::test_data_dir;

    fn unique_db_name(prefix: &str) -> String {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        format!("{prefix}-{}", COUNTER.fetch_add(1, Ordering::Relaxed))
    }

    fn write_test_image(path: &std::path::Path) {
        let img: ImageBuffer<Rgb<u8>, Vec<u8>> = ImageBuffer::from_pixel(2, 2, Rgb([255, 0, 0]));
        img.save(path).unwrap();
    }

    #[tokio::test]
    async fn continuous_scan_writes_and_closes_scan() {
        let test_env = test_data_dir();
        let root = test_env.path().to_path_buf();
        let index_db = unique_db_name("cont");
        let _ = migrate_databases_on_disk(Some(&index_db), Some(&index_db)).await.unwrap();

        let watch_dir = root.join("watch");
        std::fs::create_dir_all(&watch_dir).unwrap();
        let file_path = watch_dir.join("test.png");
        write_test_image(&file_path);

        let store = SystemConfigStore::new(root.clone());
        let mut config = store.load(&index_db).unwrap();
        config.continuous_filescan = true;
        config.included_folders = vec![watch_dir.to_string_lossy().to_string()];
        store.save(&index_db, &config).unwrap();

        let (actor, _handle) = Actor::spawn(
            None,
            ContinuousScanActor,
            ContinuousScanActorArgs {
                index_db: index_db.clone(),
                user_data_db: index_db.clone(),
                data_dir: root.clone(),
                enable_watcher: false,
            },
        )
        .await
        .unwrap();

        let prepared = process_file(file_path.clone(), &config).unwrap();
        actor
            .cast(ContinuousScanMessage::WorkerResult {
                epoch: 0,
                scan_time: current_iso_timestamp(),
                result: Ok(prepared),
            })
            .unwrap();

        let mut attempts = 0;
        loop {
            let mut conn = crate::db::open_index_db_read_no_user_data(&index_db).await.unwrap();
            let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM files")
                .fetch_one(&mut conn)
                .await
                .unwrap();
            if count.0 > 0 || attempts > 20 {
                break;
            }
            attempts += 1;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let (tx, rx) = oneshot::channel();
        actor.cast(ContinuousScanMessage::Pause { reply: tx }).unwrap();
        let _ = rx.await;

        let mut conn = crate::db::open_index_db_read_no_user_data(&index_db).await.unwrap();
        let row: Option<(Option<String>,)> =
            sqlx::query_as("SELECT end_time FROM file_scans WHERE path = ?1")
                .bind(CONTINUOUS_PATH_SENTINEL)
                .fetch_optional(&mut conn)
                .await
                .unwrap();
        assert!(row.is_some());
        assert!(row.unwrap().0.is_some());
    }

    #[tokio::test]
    async fn epoch_gating_drops_results() {
        let test_env = test_data_dir();
        let root = test_env.path().to_path_buf();
        let index_db = unique_db_name("cont");
        let _ = migrate_databases_on_disk(Some(&index_db), Some(&index_db)).await.unwrap();

        let watch_dir = root.join("watch2");
        std::fs::create_dir_all(&watch_dir).unwrap();
        let file_path = watch_dir.join("test2.png");
        write_test_image(&file_path);

        let store = SystemConfigStore::new(root.clone());
        let mut config = store.load(&index_db).unwrap();
        config.continuous_filescan = true;
        config.included_folders = vec![watch_dir.to_string_lossy().to_string()];
        store.save(&index_db, &config).unwrap();

        let (actor, _handle) = Actor::spawn(
            None,
            ContinuousScanActor,
            ContinuousScanActorArgs {
                index_db: index_db.clone(),
                user_data_db: index_db.clone(),
                data_dir: root.clone(),
                enable_watcher: false,
            },
        )
        .await
        .unwrap();

        let (tx, rx) = oneshot::channel();
        actor.cast(ContinuousScanMessage::Pause { reply: tx }).unwrap();
        let _ = rx.await;

        let prepared = process_file(file_path.clone(), &config).unwrap();
        actor
            .cast(ContinuousScanMessage::WorkerResult {
                epoch: 0,
                scan_time: current_iso_timestamp(),
                result: Ok(prepared),
            })
            .unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut conn = crate::db::open_index_db_read_no_user_data(&index_db).await.unwrap();
        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM files")
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert_eq!(count.0, 0);
    }
}
