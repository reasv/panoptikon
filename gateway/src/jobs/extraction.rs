use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use base64::{Engine as _, engine::general_purpose};
use futures_util::TryStreamExt;
use sea_query::{SqliteQueryBuilder, Value as SeaValue, Values};
use serde_json::Value;
use sqlx::{
    Row,
    sqlite::{SqliteArguments, SqliteRow},
};
use tokio::sync::{Mutex, Semaphore};

use crate::api_error::ApiError;
use crate::db::extraction_write::{DataLogUpdate, get_setter_data_types};
use crate::db::index_writer::{IndexDbWriterMessage, call_index_db_writer};
use crate::db::items::get_existing_file_for_item_id;
use crate::db::open_index_db_read;
use crate::db::pql::run_compiled_count;
use crate::db::system_config::{SystemConfig, SystemConfigStore};
use crate::inferio_client::InferenceInput;
use crate::jobs::continuous_scan;
use crate::jobs::files::{FileScanService, is_resync_needed, run_post_job_maintenance};
use crate::jobs::inference_pool::{InferencePool, job_inference_context};
use crate::pql::builder::filters::OneOrMany;
use crate::pql::model::{
    AndOperator, Column, EntityType, Match, MatchOps, MatchValues, Matches, NotOperator, PqlQuery,
    ProcessedBy, QueryElement,
};
use crate::pql::{build_query_preprocessed, preprocess_query_async};

type ApiResult<T> = std::result::Result<T, ApiError>;

mod input_handlers;
mod output_handlers;

const CACHE_KEY: &str = "batch";
const CACHE_LRU_SIZE: i64 = 1;
const CACHE_TTL_SECS: i64 = 60;

#[derive(Debug, Clone)]
pub(crate) struct ModelMetadata {
    pub group: String,
    pub inference_id: String,
    pub setter_name: String,
    pub input_handler: String,
    pub input_handler_opts: serde_json::Map<String, Value>,
    pub target_entities: Vec<String>,
    pub output_type: String,
    pub default_batch_size: i64,
    pub default_threshold: Option<f64>,
    pub input_mime_types: Vec<String>,
    pub skip_processed_items: bool,
    pub name: Option<String>,
    pub description: Option<String>,
    pub link: Option<String>,
}

#[derive(Debug, Clone)]
struct JobInputData {
    file_id: i64,
    item_id: i64,
    path: String,
    sha256: String,
    md5: String,
    last_modified: String,
    item_type: String,
    duration: Option<f64>,
    audio_tracks: Option<i64>,
    video_tracks: Option<i64>,
    subtitle_tracks: Option<i64>,
    width: Option<i64>,
    height: Option<i64>,
    data_id: Option<i64>,
    text: Option<String>,
}

#[derive(Debug, Clone)]
struct JobDefaults {
    batch_size: i64,
    threshold: Option<f64>,
}

#[derive(Debug, Clone)]
struct PreparedItem {
    item: JobInputData,
    inputs: Vec<InferenceInput>,
}

#[derive(Default)]
struct JobCounters {
    processed: i64,
    image_files: i64,
    video_files: i64,
    other_files: i64,
    total_segments: i64,
    errors: i64,
    data_load_time: f64,
    inference_time: f64,
}

pub(crate) async fn run_extraction_job(job: crate::jobs::queue::Job) -> Result<(), String> {
    let inference_id = job
        .metadata
        .clone()
        .ok_or_else(|| "Inference ID required".to_string())?;

    continuous_scan::pause_for_job(&job.index_db)
        .await
        .map_err(|err| format!("{err:?}"))?;

    let result = run_extraction_job_inner(&job, &inference_id).await;
    let _ = continuous_scan::resume_after_job(&job.index_db).await;
    if result.is_ok() {
        run_post_job_maintenance(&job.index_db, false).await;
    }
    result.map_err(|err| format!("{err:?}"))
}
async fn run_extraction_job_inner(
    job: &crate::jobs::queue::Job,
    inference_id: &str,
) -> ApiResult<()> {
    let config_store = SystemConfigStore::from_env();
    let config = config_store.load(&job.index_db)?;

    if is_resync_needed(&job.index_db, &job.user_data_db, &config).await? {
        let service = FileScanService::from_env(job.index_db.clone(), job.user_data_db.clone());
        service.run_folder_update().await?;
    }

    let model = load_model_metadata(inference_id).await?;
    let defaults = resolve_job_defaults(&config, &model, job.batch_size, job.threshold);

    let context = job_inference_context();
    if context.pool.is_empty().await {
        return Err(ApiError::internal(
            "No inference endpoints enabled for batch jobs",
        ));
    }

    let mut query = build_job_pql(&config, &model)?;
    if let Some(root) = query.query.take() {
        let preprocessed =
            preprocess_query_async(root, &context.primary, context.embedding_cache_size)
                .await
                .map_err(|err| ApiError::bad_request(err.message))?;
        query.query = preprocessed;
    }

    let compiled = compile_pql_select(query.clone())?;
    let compiled_count = compile_pql_count(query.clone())?;

    let mut count_conn = open_index_db_read(&job.index_db, &job.user_data_db).await?;
    let total_remaining =
        run_compiled_count(&mut count_conn, &compiled_count.sql, &compiled_count.params).await?;
    drop(count_conn);

    if total_remaining < 1 {
        tracing::info!(inference_id, "no items to process");
        return Ok(());
    }

    call_index_db_writer(&job.index_db, |reply| {
        IndexDbWriterMessage::RemoveIncompleteJobs { reply }
    })
    .await?;

    let scan_time = current_iso_timestamp();
    let job_id = call_index_db_writer(&job.index_db, |reply| IndexDbWriterMessage::AddDataLog {
        scan_time: scan_time.clone(),
        threshold: defaults.threshold,
        types: vec![model.output_type.clone()],
        setter: model.setter_name.clone(),
        batch_size: defaults.batch_size,
        reply,
    })
    .await?;
    let _ = call_index_db_writer(&job.index_db, |reply| IndexDbWriterMessage::UpsertSetter {
        setter_name: model.setter_name.clone(),
        reply,
    })
    .await?;

    let load_result = context
        .pool
        .load_model_all(
            &model.setter_name,
            CACHE_KEY,
            CACHE_LRU_SIZE,
            CACHE_TTL_SECS,
        )
        .await;
    if let Err(err) = load_result {
        return Err(ApiError::internal(format!("Failed to load model: {err}")));
    }

    let counters = Arc::new(Mutex::new(JobCounters::default()));
    let semaphore = Arc::new(Semaphore::new(defaults.batch_size as usize));
    let mut handles = Vec::new();

    let mut conn = open_index_db_read(&job.index_db, &job.user_data_db).await?;
    let mut query = sqlx::query(&compiled.sql);
    query = bind_params(query, &compiled.params)?;
    let mut rows = query.fetch(&mut conn);
    while let Some(row) = rows.try_next().await.map_err(|err| {
        tracing::error!(error = %err, "failed to fetch extraction rows");
        ApiError::internal("Failed to execute extraction query")
    })? {
        let Some(item) = map_job_input(&job.index_db, &job.user_data_db, &row).await? else {
            continue;
        };
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| ApiError::internal("Extraction job semaphore closed"))?;
        let model = model.clone();
        let pool = context.pool.clone();
        let counters = Arc::clone(&counters);
        let index_db = job.index_db.clone();
        let threshold = defaults.threshold;
        let handle = tokio::spawn(async move {
            let _permit = permit;
            let result = process_item(
                &index_db,
                &model,
                job_id,
                item,
                threshold,
                &pool,
                counters,
                total_remaining,
            )
            .await;
            if let Err(err) = result {
                tracing::error!(error = ?err, "extraction item failed");
            }
        });
        handles.push(handle);
    }
    drop(rows);
    drop(conn);

    for handle in handles {
        let _ = handle.await;
    }

    let remaining_after = {
        let mut count_conn = open_index_db_read(&job.index_db, &job.user_data_db).await?;
        let remaining =
            run_compiled_count(&mut count_conn, &compiled_count.sql, &compiled_count.params)
                .await?;
        remaining
    };

    let final_update = {
        let guard = counters.lock().await;
        DataLogUpdate {
            image_files: guard.image_files,
            video_files: guard.video_files,
            other_files: guard.other_files,
            total_segments: guard.total_segments,
            errors: guard.errors,
            total_remaining: remaining_after,
            data_load_time: guard.data_load_time,
            inference_time: guard.inference_time,
            finished: true,
        }
    };
    let _ = call_index_db_writer(&job.index_db, |reply| IndexDbWriterMessage::UpdateDataLog {
        job_id,
        update: final_update.clone(),
        reply,
    })
    .await;

    let _ = context
        .pool
        .unload_model_all(&model.setter_name, CACHE_KEY)
        .await;

    Ok(())
}

pub(crate) async fn run_data_deletion_job(job: crate::jobs::queue::Job) -> Result<(), String> {
    let inference_id = job
        .metadata
        .clone()
        .ok_or_else(|| "Inference ID required".to_string())?;
    let mut conn = open_index_db_read(&job.index_db, &job.user_data_db)
        .await
        .map_err(|err| format!("{err:?}"))?;
    let data_types = get_setter_data_types(&mut conn, &inference_id)
        .await
        .map_err(|err| format!("{err:?}"))?;
    drop(conn);

    call_index_db_writer(&job.index_db, |reply| {
        IndexDbWriterMessage::DeleteSetterByName {
            setter_name: inference_id.clone(),
            reply,
        }
    })
    .await
    .map_err(|err| format!("{err:?}"))?;

    if data_types.iter().any(|entry| entry == "tags") {
        let _ = call_index_db_writer(&job.index_db, |reply| {
            IndexDbWriterMessage::DeleteOrphanTags { reply }
        })
        .await;
    }
    run_post_job_maintenance(&job.index_db, true).await;
    Ok(())
}

async fn process_item(
    index_db: &str,
    model: &ModelMetadata,
    job_id: i64,
    item: JobInputData,
    threshold: Option<f64>,
    pool: &InferencePool,
    counters: Arc<Mutex<JobCounters>>,
    total_remaining: i64,
) -> ApiResult<()> {
    let load_start = Instant::now();
    let item_type = item.item_type.clone();
    let prepared = match input_handlers::prepare_item(index_db, model, item).await {
        Ok(prepared) => prepared,
        Err(err) => {
            let load_time = load_start.elapsed().as_secs_f64();
            finalize_item(
                index_db,
                job_id,
                &item_type,
                load_time,
                0.0,
                0,
                false,
                true,
                counters,
                total_remaining,
            )
            .await;
            return Err(err);
        }
    };
    let load_time = load_start.elapsed().as_secs_f64();

    if prepared.inputs.is_empty() {
        let empty_outputs = output_handlers::empty_outputs_for(&model.output_type)?;
        let result = output_handlers::handle_outputs(
            index_db,
            model,
            job_id,
            prepared.item.clone(),
            empty_outputs,
        )
        .await;
        finalize_item(
            index_db,
            job_id,
            &prepared.item.item_type,
            load_time,
            0.0,
            0,
            result.is_ok(),
            result.is_err(),
            counters,
            total_remaining,
        )
        .await;
        return result.map(|_| ());
    }

    let inference_inputs = input_handlers::apply_threshold(prepared.inputs, threshold);
    let segments = inference_inputs.len() as i64;
    let inf_start = Instant::now();
    let outputs = match pool
        .predict(
            &model.setter_name,
            CACHE_KEY,
            CACHE_LRU_SIZE,
            CACHE_TTL_SECS,
            &inference_inputs,
        )
        .await
    {
        Ok(outputs) => outputs,
        Err(err) => {
            let inference_time = inf_start.elapsed().as_secs_f64();
            let api_err = ApiError::internal(format!("Inference failed: {err}"));
            finalize_item(
                index_db,
                job_id,
                &prepared.item.item_type,
                load_time,
                inference_time,
                segments,
                false,
                true,
                counters,
                total_remaining,
            )
            .await;
            return Err(api_err);
        }
    };
    let inference_time = inf_start.elapsed().as_secs_f64();

    let result =
        output_handlers::handle_outputs(index_db, model, job_id, prepared.item.clone(), outputs)
            .await;
    finalize_item(
        index_db,
        job_id,
        &prepared.item.item_type,
        load_time,
        inference_time,
        segments,
        result.is_ok(),
        result.is_err(),
        counters,
        total_remaining,
    )
    .await;
    result.map(|_| ())
}

async fn finalize_item(
    index_db: &str,
    job_id: i64,
    item_type: &str,
    load_time: f64,
    inference_time: f64,
    segments: i64,
    count_file: bool,
    is_error: bool,
    counters: Arc<Mutex<JobCounters>>,
    total_remaining: i64,
) {
    let update = {
        let mut guard = counters.lock().await;
        guard.processed += 1;
        guard.data_load_time += load_time;
        guard.inference_time += inference_time;
        guard.total_segments += segments;

        if count_file {
            if item_type.starts_with("video") {
                guard.video_files += 1;
            } else if item_type.starts_with("image") {
                guard.image_files += 1;
            } else {
                guard.other_files += 1;
            }
        } else if is_error {
            guard.errors += 1;
        }

        let remaining = total_remaining.saturating_sub(guard.processed);
        DataLogUpdate {
            image_files: guard.image_files,
            video_files: guard.video_files,
            other_files: guard.other_files,
            total_segments: guard.total_segments,
            errors: guard.errors,
            total_remaining: remaining,
            data_load_time: guard.data_load_time,
            inference_time: guard.inference_time,
            finished: false,
        }
    };
    let _ = call_index_db_writer(index_db, |reply| IndexDbWriterMessage::UpdateDataLog {
        job_id,
        update: update.clone(),
        reply,
    })
    .await;
}

async fn map_job_input(
    index_db: &str,
    user_data_db: &str,
    row: &SqliteRow,
) -> ApiResult<Option<JobInputData>> {
    let file_id: i64 = row.try_get("file_id").map_err(map_row_err)?;
    let item_id: i64 = row.try_get("item_id").map_err(map_row_err)?;
    let sha256: String = row.try_get("sha256").map_err(map_row_err)?;
    let md5: String = row.try_get("md5").map_err(map_row_err)?;
    let path: String = row.try_get("path").map_err(map_row_err)?;
    let last_modified: String = row.try_get("last_modified").map_err(map_row_err)?;
    let item_type: String = row.try_get("type").map_err(map_row_err)?;
    let duration: Option<f64> = row.try_get("duration").unwrap_or(None);
    let audio_tracks: Option<i64> = row.try_get("audio_tracks").unwrap_or(None);
    let video_tracks: Option<i64> = row.try_get("video_tracks").unwrap_or(None);
    let subtitle_tracks: Option<i64> = row.try_get("subtitle_tracks").unwrap_or(None);
    let width: Option<i64> = row.try_get("width").unwrap_or(None);
    let height: Option<i64> = row.try_get("height").unwrap_or(None);
    let data_id: Option<i64> = row.try_get("data_id").unwrap_or(None);
    let text: Option<String> = row.try_get("text").unwrap_or(None);

    let mut input = JobInputData {
        file_id,
        item_id,
        path,
        sha256,
        md5,
        last_modified,
        item_type,
        duration,
        audio_tracks,
        video_tracks,
        subtitle_tracks,
        width,
        height,
        data_id,
        text,
    };

    if !Path::new(&input.path).exists() {
        let mut conn = open_index_db_read(index_db, user_data_db).await?;
        if let Some(file) = get_existing_file_for_item_id(&mut conn, input.item_id).await? {
            input.path = file.path;
            input.file_id = file.id;
            input.last_modified = file.last_modified;
        } else {
            return Ok(None);
        }
    }

    Ok(Some(input))
}

fn map_row_err(err: sqlx::Error) -> ApiError {
    tracing::error!(error = %err, "failed to read query row");
    ApiError::internal("Failed to read job input")
}
fn build_job_pql(config: &SystemConfig, model: &ModelMetadata) -> ApiResult<PqlQuery> {
    let mut filters = Vec::new();
    if !model.input_mime_types.is_empty() {
        filters.push(QueryElement::Match(Match {
            match_: Matches::Ops(MatchOps {
                startswith: Some(MatchValues {
                    r#type: Some(OneOrMany::Many(model.input_mime_types.clone())),
                    ..Default::default()
                }),
                ..Default::default()
            }),
        }));
    }

    if model.skip_processed_items {
        filters.push(QueryElement::Not(NotOperator {
            not_: Box::new(QueryElement::ProcessedBy(ProcessedBy {
                processed_by: model.setter_name.clone(),
            })),
        }));
    }

    let mut user_filters = Vec::new();
    for filter in &config.job_filters {
        if filter
            .setter_names
            .iter()
            .any(|name| name == "*" || name == &model.setter_name)
        {
            match &filter.pql_query {
                QueryElement::And(and) => user_filters.extend(and.and_.clone()),
                other => user_filters.push(other.clone()),
            }
        }
    }
    filters.extend(user_filters);

    let query = if filters.is_empty() {
        None
    } else if filters.len() == 1 {
        Some(filters.remove(0))
    } else {
        Some(QueryElement::And(AndOperator { and_: filters }))
    };

    let mut pql = PqlQuery::default();
    pql.query = query;
    pql.page_size = 0;
    pql.check_path = false;

    match model.target_entities.as_slice() {
        [value] if value == "items" => {
            pql.entity = EntityType::File;
            pql.partition_by = Some(vec![Column::ItemId]);
            pql.select = vec![
                Column::Sha256,
                Column::Path,
                Column::LastModified,
                Column::Type,
                Column::Md5,
                Column::Width,
                Column::Height,
                Column::Duration,
                Column::AudioTracks,
                Column::VideoTracks,
                Column::SubtitleTracks,
            ];
        }
        [value] if value == "files" => {
            pql.entity = EntityType::File;
            pql.partition_by = None;
            pql.select = vec![
                Column::Sha256,
                Column::Path,
                Column::LastModified,
                Column::Type,
                Column::Md5,
                Column::Width,
                Column::Height,
                Column::Duration,
                Column::AudioTracks,
                Column::VideoTracks,
                Column::SubtitleTracks,
            ];
        }
        [value] if value == "text" => {
            pql.entity = EntityType::Text;
            pql.partition_by = Some(vec![Column::DataId]);
            pql.select = vec![
                Column::Sha256,
                Column::Path,
                Column::LastModified,
                Column::Type,
                Column::Md5,
                Column::Width,
                Column::Height,
                Column::DataId,
                Column::Text,
            ];
        }
        _ => {
            return Err(ApiError::bad_request(
                "Only items, files, and text target entities are supported",
            ));
        }
    }

    Ok(pql)
}

fn resolve_job_defaults(
    config: &SystemConfig,
    model: &ModelMetadata,
    batch_size: Option<i64>,
    threshold: Option<f64>,
) -> JobDefaults {
    let mut chosen_batch = model.default_batch_size.max(1);
    let mut chosen_threshold = model.default_threshold;

    for setting in &config.job_settings {
        if setting.group_name == model.group && setting.inference_id.is_none() {
            if let Some(default_batch) = setting.default_batch_size {
                chosen_batch = default_batch;
            }
            if model.default_threshold.is_some() {
                if let Some(default_threshold) = setting.default_threshold {
                    chosen_threshold = Some(default_threshold);
                }
            }
        }
    }
    for setting in &config.job_settings {
        if setting.group_name == model.group
            && setting.inference_id.as_deref() == Some(&model.setter_name)
        {
            if let Some(default_batch) = setting.default_batch_size {
                chosen_batch = default_batch;
            }
            if model.default_threshold.is_some() {
                if let Some(default_threshold) = setting.default_threshold {
                    chosen_threshold = Some(default_threshold);
                }
            }
        }
    }

    if let Some(batch) = batch_size {
        if batch > 0 {
            chosen_batch = batch;
        }
    }
    if threshold.is_some() {
        chosen_threshold = threshold;
    }

    JobDefaults {
        batch_size: chosen_batch.max(1),
        threshold: chosen_threshold,
    }
}

async fn load_model_metadata(inference_id: &str) -> ApiResult<ModelMetadata> {
    let context = job_inference_context();
    let metadata = context.primary.get_metadata().await.map_err(|err| {
        tracing::error!(error = %err, "failed to load inference metadata");
        ApiError::internal("Failed to load inference metadata")
    })?;

    let (group, short_id) = inference_id
        .split_once('/')
        .ok_or_else(|| ApiError::bad_request("Inference ID must be in group/id format"))?;

    let group_meta = metadata
        .get(group)
        .and_then(Value::as_object)
        .ok_or_else(|| ApiError::bad_request("Inference group not found"))?;

    let group_metadata = group_meta
        .get("group_metadata")
        .cloned()
        .unwrap_or_else(|| Value::Object(serde_json::Map::new()));
    let inference_metadata = group_meta
        .get("inference_ids")
        .and_then(Value::as_object)
        .and_then(|map| map.get(short_id).cloned())
        .ok_or_else(|| ApiError::bad_request("Inference ID not found"))?;

    let merged = merge_metadata(group_metadata, inference_metadata);
    let input_spec = merged
        .get("input_spec")
        .and_then(Value::as_object)
        .ok_or_else(|| ApiError::bad_request("input_spec missing from metadata"))?;
    let handler = input_spec
        .get("handler")
        .and_then(Value::as_str)
        .ok_or_else(|| ApiError::bad_request("input_spec.handler missing"))?;
    let opts = input_spec
        .get("opts")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();

    let target_entities = merged
        .get("target_entities")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|value| value.as_str().map(|s| s.to_string()))
                .collect::<Vec<_>>()
        })
        .unwrap_or_else(|| vec!["items".to_string()]);

    let output_type = merged
        .get("output_type")
        .and_then(Value::as_str)
        .unwrap_or("text")
        .to_string();

    let default_batch_size = merged
        .get("default_batch_size")
        .and_then(Value::as_i64)
        .unwrap_or(64);

    let default_threshold = merged.get("default_threshold").and_then(Value::as_f64);

    let input_mime_types = merged
        .get("input_mime_types")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|value| value.as_str().map(|s| s.to_string()))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let skip_processed_items = merged
        .get("skip_processed_items")
        .and_then(Value::as_bool)
        .unwrap_or(true);

    let name = merged
        .get("name")
        .and_then(Value::as_str)
        .map(|s| s.to_string());
    let description = merged
        .get("description")
        .and_then(Value::as_str)
        .map(|s| s.to_string());
    let link = merged
        .get("link")
        .and_then(Value::as_str)
        .map(|s| s.to_string());

    Ok(ModelMetadata {
        group: group.to_string(),
        inference_id: short_id.to_string(),
        setter_name: inference_id.to_string(),
        input_handler: handler.to_string(),
        input_handler_opts: opts,
        target_entities,
        output_type,
        default_batch_size,
        default_threshold,
        input_mime_types,
        skip_processed_items,
        name,
        description,
        link,
    })
}

fn merge_metadata(
    mut group_metadata: Value,
    inference_metadata: Value,
) -> serde_json::Map<String, Value> {
    let mut merged = match group_metadata {
        Value::Object(map) => map,
        _ => serde_json::Map::new(),
    };
    if let Value::Object(inf_map) = inference_metadata {
        for (key, value) in inf_map {
            if key == "input_spec" {
                let mut base = merged
                    .get("input_spec")
                    .cloned()
                    .unwrap_or_else(|| Value::Object(serde_json::Map::new()));
                deep_merge(&mut base, &value);
                merged.insert("input_spec".to_string(), base);
            } else {
                merged.insert(key, value);
            }
        }
    }
    merged
}

fn deep_merge(base: &mut Value, overlay: &Value) {
    match (base, overlay) {
        (Value::Object(base_map), Value::Object(overlay_map)) => {
            for (key, value) in overlay_map {
                match base_map.get_mut(key) {
                    Some(base_value) => deep_merge(base_value, value),
                    None => {
                        base_map.insert(key.clone(), value.clone());
                    }
                }
            }
        }
        (base_val, overlay_val) => {
            *base_val = overlay_val.clone();
        }
    }
}

#[derive(Clone)]
struct CompiledQuery {
    sql: String,
    params: Vec<Value>,
}

fn compile_pql_select(query: PqlQuery) -> ApiResult<CompiledQuery> {
    let built =
        build_query_preprocessed(query, false).map_err(|err| ApiError::bad_request(err.message))?;
    compile_select(built)
}

fn compile_pql_count(query: PqlQuery) -> ApiResult<CompiledQuery> {
    let built =
        build_query_preprocessed(query, true).map_err(|err| ApiError::bad_request(err.message))?;
    compile_select(built)
}

fn compile_select(built: crate::pql::PqlBuilderResult) -> ApiResult<CompiledQuery> {
    let (sql, values) = match built.with_clause {
        Some(with_clause) => built.query.with(with_clause).build(SqliteQueryBuilder),
        None => built.query.build(SqliteQueryBuilder),
    };
    let params = encode_values(values)?;
    Ok(CompiledQuery { sql, params })
}

fn encode_values(values: Values) -> ApiResult<Vec<Value>> {
    let mut encoded = Vec::with_capacity(values.iter().count());
    for value in values.into_iter() {
        encoded.push(encode_value(value)?);
    }
    Ok(encoded)
}

fn encode_value(value: SeaValue) -> ApiResult<Value> {
    match value {
        SeaValue::Bool(value) => Ok(value.map(Value::Bool).unwrap_or(Value::Null)),
        SeaValue::TinyInt(value) => Ok(value.map(|v| Value::from(v as i64)).unwrap_or(Value::Null)),
        SeaValue::SmallInt(value) => {
            Ok(value.map(|v| Value::from(v as i64)).unwrap_or(Value::Null))
        }
        SeaValue::Int(value) => Ok(value.map(Value::from).unwrap_or(Value::Null)),
        SeaValue::BigInt(value) => Ok(value.map(Value::from).unwrap_or(Value::Null)),
        SeaValue::TinyUnsigned(value) => {
            Ok(value.map(|v| Value::from(v as u64)).unwrap_or(Value::Null))
        }
        SeaValue::SmallUnsigned(value) => {
            Ok(value.map(|v| Value::from(v as u64)).unwrap_or(Value::Null))
        }
        SeaValue::Unsigned(value) => {
            Ok(value.map(|v| Value::from(v as u64)).unwrap_or(Value::Null))
        }
        SeaValue::BigUnsigned(value) => Ok(value.map(Value::from).unwrap_or(Value::Null)),
        SeaValue::Float(value) => Ok(match value {
            Some(v) => serde_json::Number::from_f64(v as f64)
                .map(Value::Number)
                .unwrap_or(Value::Null),
            None => Value::Null,
        }),
        SeaValue::Double(value) => Ok(match value {
            Some(v) => serde_json::Number::from_f64(v)
                .map(Value::Number)
                .unwrap_or(Value::Null),
            None => Value::Null,
        }),
        SeaValue::String(value) => Ok(value.map(Value::String).unwrap_or(Value::Null)),
        SeaValue::Char(value) => Ok(value
            .map(|v| Value::String(v.to_string()))
            .unwrap_or(Value::Null)),
        SeaValue::Bytes(value) => match value {
            Some(bytes) => {
                let mut map = serde_json::Map::new();
                map.insert(
                    "__bytes__".to_string(),
                    Value::String(general_purpose::STANDARD.encode(bytes)),
                );
                Ok(Value::Object(map))
            }
            None => Ok(Value::Null),
        },
        SeaValue::Json(value) => Ok(value.map(|v| *v).unwrap_or(Value::Null)),
        _ => Err(ApiError::bad_request("Unsupported PQL parameter type")),
    }
}

fn bind_params<'q>(
    mut query: sqlx::query::Query<'q, sqlx::Sqlite, SqliteArguments<'q>>,
    params: &[Value],
) -> ApiResult<sqlx::query::Query<'q, sqlx::Sqlite, SqliteArguments<'q>>> {
    for param in params {
        query = bind_param(query, param)?;
    }
    Ok(query)
}

fn bind_param<'q>(
    query: sqlx::query::Query<'q, sqlx::Sqlite, SqliteArguments<'q>>,
    param: &Value,
) -> ApiResult<sqlx::query::Query<'q, sqlx::Sqlite, SqliteArguments<'q>>> {
    match param {
        Value::Null => Ok(query.bind(Option::<i64>::None)),
        Value::Bool(value) => Ok(query.bind(*value)),
        Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(query.bind(value))
            } else if let Some(value) = value.as_u64() {
                if value <= i64::MAX as u64 {
                    Ok(query.bind(value as i64))
                } else {
                    Ok(query.bind(value as f64))
                }
            } else if let Some(value) = value.as_f64() {
                Ok(query.bind(value))
            } else {
                Ok(query.bind(value.to_string()))
            }
        }
        Value::String(value) => Ok(query.bind(value.clone())),
        Value::Object(map) => {
            if let Some(Value::String(encoded)) = map.get("__bytes__") {
                let decoded = general_purpose::STANDARD
                    .decode(encoded.as_bytes())
                    .map_err(|err| {
                        tracing::error!(error = %err, "failed to decode pql bytes param");
                        ApiError::bad_request("Invalid PQL parameters")
                    })?;
                return Ok(query.bind(decoded));
            }
            let encoded = serde_json::to_string(param).map_err(|err| {
                tracing::error!(error = %err, "failed to encode pql param");
                ApiError::bad_request("Invalid PQL parameters")
            })?;
            Ok(query.bind(encoded))
        }
        Value::Array(_) => {
            let encoded = serde_json::to_string(param).map_err(|err| {
                tracing::error!(error = %err, "failed to encode pql param");
                ApiError::bad_request("Invalid PQL parameters")
            })?;
            Ok(query.bind(encoded))
        }
    }
}

fn current_iso_timestamp() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string())
}
