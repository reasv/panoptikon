use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use base64::{Engine as _, engine::general_purpose};
use futures_util::TryStreamExt;
use image::AnimationDecoder;
use image::codecs::gif::GifDecoder;
use image::{DynamicImage, GenericImageView};
use sea_query::{SqliteQueryBuilder, Value as SeaValue, Values};
use serde_json::{Value, json};
use sqlx::{
    Row,
    sqlite::{SqliteArguments, SqliteRow},
};
use tokio::sync::{Mutex, Semaphore};

use crate::api_error::ApiError;
use crate::db::extraction_write::{
    DataLogUpdate, EmbeddingEntry, TagEntry, TagTextEntry, TextEntry, get_setter_data_types,
};
use crate::db::index_writer::{IndexDbWriterMessage, call_index_db_writer};
use crate::db::items::get_existing_file_for_item_id;
use crate::db::pql::run_compiled_count;
use crate::db::storage::{StoredImage, get_frames_bytes};
use crate::db::system_config::{SystemConfig, SystemConfigStore};
use crate::db::{open_index_db_read, open_index_db_read_no_user_data};
use crate::inferio_client::{InferenceFile, InferenceInput, PredictOutput};
use crate::jobs::continuous_scan;
use crate::jobs::files::{FileScanService, is_resync_needed};
use crate::jobs::inference_pool::{InferencePool, job_inference_context};
use crate::pql::builder::filters::OneOrMany;
use crate::pql::model::{
    AndOperator, Column, EntityType, Match, MatchOps, MatchValues, Matches, NotOperator, PqlQuery,
    ProcessedBy, QueryElement,
};
use crate::pql::{build_query_preprocessed, preprocess_query_async};

type ApiResult<T> = std::result::Result<T, ApiError>;

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
    let prepared = match prepare_item(index_db, model, item).await {
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
        let empty_outputs = empty_outputs_for(&model.output_type)?;
        let result = handle_outputs(
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

    let inference_inputs = apply_threshold(prepared.inputs, threshold);
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

    let result = handle_outputs(index_db, model, job_id, prepared.item.clone(), outputs).await;
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

#[derive(Debug)]
enum OutputDisposition {
    Written,
    Skipped,
}

fn empty_outputs_for(output_type: &str) -> ApiResult<PredictOutput> {
    match output_type {
        "tags" | "text" => Ok(PredictOutput::Json(Vec::new())),
        "clip" | "text-embedding" => Ok(PredictOutput::Binary(Vec::new())),
        other => Err(ApiError::bad_request(format!(
            "Unsupported output type: {other}"
        ))),
    }
}

async fn handle_outputs(
    index_db: &str,
    model: &ModelMetadata,
    job_id: i64,
    item: JobInputData,
    outputs: PredictOutput,
) -> ApiResult<OutputDisposition> {
    match model.output_type.as_str() {
        "tags" => handle_tags_output(index_db, model, job_id, &item, outputs).await,
        "text" => handle_text_output(index_db, model, job_id, &item, outputs).await,
        "clip" => handle_clip_output(index_db, model, job_id, &item, outputs).await,
        "text-embedding" => {
            handle_text_embedding_output(index_db, model, job_id, &item, outputs).await
        }
        other => Err(ApiError::bad_request(format!(
            "Unsupported output type: {other}"
        ))),
    }
}

async fn handle_tags_output(
    index_db: &str,
    model: &ModelMetadata,
    job_id: i64,
    item: &JobInputData,
    outputs: PredictOutput,
) -> ApiResult<OutputDisposition> {
    let values = outputs.into_json("tags")?;
    if values.is_empty() {
        let _ = call_index_db_writer(index_db, |reply| IndexDbWriterMessage::WriteTagsOutput {
            job_id,
            setter_name: model.setter_name.clone(),
            item_sha256: item.sha256.clone(),
            tags: Vec::new(),
            text_entries: Vec::new(),
            reply,
        })
        .await?;
        return Ok(OutputDisposition::Written);
    }

    if values[0]
        .get("skip")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        tracing::info!(sha256 = %item.sha256, "skipping tag output");
        return Ok(OutputDisposition::Skipped);
    }

    let total_tag_groups: usize = values
        .iter()
        .map(|entry| {
            entry
                .get("tags")
                .and_then(Value::as_array)
                .map(|v| v.len())
                .unwrap_or(0)
        })
        .sum();
    if total_tag_groups == 0 {
        let _ = call_index_db_writer(index_db, |reply| IndexDbWriterMessage::WriteTagsOutput {
            job_id,
            setter_name: model.setter_name.clone(),
            item_sha256: item.sha256.clone(),
            tags: Vec::new(),
            text_entries: Vec::new(),
            reply,
        })
        .await?;
        return Ok(OutputDisposition::Written);
    }

    let mut tag_results = Vec::new();
    for entry in values {
        if let Some(result) = TagResult::from_value(entry) {
            tag_results.push(result);
        }
    }
    if tag_results.is_empty() {
        return Err(ApiError::internal("Tag outputs missing required fields"));
    }

    let main_namespace = tag_results[0].namespace.clone();
    let rating_severity = tag_results[0].rating_severity.clone();
    let aggregated = aggregate_tags(
        tag_results.iter().map(|r| r.tags.clone()).collect(),
        &rating_severity,
    );
    let mut tags = Vec::new();
    for (namespace, name, confidence) in aggregated {
        tags.push(TagEntry {
            namespace: format!("{main_namespace}:{namespace}"),
            name,
            confidence,
        });
    }

    if tags.is_empty() {
        let _ = call_index_db_writer(index_db, |reply| IndexDbWriterMessage::WriteTagsOutput {
            job_id,
            setter_name: model.setter_name.clone(),
            item_sha256: item.sha256.clone(),
            tags: Vec::new(),
            text_entries: Vec::new(),
            reply,
        })
        .await?;
        return Ok(OutputDisposition::Written);
    }

    let mut text_entries = Vec::new();
    let all_tags_string = tags
        .iter()
        .map(|entry| entry.name.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    let min_confidence = tags
        .iter()
        .map(|entry| entry.confidence)
        .fold(f64::INFINITY, f64::min);
    text_entries.push(TagTextEntry {
        index: 0,
        text: all_tags_string,
        language: main_namespace.clone(),
        language_confidence: 1.0,
        confidence: min_confidence,
    });

    if tag_results[0].mcut > 0.0 {
        let general_scores: Vec<f64> = tags
            .iter()
            .filter(|entry| entry.namespace.ends_with(":general"))
            .map(|entry| entry.confidence)
            .collect();
        if !general_scores.is_empty() {
            let m_thresh = mcut_threshold(&general_scores);
            let mcut_tags = tags
                .iter()
                .filter(|entry| {
                    !entry.namespace.ends_with(":general") || entry.confidence >= m_thresh
                })
                .map(|entry| entry.name.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            text_entries.push(TagTextEntry {
                index: 1,
                text: mcut_tags,
                language: format!("{main_namespace}-mcut"),
                language_confidence: 1.0,
                confidence: m_thresh,
            });
        }
    }

    if let Some(metadata) = &tag_results[0].metadata {
        let metadata_text = serde_json::to_string(metadata).unwrap_or_default();
        text_entries.push(TagTextEntry {
            index: 2,
            text: metadata_text,
            language: "metadata".to_string(),
            language_confidence: 1.0,
            confidence: tag_results[0].metadata_score,
        });
    }

    call_index_db_writer(index_db, |reply| IndexDbWriterMessage::WriteTagsOutput {
        job_id,
        setter_name: model.setter_name.clone(),
        item_sha256: item.sha256.clone(),
        tags: tags.clone(),
        text_entries: text_entries.clone(),
        reply,
    })
    .await?;
    Ok(OutputDisposition::Written)
}

async fn handle_text_output(
    index_db: &str,
    model: &ModelMetadata,
    job_id: i64,
    item: &JobInputData,
    outputs: PredictOutput,
) -> ApiResult<OutputDisposition> {
    let values = outputs.into_json("text")?;
    let mut entries = Vec::new();
    let mut seen = HashSet::new();
    for (idx, value) in values.iter().enumerate() {
        let transcription = value
            .get("transcription")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim()
            .to_string();
        if transcription.len() < 3 {
            continue;
        }
        let key = transcription.to_lowercase();
        if !seen.insert(key) {
            continue;
        }
        let confidence = value.get("confidence").and_then(Value::as_f64);
        let language = value
            .get("language")
            .and_then(Value::as_str)
            .map(|s| s.to_string());
        let language_confidence = value.get("language_confidence").and_then(Value::as_f64);
        entries.push(TextEntry {
            index: idx as i64,
            text: transcription,
            language,
            language_confidence,
            confidence,
        });
    }

    call_index_db_writer(index_db, |reply| IndexDbWriterMessage::WriteTextOutput {
        job_id,
        setter_name: model.setter_name.clone(),
        item_sha256: item.sha256.clone(),
        entries: entries.clone(),
        reply,
    })
    .await?;
    Ok(OutputDisposition::Written)
}

async fn handle_clip_output(
    index_db: &str,
    model: &ModelMetadata,
    job_id: i64,
    item: &JobInputData,
    outputs: PredictOutput,
) -> ApiResult<OutputDisposition> {
    let mut entries = Vec::new();
    match outputs {
        PredictOutput::Binary(buffers) => {
            for (idx, buffer) in buffers.iter().enumerate() {
                let embedding = parse_npy_to_f32(buffer)?;
                entries.push(EmbeddingEntry {
                    index: idx as i64,
                    embedding: serialize_f32(&embedding),
                });
            }
        }
        PredictOutput::Json(values) => {
            for (idx, value) in values.iter().enumerate() {
                let embedding = parse_embedding_json(value)?;
                entries.push(EmbeddingEntry {
                    index: idx as i64,
                    embedding: serialize_f32(&embedding),
                });
            }
        }
    }

    call_index_db_writer(index_db, |reply| IndexDbWriterMessage::WriteClipOutput {
        job_id,
        setter_name: model.setter_name.clone(),
        item_sha256: item.sha256.clone(),
        entries: entries.clone(),
        reply,
    })
    .await?;
    Ok(OutputDisposition::Written)
}

async fn handle_text_embedding_output(
    index_db: &str,
    model: &ModelMetadata,
    job_id: i64,
    item: &JobInputData,
    outputs: PredictOutput,
) -> ApiResult<OutputDisposition> {
    let source_data_id = item.data_id;
    let mut entries = Vec::new();
    let buffers = outputs.into_binary("text-embedding")?;
    if buffers.is_empty() {
        call_index_db_writer(index_db, |reply| {
            IndexDbWriterMessage::WriteTextEmbeddingOutput {
                job_id,
                setter_name: model.setter_name.clone(),
                item_sha256: item.sha256.clone(),
                source_data_id,
                entries: Vec::new(),
                reply,
            }
        })
        .await?;
        return Ok(OutputDisposition::Written);
    }
    if buffers.len() != 1 {
        return Err(ApiError::internal("Text embedding output mismatch"));
    }
    let embedding_rows = parse_npy_to_f32_rows(&buffers[0])?;
    for (idx, embedding) in embedding_rows.into_iter().enumerate() {
        entries.push(EmbeddingEntry {
            index: idx as i64,
            embedding: serialize_f32(&embedding),
        });
    }

    call_index_db_writer(index_db, |reply| {
        IndexDbWriterMessage::WriteTextEmbeddingOutput {
            job_id,
            setter_name: model.setter_name.clone(),
            item_sha256: item.sha256.clone(),
            source_data_id,
            entries: entries.clone(),
            reply,
        }
    })
    .await?;
    Ok(OutputDisposition::Written)
}
async fn prepare_item(
    index_db: &str,
    model: &ModelMetadata,
    item: JobInputData,
) -> ApiResult<PreparedItem> {
    let inputs = match model.input_handler.as_str() {
        "image_frames" => build_image_frames_inputs(index_db, &item, model).await?,
        "audio_tracks" => build_audio_tracks_inputs(&item, model).await?,
        "audio_files" => build_audio_files_inputs(&item, model).await?,
        "extracted_text" => build_extracted_text_inputs(&item)?,
        "md5" => build_md5_inputs(&item)?,
        "md5_image" => build_md5_image_inputs(index_db, &item).await?,
        "sha256_md5_path" => build_sha256_md5_path_inputs(&item)?,
        handler => {
            return Err(ApiError::bad_request(format!(
                "Unknown input handler: {handler}"
            )));
        }
    };
    Ok(PreparedItem { item, inputs })
}

async fn build_image_frames_inputs(
    index_db: &str,
    item: &JobInputData,
    model: &ModelMetadata,
) -> ApiResult<Vec<InferenceInput>> {
    let opts = &model.input_handler_opts;
    let max_frames = opts.get("max_frames").and_then(Value::as_i64).unwrap_or(4) as usize;
    let slice_frames = opts
        .get("slice_frames")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let slice_settings = if slice_frames {
        opts.get("slice_settings")
            .map(ImageSliceSettings::from_value)
            .transpose()?
    } else {
        None
    };

    if let (Some(width), Some(height)) = (item.width, item.height) {
        if width < 3 || height < 3 {
            return Ok(Vec::new());
        }
    }

    let frames = load_base_frames(index_db, item).await?;

    if frames.is_empty() {
        return Ok(Vec::new());
    }

    let sliced = slice_target_size(frames, item.width, item.height, slice_settings.as_ref())?;

    let mut outputs = Vec::new();
    for frame in sliced.into_iter().take(max_frames) {
        outputs.push(InferenceInput::new(
            json!({}),
            Some(InferenceFile::Bytes(frame)),
        ));
    }
    Ok(outputs)
}

async fn load_base_frames(index_db: &str, item: &JobInputData) -> ApiResult<Vec<Vec<u8>>> {
    if item.item_type.starts_with("image/gif") {
        return gif_to_frames(&item.path);
    }
    if item.item_type.starts_with("image") {
        let buffer = tokio::fs::read(&item.path).await.map_err(|err| {
            tracing::error!(error = %err, path = %item.path, "failed to read image");
            ApiError::internal("Failed to read image")
        })?;
        return Ok(vec![buffer]);
    }
    if item.item_type.starts_with("video") {
        let mut conn = open_index_db_read_no_user_data(index_db).await?;
        let cached = get_frames_bytes(&mut conn, &item.sha256)
            .await
            .unwrap_or_default();
        if !cached.is_empty() {
            return Ok(cached);
        }
        if item.duration.unwrap_or(0.0) > 0.0 && item.video_tracks.unwrap_or(0) > 0 {
            let extracted = tokio::task::spawn_blocking({
                let path = item.path.clone();
                move || extract_video_frames(&path, 4)
            })
            .await
            .map_err(|_| ApiError::internal("Failed to extract frames"))??;
            let frames = extracted
                .iter()
                .map(|img| encode_jpeg(img))
                .collect::<Result<Vec<_>, ApiError>>()?;

            let mut stored = Vec::with_capacity(extracted.len());
            for (idx, img) in extracted.iter().enumerate() {
                stored.push(StoredImage {
                    idx: idx as i64,
                    width: img.width() as i64,
                    height: img.height() as i64,
                    bytes: encode_jpeg(img)?,
                });
            }
            let _ = call_index_db_writer(index_db, |reply| IndexDbWriterMessage::StoreFrames {
                sha256: item.sha256.clone(),
                mime_type: item.item_type.clone(),
                process_version: 1,
                frames: stored.clone(),
                reply,
            })
            .await;
            return Ok(frames);
        }
        return Ok(Vec::new());
    }
    if item.item_type.starts_with("application/pdf") {
        return render_pdf_frames(&item.path);
    }
    if item.item_type.starts_with("text/html") {
        return render_html_frames(&item.path);
    }
    Ok(Vec::new())
}

async fn build_audio_tracks_inputs(
    item: &JobInputData,
    model: &ModelMetadata,
) -> ApiResult<Vec<InferenceInput>> {
    if !item.item_type.starts_with("video") && !item.item_type.starts_with("audio") {
        return Ok(Vec::new());
    }
    let opts = &model.input_handler_opts;
    let sample_rate = opts
        .get("sample_rate")
        .and_then(Value::as_i64)
        .unwrap_or(16000) as u32;
    let max_tracks = opts.get("max_tracks").and_then(Value::as_i64).unwrap_or(4) as usize;

    let audio = load_audio_single(&item.path, sample_rate)?;
    let mut outputs = Vec::new();
    for track in audio.into_iter().take(max_tracks) {
        let bytes = serialize_npy_f32(&track);
        outputs.push(InferenceInput::new(
            json!({}),
            Some(InferenceFile::Bytes(bytes)),
        ));
    }
    Ok(outputs)
}

async fn build_audio_files_inputs(
    item: &JobInputData,
    model: &ModelMetadata,
) -> ApiResult<Vec<InferenceInput>> {
    if !item.item_type.starts_with("video") && !item.item_type.starts_with("audio") {
        return Ok(Vec::new());
    }
    let opts = &model.input_handler_opts;
    let sample_rate = opts
        .get("sample_rate")
        .and_then(Value::as_i64)
        .unwrap_or(48000) as u32;
    let max_tracks = opts.get("max_tracks").and_then(Value::as_i64).unwrap_or(4) as usize;

    let audio = load_audio_single(&item.path, sample_rate)?;
    let mut outputs = Vec::new();
    for track in audio.into_iter().take(max_tracks) {
        let wav_bytes = audio_to_wav_bytes(&track, sample_rate);
        outputs.push(InferenceInput::new(
            json!({"type": "audio"}),
            Some(InferenceFile::Bytes(wav_bytes)),
        ));
    }
    Ok(outputs)
}

fn build_extracted_text_inputs(item: &JobInputData) -> ApiResult<Vec<InferenceInput>> {
    let Some(text) = item.text.clone() else {
        return Err(ApiError::bad_request("Text input missing text field"));
    };
    Ok(vec![InferenceInput::new(json!({"text": text}), None)])
}

fn build_md5_inputs(item: &JobInputData) -> ApiResult<Vec<InferenceInput>> {
    Ok(vec![InferenceInput::new(json!({"md5": item.md5}), None)])
}

async fn build_md5_image_inputs(
    index_db: &str,
    item: &JobInputData,
) -> ApiResult<Vec<InferenceInput>> {
    let frames = load_base_frames(index_db, item).await?;
    let frame = frames.into_iter().next();

    Ok(vec![InferenceInput::new(
        json!({"md5": item.md5}),
        frame.map(InferenceFile::Bytes),
    )])
}

fn build_sha256_md5_path_inputs(item: &JobInputData) -> ApiResult<Vec<InferenceInput>> {
    Ok(vec![InferenceInput::new(
        json!({"sha256": item.sha256, "md5": item.md5, "path": item.path}),
        None,
    )])
}

fn apply_threshold(mut inputs: Vec<InferenceInput>, threshold: Option<f64>) -> Vec<InferenceInput> {
    let Some(threshold) = threshold else {
        return inputs;
    };
    for input in inputs.iter_mut() {
        if let Value::Object(map) = &mut input.data {
            map.insert("threshold".to_string(), Value::from(threshold));
        } else {
            input.data = Value::Object(serde_json::Map::from_iter([(
                "threshold".to_string(),
                Value::from(threshold),
            )]));
        }
    }
    inputs
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

impl PredictOutput {
    fn into_json(self, label: &str) -> ApiResult<Vec<Value>> {
        match self {
            PredictOutput::Json(values) => Ok(values),
            PredictOutput::Binary(_) => Err(ApiError::internal(format!(
                "Expected JSON output for {label}"
            ))),
        }
    }

    fn into_binary(self, label: &str) -> ApiResult<Vec<Vec<u8>>> {
        match self {
            PredictOutput::Binary(values) => Ok(values),
            PredictOutput::Json(_) => Err(ApiError::internal(format!(
                "Expected binary output for {label}"
            ))),
        }
    }
}
#[derive(Debug, Clone)]
struct TagResult {
    namespace: String,
    tags: Vec<(String, HashMap<String, f64>)>,
    mcut: f64,
    rating_severity: Vec<String>,
    metadata: Option<serde_json::Map<String, Value>>,
    metadata_score: f64,
}

impl TagResult {
    fn from_value(value: Value) -> Option<Self> {
        let namespace = value.get("namespace")?.as_str()?.to_string();
        let tags_array = value.get("tags")?.as_array()?.clone();
        let mut tags = Vec::new();
        for entry in tags_array {
            let arr = entry.as_array()?;
            if arr.len() != 2 {
                continue;
            }
            let ns = arr[0].as_str()?.to_string();
            let tag_map = arr[1].as_object()?;
            let mut tags_obj = HashMap::new();
            for (tag, score) in tag_map {
                if let Some(score) = score.as_f64() {
                    tags_obj.insert(tag.clone(), score);
                }
            }
            tags.push((ns, tags_obj));
        }
        let mcut = value.get("mcut").and_then(Value::as_f64).unwrap_or(0.0);
        let rating_severity = value
            .get("rating_severity")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let metadata = value.get("metadata").and_then(Value::as_object).cloned();
        let metadata_score = value
            .get("metadata_score")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);

        Some(TagResult {
            namespace,
            tags,
            mcut,
            rating_severity,
            metadata,
            metadata_score,
        })
    }
}

fn aggregate_tags(
    namespaces_tags: Vec<Vec<(String, HashMap<String, f64>)>>,
    severity_order: &[String],
) -> Vec<(String, String, f64)> {
    let mut combined: HashMap<String, Vec<HashMap<String, f64>>> = HashMap::new();
    for namespaces in namespaces_tags {
        for (namespace, tags) in namespaces {
            combined.entry(namespace).or_default().push(tags);
        }
    }

    let mut output = Vec::new();
    for (namespace, tags) in combined {
        if namespace == "rating" {
            if let Some((rating, score)) = get_rating(&tags, severity_order) {
                output.push((namespace.clone(), format!("rating:{rating}"), score));
            }
        } else {
            let combined = combine_tags(&tags);
            for (tag, score) in combined {
                output.push((namespace.clone(), tag, score));
            }
        }
    }
    output.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));
    output
}

fn combine_tags(tags: &[HashMap<String, f64>]) -> Vec<(String, f64)> {
    let mut combined: HashMap<String, f64> = HashMap::new();
    for entry in tags {
        for (tag, score) in entry {
            let update = combined
                .get(tag)
                .map(|existing| existing.max(*score))
                .unwrap_or(*score);
            combined.insert(tag.clone(), update);
        }
    }
    let mut result = combined.into_iter().collect::<Vec<_>>();
    result.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    result
}

fn get_rating(tags: &[HashMap<String, f64>], severity_order: &[String]) -> Option<(String, f64)> {
    let mut severity_map = HashMap::new();
    for (idx, label) in severity_order.iter().enumerate() {
        severity_map.insert(label, idx);
    }
    let mut final_rating: Option<String> = None;
    let mut final_score = 0.0;
    for entry in tags {
        if let Some((rating, score)) = entry
            .iter()
            .max_by(|a, b| a.1.partial_cmp(b.1).unwrap_or(std::cmp::Ordering::Equal))
        {
            let rating_idx = *severity_map.get(&rating.to_string()).unwrap_or(&0);
            let current_idx = final_rating
                .as_ref()
                .and_then(|r| severity_map.get(r))
                .copied()
                .unwrap_or(0);
            if final_rating.is_none()
                || rating_idx > current_idx
                || (rating_idx == current_idx && *score > final_score)
            {
                final_rating = Some(rating.to_string());
                final_score = *score;
            }
        }
    }
    final_rating.map(|rating| (rating, final_score))
}

fn mcut_threshold(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal));
    let mut max_diff = 0.0;
    let mut idx = 0usize;
    for i in 0..sorted.len().saturating_sub(1) {
        let diff = sorted[i] - sorted[i + 1];
        if diff > max_diff {
            max_diff = diff;
            idx = i;
        }
    }
    if idx + 1 >= sorted.len() {
        return sorted[0];
    }
    (sorted[idx] + sorted[idx + 1]) / 2.0
}

fn parse_embedding_json(value: &Value) -> ApiResult<Vec<f32>> {
    let arr = value
        .as_array()
        .ok_or_else(|| ApiError::internal("Embedding output must be an array"))?;
    let mut embedding = Vec::with_capacity(arr.len());
    for v in arr {
        if let Some(value) = v.as_f64() {
            embedding.push(value as f32);
        }
    }
    Ok(embedding)
}

fn serialize_f32(values: &[f32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(values.len() * 4);
    for value in values {
        out.extend_from_slice(&value.to_le_bytes());
    }
    out
}

fn serialize_npy_f32(values: &[f32]) -> Vec<u8> {
    let mut header = format!(
        "{{'descr': '<f4', 'fortran_order': False, 'shape': ({},), }}",
        values.len()
    );
    let header_len = header.len() + 1;
    let padding = (16 - ((10 + header_len) % 16)) % 16;
    header.push_str(&" ".repeat(padding));
    header.push('\n');
    let header_len = header.len() as u16;

    let mut out = Vec::with_capacity(10 + header.len() + values.len() * 4);
    out.extend_from_slice(b"\x93NUMPY");
    out.push(1);
    out.push(0);
    out.extend_from_slice(&header_len.to_le_bytes());
    out.extend_from_slice(header.as_bytes());
    for value in values {
        out.extend_from_slice(&value.to_le_bytes());
    }
    out
}

fn parse_npy_to_f32(buffer: &[u8]) -> ApiResult<Vec<f32>> {
    let (shape, data) = parse_npy(buffer)?;
    if shape.len() != 1 {
        return Err(ApiError::internal("Expected 1D embedding"));
    }
    Ok(data)
}

fn parse_npy_to_f32_rows(buffer: &[u8]) -> ApiResult<Vec<Vec<f32>>> {
    let (shape, data) = parse_npy(buffer)?;
    if shape.len() == 1 {
        return Ok(vec![data]);
    }
    if shape.len() != 2 {
        return Err(ApiError::internal("Expected 1D or 2D embedding"));
    }
    let rows = shape[0];
    let cols = shape[1];
    if rows * cols != data.len() {
        return Err(ApiError::internal("Embedding shape mismatch"));
    }
    let mut out = Vec::with_capacity(rows);
    for row in 0..rows {
        let start = row * cols;
        out.push(data[start..start + cols].to_vec());
    }
    Ok(out)
}

fn parse_npy(buffer: &[u8]) -> ApiResult<(Vec<usize>, Vec<f32>)> {
    const MAGIC: &[u8] = b"\x93NUMPY";
    if buffer.len() < 10 || &buffer[..6] != MAGIC {
        return Err(ApiError::internal("Invalid NPY buffer"));
    }
    let major = buffer[6];
    let header_len = match major {
        1 => u16::from_le_bytes([buffer[8], buffer[9]]) as usize,
        2 | 3 => u32::from_le_bytes([buffer[8], buffer[9], buffer[10], buffer[11]]) as usize,
        _ => return Err(ApiError::internal("Unsupported NPY version")),
    };
    let header_start = if major == 1 { 10 } else { 12 };
    let header_end = header_start + header_len;
    if buffer.len() < header_end {
        return Err(ApiError::internal("Invalid NPY header"));
    }
    let header = std::str::from_utf8(&buffer[header_start..header_end])
        .map_err(|_| ApiError::internal("Invalid NPY header"))?;
    let descr =
        parse_npy_field(header, "descr").ok_or_else(|| ApiError::internal("NPY descr missing"))?;
    if descr != "<f4" {
        return Err(ApiError::internal("Unsupported NPY dtype"));
    }
    let fortran = parse_npy_field(header, "fortran_order").unwrap_or("False".to_string());
    if fortran.trim() != "False" {
        return Err(ApiError::internal("Fortran order not supported"));
    }
    let shape_str =
        parse_npy_field(header, "shape").ok_or_else(|| ApiError::internal("NPY shape missing"))?;
    let shape = parse_shape(&shape_str)?;
    let data_start = header_end;
    let expected = shape.iter().product::<usize>() * 4;
    if buffer.len() < data_start + expected {
        return Err(ApiError::internal("NPY data truncated"));
    }
    let mut values = Vec::with_capacity(expected / 4);
    for chunk in buffer[data_start..data_start + expected].chunks_exact(4) {
        values.push(f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
    }
    Ok((shape, values))
}

fn parse_npy_field(header: &str, key: &str) -> Option<String> {
    let needle = format!("'{}':", key);
    let idx = header.find(&needle)?;
    let value_start = idx + needle.len();
    let rest = header[value_start..].trim_start();
    let mut depth: i32 = 0;
    let mut end = 0;
    for (i, ch) in rest.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth = depth.saturating_sub(1);
            }
            ',' if depth == 0 => {
                end = i;
                break;
            }
            _ => {}
        }
    }
    if end == 0 {
        end = rest.len();
    }
    Some(rest[..end].trim().trim_matches('\'').to_string())
}

fn parse_shape(shape_str: &str) -> ApiResult<Vec<usize>> {
    let trimmed = shape_str.trim().trim_matches(|c| c == '(' || c == ')');
    if trimmed.is_empty() {
        return Err(ApiError::internal("Invalid NPY shape"));
    }
    let mut dims = Vec::new();
    for part in trimmed.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let value = part
            .parse::<usize>()
            .map_err(|_| ApiError::internal("Invalid NPY shape"))?;
        dims.push(value);
    }
    if dims.is_empty() {
        return Err(ApiError::internal("Invalid NPY shape"));
    }
    Ok(dims)
}
#[derive(Debug, Clone)]
struct ImageSliceSettings {
    mode: String,
    ratio_larger: f64,
    ratio_smaller: f64,
    max_multiplier: f64,
    target_multiplier: f64,
    minimum_size: f64,
    pixel_target_size: f64,
    pixel_max_size: f64,
}

impl ImageSliceSettings {
    fn from_value(value: &Value) -> ApiResult<Self> {
        let obj = value
            .as_object()
            .ok_or_else(|| ApiError::bad_request("slice_settings must be an object"))?;
        Ok(Self {
            mode: obj
                .get("mode")
                .and_then(Value::as_str)
                .unwrap_or("aspect-ratio")
                .to_string(),
            ratio_larger: obj
                .get("ratio_larger")
                .and_then(Value::as_f64)
                .unwrap_or(16.0),
            ratio_smaller: obj
                .get("ratio_smaller")
                .and_then(Value::as_f64)
                .unwrap_or(9.0),
            max_multiplier: obj
                .get("max_multiplier")
                .and_then(Value::as_f64)
                .unwrap_or(2.0),
            target_multiplier: obj
                .get("target_multiplier")
                .and_then(Value::as_f64)
                .unwrap_or(1.5),
            minimum_size: obj
                .get("minimum_size")
                .and_then(Value::as_f64)
                .unwrap_or(1024.0),
            pixel_target_size: obj
                .get("pixel_target_size")
                .and_then(Value::as_f64)
                .unwrap_or(1024.0),
            pixel_max_size: obj
                .get("pixel_max_size")
                .and_then(Value::as_f64)
                .unwrap_or(4096.0),
        })
    }
}

fn slice_target_size(
    input_images: Vec<Vec<u8>>,
    width: Option<i64>,
    height: Option<i64>,
    settings: Option<&ImageSliceSettings>,
) -> ApiResult<Vec<Vec<u8>>> {
    let (Some(width), Some(height), Some(settings)) = (width, height, settings) else {
        return Ok(input_images);
    };
    let width = width as f64;
    let height = height as f64;
    match settings.mode.as_str() {
        "aspect-ratio" => {
            if width.max(height) <= settings.minimum_size
                || !is_excessive_ratio(width, height, settings)
            {
                return Ok(input_images);
            }
            let slices = calculate_slices_needed(width, height, settings);
            let mut output = Vec::new();
            for image in input_images {
                output.extend(slice_image(&image, slices)?);
            }
            Ok(output)
        }
        "pixels" => {
            if width.max(height) <= settings.pixel_max_size {
                return Ok(input_images);
            }
            let (rows, cols) = grid_for_pixels(width, height, settings);
            let mut output = Vec::new();
            for image in input_images {
                output.extend(slice_image_grid(&image, rows, cols)?);
            }
            Ok(output)
        }
        _ => Ok(input_images),
    }
}

fn is_excessive_ratio(width: f64, height: f64, settings: &ImageSliceSettings) -> bool {
    let image_ratio = if width >= height {
        width / height
    } else {
        height / width
    };
    let target_ratio = settings.ratio_larger / settings.ratio_smaller;
    image_ratio > (target_ratio * settings.max_multiplier)
}

fn calculate_slices_needed(width: f64, height: f64, settings: &ImageSliceSettings) -> usize {
    let is_landscape = width >= height;
    let image_ratio = if is_landscape {
        width / height
    } else {
        height / width
    };
    let base_ratio = settings.ratio_larger / settings.ratio_smaller;
    let max_ratio = base_ratio * settings.max_multiplier;
    let target_ratio = base_ratio * settings.target_multiplier;
    if image_ratio <= max_ratio {
        return 1;
    }
    ((image_ratio / target_ratio).ceil() as usize).max(1)
}

fn slice_image(image_bytes: &[u8], num_slices: usize) -> ApiResult<Vec<Vec<u8>>> {
    let image = load_dynamic_image(image_bytes)?;
    let (width, height) = image.dimensions();
    let mut output = Vec::new();
    if width >= height {
        let slice_width = width / num_slices as u32;
        for idx in 0..num_slices {
            let start = idx as u32 * slice_width;
            let end = if idx == num_slices - 1 {
                width
            } else {
                start + slice_width
            };
            let cropped = image.crop_imm(start, 0, end - start, height);
            output.push(encode_jpeg(&cropped)?);
        }
    } else {
        let slice_height = height / num_slices as u32;
        for idx in 0..num_slices {
            let start = idx as u32 * slice_height;
            let end = if idx == num_slices - 1 {
                height
            } else {
                start + slice_height
            };
            let cropped = image.crop_imm(0, start, width, end - start);
            output.push(encode_jpeg(&cropped)?);
        }
    }
    Ok(output)
}

fn grid_for_pixels(width: f64, height: f64, settings: &ImageSliceSettings) -> (usize, usize) {
    let rows = (height / settings.pixel_target_size).ceil().max(1.0) as usize;
    let cols = (width / settings.pixel_target_size).ceil().max(1.0) as usize;
    (rows, cols)
}

fn slice_image_grid(image_bytes: &[u8], rows: usize, cols: usize) -> ApiResult<Vec<Vec<u8>>> {
    let image = load_dynamic_image(image_bytes)?;
    let (width, height) = image.dimensions();
    let tile_w = width as f64 / cols as f64;
    let tile_h = height as f64 / rows as f64;
    let mut output = Vec::new();
    for row in 0..rows {
        for col in 0..cols {
            let left = (col as f64 * tile_w).round() as u32;
            let top = (row as f64 * tile_h).round() as u32;
            let right = ((col + 1) as f64 * tile_w).round() as u32;
            let bottom = ((row + 1) as f64 * tile_h).round() as u32;
            let cropped = image.crop_imm(left, top, right - left, bottom - top);
            output.push(encode_jpeg(&cropped)?);
        }
    }
    Ok(output)
}

fn load_dynamic_image(buffer: &[u8]) -> ApiResult<DynamicImage> {
    image::load_from_memory(buffer).map_err(|err| {
        tracing::error!(error = %err, "failed to decode image");
        ApiError::internal("Failed to decode image")
    })
}

fn encode_jpeg(image: &DynamicImage) -> ApiResult<Vec<u8>> {
    let mut buffer = Vec::new();
    let mut encoder = image::codecs::jpeg::JpegEncoder::new_with_quality(&mut buffer, 85);
    let rgb = image.to_rgb8();
    encoder
        .encode(
            &rgb,
            rgb.width(),
            rgb.height(),
            image::ColorType::Rgb8.into(),
        )
        .map_err(|err| {
            tracing::error!(error = %err, "failed to encode image");
            ApiError::internal("Failed to encode image")
        })?;
    Ok(buffer)
}

fn gif_to_frames(path: &str) -> ApiResult<Vec<Vec<u8>>> {
    let file = std::fs::File::open(path).map_err(|err| {
        tracing::error!(error = %err, "failed to open gif");
        ApiError::internal("Failed to open gif")
    })?;
    let decoder = GifDecoder::new(std::io::BufReader::new(file)).map_err(|err| {
        tracing::error!(error = %err, "failed to decode gif");
        ApiError::internal("Failed to decode gif")
    })?;
    let frames = decoder.into_frames().collect_frames().map_err(|err| {
        tracing::error!(error = %err, "failed to collect gif frames");
        ApiError::internal("Failed to decode gif")
    })?;
    if frames.is_empty() {
        return Ok(Vec::new());
    }
    let total_frames = frames.len();
    let step = std::cmp::max(total_frames / 4, 1);
    let mut output = Vec::new();
    for (idx, frame) in frames.into_iter().enumerate() {
        if idx % step == 0 {
            let image: image::RgbaImage = frame.into_buffer();
            let image = DynamicImage::ImageRgba8(image);
            output.push(encode_jpeg(&image)?);
        }
        if output.len() >= 4 {
            break;
        }
    }
    Ok(output)
}

fn extract_video_frames(path: &str, num_frames: usize) -> ApiResult<Vec<DynamicImage>> {
    let duration = probe_duration(path)?;
    if duration <= 0.0 {
        return Ok(Vec::new());
    }
    let interval = duration / num_frames as f64;
    let temp_dir = temp_dir_path();
    std::fs::create_dir_all(&temp_dir).map_err(|err| {
        tracing::error!(error = %err, "failed to create temp dir");
        ApiError::internal("Failed to extract frames")
    })?;
    let output_pattern = temp_dir.join("frame_%04d.png");
    let status = std::process::Command::new("ffmpeg")
        .arg("-i")
        .arg(path)
        .arg("-vf")
        .arg(format!("fps=1/{interval}"))
        .arg("-vsync")
        .arg("vfr")
        .arg(&output_pattern)
        .status()
        .map_err(|err| {
            tracing::error!(error = %err, "ffmpeg failed");
            ApiError::internal("Failed to extract frames")
        })?;
    if !status.success() {
        return Err(ApiError::internal("ffmpeg failed to extract frames"));
    }
    let mut paths = std::fs::read_dir(&temp_dir)
        .map_err(|err| ApiError::internal(format!("Failed to read frames: {err}")))?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("png"))
        .collect::<Vec<_>>();
    paths.sort();
    let mut frames = Vec::new();
    for frame_path in paths.into_iter().take(num_frames) {
        if let Ok(image) = image::open(&frame_path) {
            frames.push(image);
        }
        let _ = std::fs::remove_file(&frame_path);
    }
    Ok(frames)
}

fn probe_duration(path: &str) -> ApiResult<f64> {
    let output = std::process::Command::new("ffprobe")
        .arg("-v")
        .arg("error")
        .arg("-show_entries")
        .arg("format=duration")
        .arg("-of")
        .arg("default=noprint_wrappers=1:nokey=1")
        .arg(path)
        .output()
        .map_err(|err| {
            tracing::error!(error = %err, "ffprobe failed");
            ApiError::internal("Failed to probe video")
        })?;
    if !output.status.success() {
        return Err(ApiError::internal("ffprobe failed"));
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout.trim().parse::<f64>().map_err(|err| {
        tracing::error!(error = %err, "failed to parse duration");
        ApiError::internal("Failed to probe video")
    })
}

fn temp_dir_path() -> PathBuf {
    let base = std::env::temp_dir();
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    base.join(format!("panoptikon-extract-{unique}"))
}

fn render_pdf_frames(path: &str) -> ApiResult<Vec<Vec<u8>>> {
    render_pdf_or_html_frames(path, None)
}

fn render_html_frames(path: &str) -> ApiResult<Vec<Vec<u8>>> {
    render_pdf_or_html_frames(path, Some("html"))
}

fn render_pdf_or_html_frames(path: &str, kind: Option<&str>) -> ApiResult<Vec<Vec<u8>>> {
    let temp_dir = temp_dir_path();
    std::fs::create_dir_all(&temp_dir).map_err(|err| {
        tracing::error!(error = %err, "failed to create temp dir");
        ApiError::internal("Failed to render document")
    })?;
    let script = r#"
import sys
from pathlib import Path
try:
    import pypdfium2 as pdfium
except Exception as e:
    print(f"pypdfium2 unavailable: {e}", file=sys.stderr)
    sys.exit(2)

kind = sys.argv[1]
src = sys.argv[2]
out_dir = Path(sys.argv[3])

if kind == "html":
    try:
        from weasyprint import HTML
    except Exception as e:
        print(f"weasyprint unavailable: {e}", file=sys.stderr)
        sys.exit(2)
    pdf_bytes = HTML(src).write_pdf()
    doc = pdfium.PdfDocument(pdf_bytes)
else:
    doc = pdfium.PdfDocument(src)

for idx, page in enumerate(doc):
    image = page.render(scale=2, rev_byteorder=True).to_pil()
    image.save(out_dir / f"page_{idx:04d}.jpg", format="JPEG")
    image.close()
doc.close()
"#;
    let kind_arg = kind.unwrap_or("pdf");
    let output = std::process::Command::new("python")
        .arg("-c")
        .arg(script)
        .arg(kind_arg)
        .arg(path)
        .arg(temp_dir.to_string_lossy().to_string())
        .output()
        .map_err(|err| {
            tracing::error!(error = %err, "failed to render document");
            ApiError::internal("Failed to render document")
        })?;
    if !output.status.success() {
        tracing::warn!(
            stderr = %String::from_utf8_lossy(&output.stderr),
            "document render failed"
        );
        return Ok(Vec::new());
    }
    let mut paths = std::fs::read_dir(&temp_dir)
        .map_err(|err| ApiError::internal(format!("Failed to read rendered pages: {err}")))?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("jpg"))
        .collect::<Vec<_>>();
    paths.sort();
    let mut pages = Vec::new();
    for page_path in paths {
        if let Ok(bytes) = std::fs::read(&page_path) {
            pages.push(bytes);
        }
        let _ = std::fs::remove_file(&page_path);
    }
    Ok(pages)
}

fn load_audio_single(path: &str, sample_rate: u32) -> ApiResult<Vec<Vec<f32>>> {
    let output = std::process::Command::new("ffmpeg")
        .arg("-nostdin")
        .arg("-threads")
        .arg("0")
        .arg("-i")
        .arg(path)
        .arg("-f")
        .arg("s16le")
        .arg("-ac")
        .arg("1")
        .arg("-acodec")
        .arg("pcm_s16le")
        .arg("-ar")
        .arg(sample_rate.to_string())
        .arg("-")
        .output();

    match output {
        Ok(output) => {
            if output.status.success() {
                let audio = s16le_to_f32(&output.stdout);
                return Ok(vec![audio]);
            }
            if !has_audio_stream(path)? {
                return Ok(Vec::new());
            }
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(ApiError::internal(format!("ffmpeg failed: {stderr}")));
        }
        Err(err) => Err(ApiError::internal(format!("ffmpeg failed: {err}"))),
    }
}

fn has_audio_stream(path: &str) -> ApiResult<bool> {
    let output = std::process::Command::new("ffprobe")
        .arg("-v")
        .arg("error")
        .arg("-show_entries")
        .arg("stream=codec_type")
        .arg("-of")
        .arg("json")
        .arg(path)
        .output()
        .map_err(|err| ApiError::internal(format!("ffprobe failed: {err}")))?;
    if !output.status.success() {
        return Ok(false);
    }
    let value: Value = serde_json::from_slice(&output.stdout).unwrap_or(Value::Null);
    let streams = value
        .get("streams")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    for stream in streams {
        if stream.get("codec_type").and_then(Value::as_str) == Some("audio") {
            return Ok(true);
        }
    }
    Ok(false)
}

fn s16le_to_f32(bytes: &[u8]) -> Vec<f32> {
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for chunk in bytes.chunks_exact(2) {
        let value = i16::from_le_bytes([chunk[0], chunk[1]]);
        out.push(value as f32 / 32768.0);
    }
    out
}

fn audio_to_wav_bytes(samples: &[f32], sample_rate: u32) -> Vec<u8> {
    let mut pcm_bytes = Vec::with_capacity(samples.len() * 2);
    for sample in samples {
        let clamped = sample.clamp(-1.0, 1.0);
        let value = (clamped * 32768.0) as i16;
        pcm_bytes.extend_from_slice(&value.to_le_bytes());
    }

    let data_size = pcm_bytes.len() as u32;
    let mut out = Vec::with_capacity(44 + pcm_bytes.len());
    out.extend_from_slice(b"RIFF");
    out.extend_from_slice(&(36 + data_size).to_le_bytes());
    out.extend_from_slice(b"WAVE");
    out.extend_from_slice(b"fmt ");
    out.extend_from_slice(&16u32.to_le_bytes());
    out.extend_from_slice(&1u16.to_le_bytes());
    out.extend_from_slice(&1u16.to_le_bytes());
    out.extend_from_slice(&sample_rate.to_le_bytes());
    let byte_rate = sample_rate * 2;
    out.extend_from_slice(&byte_rate.to_le_bytes());
    out.extend_from_slice(&2u16.to_le_bytes());
    out.extend_from_slice(&16u16.to_le_bytes());
    out.extend_from_slice(b"data");
    out.extend_from_slice(&data_size.to_le_bytes());
    out.extend_from_slice(&pcm_bytes);
    out
}
