use std::collections::HashMap;

use serde_json::Value;

use crate::api_error::ApiError;
use crate::db::extraction_write::{TagEntry, TagTextEntry};
use crate::db::index_writer::{IndexDbWriterMessage, call_index_db_writer};
use crate::inferio_client::PredictOutput;
use crate::jobs::extraction::{ApiResult, JobInputData, ModelMetadata};

use super::OutputDisposition;

pub(super) async fn handle_tags_output(
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
        // An empty metadata object counts as "no metadata", matching
        // Python's truthiness check — otherwise every item gets a junk
        // "{}" text entry.
        let metadata = value
            .get("metadata")
            .and_then(Value::as_object)
            .filter(|map| !map.is_empty())
            .cloned();
        let metadata_score = value
            .get("metadata_score")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);

        Some(Self {
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
    // Namespaces keep their first-appearance order and tags are sorted by
    // confidence within each namespace, matching the Python aggregation.
    // The order feeds the searchable/displayed "tag1, tag2, ..." text
    // entries, so display parity matters here.
    let mut namespace_order: Vec<String> = Vec::new();
    let mut combined: HashMap<String, Vec<HashMap<String, f64>>> = HashMap::new();
    for namespaces in namespaces_tags {
        for (namespace, tags) in namespaces {
            if !combined.contains_key(&namespace) {
                namespace_order.push(namespace.clone());
            }
            combined.entry(namespace).or_default().push(tags);
        }
    }

    let mut output = Vec::new();
    for namespace in namespace_order {
        let Some(tags) = combined.get(&namespace) else {
            continue;
        };
        if namespace == "rating" {
            if let Some((rating, score)) = get_rating(tags, severity_order) {
                output.push((namespace.clone(), format!("rating:{rating}"), score));
            }
        } else {
            let combined = combine_tags(tags);
            for (tag, score) in combined {
                output.push((namespace.clone(), tag, score));
            }
        }
    }
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
