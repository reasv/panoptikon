import logging
import sqlite3
from typing import (
    TYPE_CHECKING,
    Any,
    Generator,
    List,
    Sequence,
    Tuple,
)

import panoptikon.data_extractors.types as job_types
from panoptikon.db.extraction_log import get_setter_data_types
from panoptikon.db.setters import delete_setter_by_name
from panoptikon.db.tags import delete_orphan_tags
from panoptikon.types import OutputDataType
from panoptikon.utils import get_inference_api_url_weights, get_inference_api_urls

if TYPE_CHECKING:
    from panoptikon.config_type import SystemConfig
    from panoptikon.db.pql.pql_model import AndOperator

logger = logging.getLogger(__name__)

def get_inference_api_client():
    from inferio.client import DistributedInferenceAPIClient, InferenceAPIClient

    inference_api_urls = get_inference_api_urls()
    if len(inference_api_urls) == 1:
        inference_api_url = inference_api_urls[0]
        logger.debug(
            f"Using single inference API client for {inference_api_url}"
        )
        return InferenceAPIClient(f"{inference_api_url}/api/inference")
    weights = get_inference_api_url_weights()

    logger.info(
        f"Using distributed inference API client for {inference_api_urls} with weights {weights or '(No weights supplied)'}"
    )
    return DistributedInferenceAPIClient([f"{inference_api_url}/api/inference" for inference_api_url in inference_api_urls], weights=weights)

class MissingModelException(Exception):
    """Exception raised when a model or group is not found."""
    pass

def get_model_metadata(group_or_setter_name: str, inference_id: str | None = None) -> job_types.ModelMetadata:
    """
    Get the model metadata for a given group and inference_id or full setter name.
    """
    if inference_id is None:
        setter_name = group_or_setter_name
        group_name, inference_id = setter_name.split("/", 1)
    else:
        group_name = group_or_setter_name
        setter_name = f"{group_name}/{inference_id}"

    groups_metadata = get_inference_api_client().get_metadata()

    if group_name not in groups_metadata:
        raise MissingModelException(f"Group does not exist: {group_name}")
    
    if inference_id not in groups_metadata[group_name]["inference_ids"]:
        raise MissingModelException(f"Inference ID does not exist: {group_name}/{inference_id}")

    group_metadata = groups_metadata[group_name]["group_metadata"]
    model_metadata = {
        **group_metadata,
        **groups_metadata[group_name]["inference_ids"][inference_id],
    }
    return job_types.ModelMetadata(
        group=group_name,
        inference_id=inference_id,
        setter_name=setter_name,
        input_handler=model_metadata["input_spec"]["handler"],
        input_handler_opts=model_metadata["input_spec"].get("opts", {}),
        target_entities=model_metadata.get("target_entities", ["items"]),
        output_type=model_metadata.get("output_type", "text"),
        default_batch_size=model_metadata.get("default_batch_size", 64),
        default_threshold=model_metadata.get("default_threshold"),
        input_mime_types=model_metadata.get("input_mime_types", []),
        name=model_metadata.get("name"),
        description=model_metadata.get("description"),
        link=model_metadata.get("link"),
        input_query=build_input_query(setter_name, model_metadata),
        raw_metadata=model_metadata,
        raw_group_metadata=group_metadata,
    )

def model_exists(group_or_setter_name: str, inference_id: str | None = None) -> bool:
    """
    Check if a model exists for a given group and inference_id or full setter name.
    """
    try:
        get_model_metadata(group_or_setter_name, inference_id)
        return True
    except MissingModelException:
        return False

def build_input_query(setter_name: str, raw_metadata: dict) -> "AndOperator":
    """
    Build the input query from a model's raw metadata.
    """
    from panoptikon.db.pql.filters.kvfilters import (
        Match,
        MatchOps,
        MatchValues,
    )
    from panoptikon.db.pql.filters.processed_by import ProcessedBy
    from panoptikon.db.pql.pql_model import AndOperator, NotOperator

    item_filter = AndOperator(and_=[])
    mime_types = raw_metadata.get("input_mime_types", [])
    if mime_types:
        item_filter.and_.append(
            Match(
                match=MatchOps(
                    startswith=MatchValues(
                        type=mime_types,
                    )
                )
            )
        )
    if raw_metadata.get("skip_processed_items", True) == False:
        item_filter.and_.append(
            NotOperator(not_=ProcessedBy(processed_by=setter_name))
        )

    return item_filter

def run_model_extractor(
    conn: sqlite3.Connection,
    config: "SystemConfig",
    model: job_types.ModelMetadata,
    batch_size: int | None = None,
    threshold: float | None = None,
) -> Generator[
    job_types.ExtractionJobProgress
    | job_types.ExtractionJobReport
    | job_types.ExtractionJobStart,
    Any,
    None,
]:
    """
    Run the model extractor for a given model.
    """
    from panoptikon.data_extractors.dynamic_job import run_dynamic_extraction_job

    return run_dynamic_extraction_job(
        conn,
        config,
        model,
        batch_size=batch_size or model.default_batch_size,
        threshold=threshold or model.default_threshold,
    )

def load_model(setter_name: str, cache_key: str, lru_size: int, ttl_seconds: int):
    """ Ask the inference API to load a model into memory """
    get_inference_api_client().load_model(
        setter_name, cache_key, lru_size, ttl_seconds
    )

def unload_model(setter_name: str, cache_key: str):
    """
    Signal to the inference API that the model is no longer needed.
    This will unload the model from the inference API and free up resources.
    """
    get_inference_api_client().unload_model(setter_name, cache_key)

def run_batch_inference(
    setter_name: str,
    cache_key: str,
    lru_size: int,
    ttl_seconds: int,
    inputs: Sequence[Tuple[str | dict | None, bytes | None]],
) -> Any:
    """
    Run batch inference for a given model.
    """
    return get_inference_api_client().predict(
        setter_name, cache_key, lru_size, ttl_seconds, inputs
    )

def delete_extracted_data(conn: sqlite3.Connection, setter_name: str):
    """
    Delete all data generated by a given model.
    This includes the data extracted from items and any orphaned tags resulting
    from the deletion of the extracted data.
    """
    data_types: List[OutputDataType] = get_setter_data_types(conn, setter_name)
    delete_setter_by_name(conn, setter_name)
    msg =  f"Deleted data extracted from items by model {setter_name}.\n"
    if "tags" in data_types:
        orphans_deleted = delete_orphan_tags(conn)
        msg += f"\nDeleted {orphans_deleted} orphaned tags.\n"
    return msg