import os
import logging
from typing import Any, Dict
from inferio.inferio_ray.create_deployment import DeploymentConfig
from inferio.config import get_model_config
from inferio.impl.utils import get_device

logger = logging.getLogger(__name__)

def get_deployment_config(model_inference_id: str, global_config: Dict[str, Any]) -> DeploymentConfig:
    """Get the deployment configuration from environment variables."""

    config = get_model_config(model_inference_id, global_config)
    impl_class_name = config.pop("impl_class", None)
    if impl_class_name is None:
        raise ValueError(f"Model class name not found in config for inference_id: {model_inference_id}")
    devices = get_device()
    ray_config = config.pop("ray_config", {})
    max_replicas = ray_config.pop(
        "max_replicas",
        len(devices)
    )
    batch_wait_timeout_s = ray_config.pop(
        "batch_wait_timeout_s", 
        float(os.getenv("RAY_BATCH_WAIT_TIMEOUT_S", "0.1"))
    )
    max_batch_size = ray_config.pop(
        "max_batch_size",
        int(os.getenv("RAY_MAX_BATCH_SIZE", "64"))
    )
    num_gpus = ray_config.pop(
        "num_gpus",
        int(os.getenv("RAY_MODEL_NUM_GPUS", "1"))
    )
    num_cpus = ray_config.pop(
        "num_cpus",
        float(os.getenv("RAY_MODEL_NUM_CPUS", "0.1"))
    )
    target_ongoing_requests = ray_config.pop(
        "target_ongoing_requests",
        int(os.getenv("RAY_TARGET_ONGOING_REQUESTS", "2"))
    )
    upscale_delay_s = ray_config.pop(
        "upscale_delay_s",
        int(os.getenv("RAY_UPSCALE_DELAY_S", "5"))
    )
    downscale_delay_s = ray_config.pop(
        "downscale_delay_s",
        int(os.getenv("RAY_DOWNSCALE_DELAY_S", "30"))
    )
    if downscale_delay_s < 10:
        downscale_delay_s = 10
        logger.warning(
            "Downscale delay cannot be less than 10 seconds. Overriding to 10 seconds."
        )
    initial_replicas = ray_config.pop(
        "initial_replicas",
        int(os.getenv("RAY_INITIAL_REPLICAS", "1"))
    )

    min_replicas = ray_config.pop(
        "min_replicas",
        int(os.getenv("RAY_MIN_REPLICAS", "0"))
    )

    return DeploymentConfig(
        model_inference_id=model_inference_id,
        max_replicas=max_replicas,
        batch_wait_timeout_s=batch_wait_timeout_s,
        max_batch_size=max_batch_size,
        num_gpus=num_gpus,
        target_ongoing_requests=target_ongoing_requests,
        upscale_delay_s=upscale_delay_s,
        downscale_delay_s=downscale_delay_s,
        initial_replicas=initial_replicas,
        min_replicas=min_replicas,
        num_cpus=num_cpus
    )
