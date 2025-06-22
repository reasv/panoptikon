import os
from typing import Any, Dict
from inferio.inferio_ray.create_deployment import DeploymentConfig
from inferio.config import get_model_config
from inferio.impl.utils import get_device

def get_deployment_config(model_inference_id: str, global_config: Dict[str, Any]) -> DeploymentConfig:
    """Get the deployment configuration from environment variables."""

    config = get_model_config(model_inference_id, global_config)
    impl_class_name = config.pop("impl_class", None)
    if impl_class_name is None:
        raise ValueError(f"Model class name not found in config for inference_id: {model_inference_id}")
    devices = get_device()
    max_replicas = config.pop(
        "max_replicas",
        len(devices)
    )
    batch_wait_timeout_s = config.pop(
        "batch_wait_timeout_s", 
        float(os.getenv("BATCH_WAIT_TIMEOUT_S", "0.1"))
    )
    max_batch_size = config.pop(
        "max_batch_size",
        int(os.getenv("MAX_BATCH_SIZE", "64"))
    )
    return DeploymentConfig(
        model_inference_id=model_inference_id,
        max_replicas=max_replicas,
        batch_wait_timeout_s=batch_wait_timeout_s,
        max_batch_size=max_batch_size
    )
