from pydantic.dataclasses import dataclass

@dataclass
class DeploymentConfig:
    """Configuration for the deployment."""
    model_inference_id: str
    max_replicas: int = 1
    batch_wait_timeout_s: float = 0.1
    max_batch_size: int = 64
    num_gpus: int = 1
    target_ongoing_requests: int = 2
    upscale_delay_s: int = 5
    downscale_delay_s: int = 30
    initial_replicas: int = 1
    min_replicas: int = 0
    num_cpus: float = 0.1