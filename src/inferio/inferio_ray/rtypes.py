from pydantic.dataclasses import dataclass

@dataclass
class DeploymentConfig:
    """Configuration for the deployment."""
    model_inference_id: str
    max_replicas: int = 1
    batch_wait_timeout_s: float = 0.1
    max_batch_size: int = 64
    num_gpus: int = 1