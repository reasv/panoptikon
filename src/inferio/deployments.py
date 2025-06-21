import os
import asyncio
from typing import Any, Dict, List
import ray
from ray import serve
from ray.serve.handle import DeploymentHandle
import logging
from inferio.config import get_model_config, load_config
from inferio.impl.utils import get_device
from inferio.inferio_types import PredictionInput
from inferio.model import InferenceModel

def build_inference_deployment(inference_id: str, global_config: Dict[str, Any]):
    config = get_model_config(inference_id, global_config)
    impl_class_name = config.pop("impl_class", None)
    if impl_class_name is None:
        raise ValueError(f"Model class name not found in config for inference_id: {inference_id}")
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

    @serve.deployment(
        name=f"{inference_id}_deployment",
        ray_actor_options={
            "num_cpus": 0.1,
        },
        autoscaling_config={
            "min_replicas": 0,
            "max_replicas": max_replicas,
            "initial_replicas": 1,
            "target_ongoing_requests": 2,
            "upscale_delay_s": 10,
            "downscale_delay_s": 30
        }
    )
    class InferenceDeployment:
        logger: logging.Logger
        model: InferenceModel
        def __init__(self):
            """Initialize the inference deployment."""
            import logging
            from dotenv import load_dotenv
            from inferio.utils import get_impl_classes
            from panoptikon.log import setup_logging
            load_dotenv()
            setup_logging()
            self.logger = logging.getLogger(f"deployments.{inference_id}")
            impl_classes = get_impl_classes(self.logger)
            for cls in impl_classes:
                if cls.name == impl_class_name:
                    self.model = cls(**config)
                    break
            else:
                raise ValueError(f"Model class {impl_class_name} not found in impl_classes")
            self.logger.info(f"[{inference_id}] init in PID {os.getpid()} with impl_class {impl_class_name}")
        
        @serve.batch(max_batch_size=max_batch_size, batch_wait_timeout_s=batch_wait_timeout_s)
        async def __call__(self, inputs: List[PredictionInput]) -> List[bytes | dict | list | str]:
            self.logger.debug(f"Received {len(inputs)} batch inputs")
            return list(self.model.predict(inputs))

        @serve.batch(max_batch_size=max_batch_size, batch_wait_timeout_s=batch_wait_timeout_s)
        async def predict(self, inputs: List[PredictionInput]) -> List[bytes | dict | list | str]:
            self.logger.debug(f"Received {len(inputs)} inputs for prediction")
            return list(self.model.predict(inputs))
        
        async def load(self) -> None:
            """Load the model."""
            self.logger.info(f"Loading model")
            self.model.load()
        
        async def keepalive(self) -> None:
            """Keep the model alive."""
            self.logger.info(f"Keeping model alive")
            # This can be used to keep the model loaded or perform any periodic tasks.
        
    app = InferenceDeployment.bind()
    handle = serve.run(app, name=f"{inference_id}_app", blocking=False, route_prefix=None)

    return handle

@serve.deployment(
    name="ModelRouter",
    ray_actor_options={
        "num_cpus": 0.1,
    },
    autoscaling_config={
        "min_replicas": 1,
        "initial_replicas": 1,
        "target_ongoing_requests": 2,
        "upscale_delay_s": 10,
        "downscale_delay_s": 10
    }
)
class ModelRouter:
    def __init__(self):
        import logging
        from dotenv import load_dotenv
        from panoptikon.log import setup_logging
        load_dotenv()
        setup_logging()
        self._handles: dict[str, DeploymentHandle] = {}
        self._lock = asyncio.Lock()
        self.logger = logging.getLogger("ModelRouter")
        self._config, self._mtime = load_config()
        self.logger.info(f"ModelRouter initialized")

    async def get_config(self):
        """Reload the configuration if it has changed."""
        self._config, self._mtime = load_config(self._config, self._mtime)
        return self._config

    async def _ensure(self, inference_id: str):
        if inference_id in self._handles:
            return self._handles[inference_id]

        async with self._lock:
            if inference_id in self._handles:
                return self._handles[inference_id]
            self.logger.info(f"Building deployment for {inference_id}")
            handle = build_inference_deployment(inference_id, self._config)
            self._handles[inference_id] = handle
            return handle

    async def __call__(self, inference_id: str, inputs: List[PredictionInput]) -> List[bytes | dict | list | str]:
        h = await self._ensure(inference_id)
        return await h.remote(inputs)
    
    async def load(self, inference_id: str) -> None:
        """Load the model for the given inference ID."""
        h = await self._ensure(inference_id)
        h = h.options(method_name="load")
        await h.remote()

    async def keepalive(self, inference_id: str) -> None:
        """Keep the model alive for the given inference ID."""
        h = await self._ensure(inference_id)
        h = h.options(method_name="keepalive")
        await h.remote()