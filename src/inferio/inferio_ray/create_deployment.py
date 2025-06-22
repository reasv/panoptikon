import os
from typing import List
import logging

from ray import serve
from ray.serve.handle import DeploymentHandle

from inferio.cudnnsetup import add_cudnn_to_path
from inferio.inferio_ray.rtypes import DeploymentConfig
from inferio.inferio_types import PredictionInput
from inferio.model import InferenceModel

def build_inference_deployment(
        model_inference_id: str,
        deployment_config: DeploymentConfig,
    ) -> DeploymentHandle:
    clean_id = model_inference_id.replace("/", "_")
    @serve.deployment(
        name=f"{clean_id}_deployment",
        ray_actor_options={
            "num_cpus": deployment_config.num_cpus,
            "num_gpus": deployment_config.num_gpus,
        },
        autoscaling_config={
            "min_replicas":  deployment_config.min_replicas,
            "max_replicas": deployment_config.max_replicas,
            "initial_replicas": deployment_config.initial_replicas,
            "target_ongoing_requests": deployment_config.target_ongoing_requests,
            "upscale_delay_s": deployment_config.upscale_delay_s,
            "downscale_delay_s": deployment_config.downscale_delay_s,
        }
    )
    class InferenceDeployment:
        logger: logging.Logger
        model: InferenceModel
        def __init__(self, inference_id: str):
            """Initialize the inference deployment."""
            import logging
            import asyncio
            from dotenv import load_dotenv
            from inferio.utils import get_impl_classes
            from inferio.config import get_model_config, load_config
            from panoptikon.log import setup_logging
            load_dotenv()
            setup_logging()
            self._load_lock = asyncio.Lock()
            self.logger = logging.getLogger(f"deployments.{inference_id}")
            if os.getenv("NO_CUDNN", "false").lower() not in ("1", "true"):
                self.logger.info("Setting up cuDNN")
                add_cudnn_to_path()
            else:
                self.logger.info("Skipping cuDNN setup as per NO_CUDNN environment variable")
            global_config, _ = load_config()
            model_config = get_model_config(inference_id, global_config)
            impl_class_name = model_config.pop("impl_class", None)
            # Remove all the external config keys
            model_config = {k: v for k, v in model_config.items() if k not in ["impl_class", "ray_config"]}
            self.logger.info("Initializing deployment")
            impl_classes = get_impl_classes(self.logger)
            for cls in impl_classes:
                if cls.name() == impl_class_name:
                    self.model = cls(**model_config)
                    break
            else:
                raise ValueError(f"Model class {impl_class_name} not found in impl_classes")
            self.logger.info(f"init in PID {os.getpid()} with impl_class {impl_class_name}")
        
        def _process_batch(self, inputs: List[List[PredictionInput]]) -> List[List[bytes | dict | list | str]]:
            # Flatten the batch of inputs into a single list for the model
            batch_sizes = [len(batch) for batch in inputs]
            flattened_inputs = [item for batch in inputs for item in batch]

            # Get predictions for the flattened list
            predictions = list(self.model.predict(flattened_inputs))

            # Unflatten the predictions to match the original batch structure
            output = []
            start = 0
            for size in batch_sizes:
                output.append(predictions[start:start + size])
                start += size
            
            return output

        @serve.batch(max_batch_size=deployment_config.max_batch_size, batch_wait_timeout_s=deployment_config.batch_wait_timeout_s)
        async def __call__(self, inputs: List[List[PredictionInput]]) -> List[List[bytes | dict | list | str]]:
            self.logger.debug(f"Received {len(inputs)} batch inputs")
            return self._process_batch(inputs)

        @serve.batch(max_batch_size=deployment_config.max_batch_size, batch_wait_timeout_s=deployment_config.batch_wait_timeout_s)
        async def predict(self, inputs: List[List[PredictionInput]]) -> List[List[bytes | dict | list | str]]:
            self.logger.debug(f"Received {len(inputs)} inputs for prediction")
            return self._process_batch(inputs)
        
        async def load(self) -> None:
            """Load the model."""
            async with self._load_lock:
                self.logger.info(f"Loading model")
                self.model.load()

        async def keepalive(self) -> None:
            """Keep the model alive."""
            self.logger.debug(f"Keeping model alive")
        
    app = InferenceDeployment.bind(model_inference_id)
    handle = serve.run(app, name=f"{clean_id}_app", blocking=False, route_prefix=None)

    return handle