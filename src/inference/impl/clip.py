import multiprocessing as mp
from io import BytesIO
from multiprocessing.queues import Queue
from typing import List, Optional, Sequence, Tuple, Union

import numpy as np
import open_clip
import torch
from PIL import Image as PILImage

from src.inference.impl.utils import clear_cache, get_device
from src.inference.model import InferenceModel
from src.inference.types import PredictionInput


def worker_process(
    model_name: str,
    pretrained: Optional[str],
    init_args: dict,
    device_id: int,
    task_queue: Queue,
    result_queue: Queue,
) -> None:
    torch.cuda.set_device(device_id)
    model, _, preprocess = open_clip.create_model_and_transforms(
        model_name=model_name,
        pretrained=pretrained,
        **init_args,
    )
    model.eval().to(f"cuda:{device_id}")

    assert not isinstance(preprocess, tuple), "Multiple preprocess functions"

    while True:
        task: Optional[
            Tuple[str, Union[torch.Tensor, List[PILImage.Image]]]
        ] = task_queue.get()
        if task is None:  # Poison pill to stop the process
            break

        mode, batch = task
        with torch.inference_mode():
            if mode == "text":
                tokens: torch.Tensor = torch.tensor(batch).to(
                    f"cuda:{device_id}"
                )
                features: torch.Tensor = model.encode_text(tokens)
            else:  # image
                images: torch.Tensor = torch.stack(
                    [preprocess(img).to(f"cuda:{device_id}") for img in batch]  # type: ignore
                )
                features: torch.Tensor = model.encode_image(images)

            features /= features.norm(dim=-1, keepdim=True)
            result_queue.put(features.cpu().numpy())

    del model
    torch.cuda.empty_cache()


class ClipModel(InferenceModel):
    def __init__(
        self,
        model_name: str,
        pretrained: Optional[str] = None,
        context_length: Optional[int] = None,
        **kwargs: dict,
    ) -> None:
        self.model_name: str = model_name
        self.pretrained: Optional[str] = pretrained
        self.context_length: Optional[int] = context_length
        self.init_args: dict = kwargs
        self._model_loaded: bool = False
        self.processes: List[mp.Process] = []
        self.task_queues: List[Queue] = []
        self.result_queues: List[Queue] = []
        self.devices

    def load(self) -> None:
        if self._model_loaded:
            return

        self.devices = get_device()

        num_gpus: int = len(self.devices)

        for i in range(num_gpus):
            task_queue: Queue = mp.Queue()
            result_queue: Queue = mp.Queue()
            process: mp.Process = mp.Process(
                target=worker_process,
                args=(
                    self.model_name,
                    self.pretrained,
                    self.init_args,
                    i,
                    task_queue,
                    result_queue,
                ),
            )
            process.start()
            self.processes.append(process)
            self.task_queues.append(task_queue)
            self.result_queues.append(result_queue)

        self.tokenizer = open_clip.get_tokenizer(
            model_name=self.model_name, context_length=self.context_length
        )
        self._model_loaded = True

    def predict(
        self, inputs: Sequence[PredictionInput]
    ) -> Sequence[Union[bytes, dict, list, str]]:
        self.load()

        text_inputs: List[Tuple[int, str]] = []
        image_inputs: List[Tuple[int, PILImage.Image]] = []
        results: List[Optional[bytes]] = [None] * len(inputs)

        for idx, input_item in enumerate(inputs):
            if isinstance(input_item.data, str):
                text_inputs.append((idx, input_item.data))
            elif input_item.file:
                image: PILImage.Image = PILImage.open(
                    BytesIO(input_item.file)
                ).convert("RGB")
                image_inputs.append((idx, image))

        num_gpus: int = len(self.devices)

        if text_inputs:
            indices, texts = zip(*text_inputs)
            tokens: torch.Tensor = self.tokenizer(list(texts))
            text_batches: List[np.ndarray] = np.array_split(tokens, num_gpus)

            for i, batch in enumerate(text_batches):
                if batch.size > 0:
                    self.task_queues[i].put(("text", batch))

            text_features: List[np.ndarray] = []
            for i in range(num_gpus):
                if text_batches[i].size > 0:
                    text_features.extend(self.result_queues[i].get())

            for idx, feature in zip(indices, text_features):
                results[idx] = feature.tobytes()

        if image_inputs:
            indices, images = zip(*image_inputs)
            image_batches: Sequence[Sequence[PILImage.Image]] = [
                list(batch) for batch in np.array_split(images, num_gpus)
            ]

            for i, batch in enumerate(image_batches):
                if len(batch) > 0:
                    self.task_queues[i].put(("image", batch))

            image_features: List[np.ndarray] = []
            for i in range(num_gpus):
                if len(image_batches[i]) > 0:
                    image_features.extend(self.result_queues[i].get())

            for idx, feature in zip(indices, image_features):
                results[idx] = feature.tobytes()

        output: List[bytes] = [res for res in results if res is not None]
        assert len(output) == len(
            inputs
        ), "Mismatched output length and input length"
        return output

    def unload(self) -> None:
        if self._model_loaded:
            for queue in self.task_queues:
                queue.put(None)  # Send poison pill to stop the process
            for process in self.processes:
                process.join()
            self.processes = []
            self.task_queues = []
            self.result_queues = []
            clear_cache()
            self._model_loaded = False

    def __del__(self) -> None:
        self.unload()
