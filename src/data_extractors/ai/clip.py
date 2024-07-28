from typing import List, Sequence

import numpy as np
import open_clip
import torch
from PIL import Image as PILImage


class CLIPModelSingleton:
    _instances = {}
    _reference_counts = {}

    @classmethod
    def get_instance(cls, model_name, pretrained):
        key = (model_name, pretrained)
        if key not in cls._instances:
            print(f"Creating new instance for {model_name} {pretrained}")
            model, _, preprocess = open_clip.create_model_and_transforms(
                model_name, pretrained=pretrained
            )
            tokenizer = open_clip.get_tokenizer(model_name)
            cls._instances[key] = {
                "model": model,
                "preprocess": preprocess,
                "tokenizer": tokenizer,
            }
            cls._reference_counts[key] = 0
        else:
            print(f"Reusing instance for {model_name} {pretrained}")
        cls._reference_counts[key] += 1
        return cls._instances[key]

    @classmethod
    def release_instance(cls, model_name, pretrained):
        key = (model_name, pretrained)
        if key in cls._reference_counts:
            cls._reference_counts[key] -= 1
            if cls._reference_counts[key] == 0:
                print(f"Deleting instance for {model_name} {pretrained}")
                del cls._instances[key]
                del cls._reference_counts[key]
                if torch.cuda.is_available():
                    torch.cuda.empty_cache()
                return True
        return False


class CLIPEmbedder:
    model_name: str
    pretrained: str
    batch_size: int

    def __init__(
        self,
        model_name="ViT-H-14-378-quickgelu",
        pretrained="dfn5b",
        batch_size=8,
        persistent=False,
    ):
        self.model_name = model_name
        self.pretrained = pretrained
        self.batch_size = batch_size
        self.model = None
        self.tokenizer = None
        self.preprocess = None
        self.device = torch.device(
            "cuda" if torch.cuda.is_available() else "cpu"
        )
        self._model_loaded = False
        self.persistent = persistent

    def _load_model(self):
        if not self._model_loaded:
            instance = CLIPModelSingleton.get_instance(
                self.model_name, self.pretrained
            )
            self.model = instance["model"].eval().to(self.device)
            self.preprocess = instance["preprocess"]
            self.tokenizer = instance["tokenizer"]
            self._model_loaded = True

    def load_model(self):
        self._load_model()

    def get_image_embeddings(
        self, images: Sequence[str | PILImage.Image | np.ndarray]
    ):
        self._load_model()
        embeddings: List[np.ndarray] = []

        for i in range(0, len(images), self.batch_size):
            image_batch = images[i : i + self.batch_size]
            # Check if they're all PIL images rather than paths
            if all(isinstance(image, PILImage.Image) for image in image_batch):
                batch_images = [self.preprocess(image).unsqueeze(0) for image in image_batch]  # type: ignore
            # Check if they're ndarrays
            elif all(isinstance(image, np.ndarray) for image in image_batch):
                batch_images = [self.preprocess(PILImage.fromarray(image_array)).unsqueeze(0) for image_array in image_batch]  # type: ignore
            # Otherwise, assume they're all paths
            else:
                batch_images = [self.preprocess(PILImage.open(image)).unsqueeze(0) for image in image_batch]  # type: ignore
            batch_images = torch.cat(batch_images).to(self.device)

            with torch.no_grad(), torch.cuda.amp.autocast():
                image_features = self.model.encode_image(batch_images)  # type: ignore
                image_features /= image_features.norm(dim=-1, keepdim=True)

            embeddings.extend(image_features.cpu().numpy())

        return embeddings

    def get_text_embeddings(self, texts: List[str]):
        self._load_model()
        embeddings = []

        for i in range(0, len(texts), self.batch_size):
            batch_texts = texts[i : i + self.batch_size]
            batch_tokens = self.tokenizer(batch_texts).to(self.device)  # type: ignore

            with torch.no_grad(), torch.cuda.amp.autocast():
                text_features = self.model.encode_text(batch_tokens)  # type: ignore
                text_features /= text_features.norm(dim=-1, keepdim=True)

            embeddings.extend(text_features.cpu().numpy())

        return embeddings

    def unload_model(self):
        if self._model_loaded:
            self.model = None
            self.preprocess = None
            self.tokenizer = None
            CLIPModelSingleton.release_instance(
                self.model_name, self.pretrained
            )
            self._model_loaded = False

    def __del__(self):
        if not self.persistent:
            self.unload_model()

    def rank_images_by_similarity(self, image_embeddings_dict, text_embedding):
        image_hashes = list(image_embeddings_dict.keys())
        image_embeddings = torch.tensor(list(image_embeddings_dict.values()))

        # Normalize text embedding
        text_embedding = torch.tensor(text_embedding).unsqueeze(0)
        text_embedding /= text_embedding.norm(dim=-1, keepdim=True)

        # Compute similarities
        similarities = torch.matmul(
            image_embeddings, text_embedding.T
        ).squeeze()

        # Sort image hashes by similarity
        sorted_indices = torch.argsort(similarities, descending=True)
        sorted_hashes = [image_hashes[i] for i in sorted_indices]

        return sorted_hashes
