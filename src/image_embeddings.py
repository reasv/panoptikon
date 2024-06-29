from typing import List, Sequence, Union, cast
import torch
from PIL import Image as PILImage
import open_clip
import numpy as np
from numpy.typing import NDArray
from chromadb.types import Vector
from chromadb.api.types import is_image, is_document, EmbeddingFunction
from chromadb.utils.embedding_functions import OpenCLIPEmbeddingFunction

ImageDType = Union[np.uint, np.int_, np.float_]
Image = NDArray[ImageDType]
Images = List[Image]

Document = str
Documents = List[Document]

Embedding = Vector
Embeddings = List[Embedding]

class CLIPEmbedder(EmbeddingFunction[Union[Documents, Images]]):
    def __init__(
            self,
            model_name='ViT-H-14-378-quickgelu',
            pretrained='dfn5b',
            batch_size=8
        ):
        self.model_name = model_name
        self.pretrained = pretrained
        self.batch_size = batch_size
        self.model = None
        self.tokenizer = None
        self.preprocess = None
        self.device = torch.device(
            "cuda" if torch.cuda.is_available() 
            else "cpu"
        )

    def _load_model(self):
        if self.model is None:
            self.model, _, self.preprocess = open_clip.create_model_and_transforms(
                self.model_name, pretrained=self.pretrained
            )
            self.model.eval().to(self.device)
            self.tokenizer = open_clip.get_tokenizer(self.model_name)
    
    def load_model(self):
        self._load_model()

    def get_image_embeddings(
            self,
            images: Sequence[str | PILImage.Image | np.ndarray]
        ):
        self._load_model()
        embeddings = []

        for i in range(0, len(images), self.batch_size):
            image_batch = images[i:i + self.batch_size]
            # Check if they're all PIL images rather than paths
            if all(isinstance(image, PILImage.Image) for image in image_batch):
                batch_images = [self.preprocess(image).unsqueeze(0) for image in image_batch] # type: ignore
            # Check if they're ndarrays
            elif all(isinstance(image, np.ndarray) for image in image_batch):
                batch_images = [self.preprocess(PILImage.fromarray(image_array)).unsqueeze(0) for image_array in image_batch] # type: ignore
            # Otherwise, assume they're all paths
            else:
                batch_images = [self.preprocess(PILImage.open(image)).unsqueeze(0) for image in image_batch] # type: ignore
            batch_images = torch.cat(batch_images).to(self.device)

            with torch.no_grad(), torch.cuda.amp.autocast():
                image_features = self.model.encode_image(batch_images) # type: ignore
                image_features /= image_features.norm(dim=-1, keepdim=True)
            
            embeddings.extend(image_features.cpu().numpy())

        return embeddings

    def get_text_embeddings(self, texts: List[str]):
        self._load_model()
        embeddings = []

        for i in range(0, len(texts), self.batch_size):
            batch_texts = texts[i:i + self.batch_size]
            batch_tokens = self.tokenizer(batch_texts).to(self.device) # type: ignore

            with torch.no_grad(), torch.cuda.amp.autocast():
                text_features = self.model.encode_text(batch_tokens) # type: ignore
                text_features /= text_features.norm(dim=-1, keepdim=True)

            embeddings.extend(text_features.cpu().numpy())

        return embeddings

    def unload_model(self):
        if self.model:
            del self.model
            self.model = None
            torch.cuda.empty_cache()

    def rank_images_by_similarity(self, image_embeddings_dict, text_embedding):
        image_hashes = list(image_embeddings_dict.keys())
        image_embeddings = torch.tensor(list(image_embeddings_dict.values()))

        # Normalize text embedding
        text_embedding = torch.tensor(text_embedding).unsqueeze(0)
        text_embedding /= text_embedding.norm(dim=-1, keepdim=True)

        # Compute similarities
        similarities = torch.matmul(image_embeddings, text_embedding.T).squeeze()

        # Sort image hashes by similarity
        sorted_indices = torch.argsort(similarities, descending=True)
        sorted_hashes = [image_hashes[i] for i in sorted_indices]

        return sorted_hashes
    
    def __call__(self, input: Union[Documents, Images]) -> Embeddings:
        embeddings: Embeddings = []
        for item in input:
            if is_image(item):
                embeddings.append(self.get_image_embeddings([cast(Image, item)])[0].squeeze().tolist())
            elif is_document(item):
                embeddings.append(self.get_text_embeddings([cast(Document, item)])[0].squeeze().tolist())
        return embeddings