import logging
from typing import Dict, List, Sequence

from inferio.impl.utils import clear_cache, get_device, serialize_array
from inferio.model import InferenceModel
from inferio.types import PredictionInput

logger = logging.getLogger(__name__)


class SentenceTransformersModel(InferenceModel):
    def __init__(
        self,
        model_name: str,
        query_prompt_name_map: dict = {},
        combine_threshold: int = -1,
        init_args: dict = {},
        inf_args: dict = {},
    ):
        self.model_name: str = model_name
        self.init_args = init_args
        self.inf_args = inf_args
        self.query_prompt_name = query_prompt_name_map
        self.combine_threshold = combine_threshold
        self._model_loaded: bool = False

    @classmethod
    def name(cls) -> str:
        return "sentence_transformers"

    def load(self) -> None:
        from sentence_transformers import SentenceTransformer

        if self._model_loaded:
            return

        self.devices = get_device()
        self.model = SentenceTransformer(
            model_name_or_path=self.model_name,
            **self.init_args,
        )
        self.pool = None
        # if len(self.devices) > 1:
        #     self.pool = self.model.start_multi_process_pool()
        self._model_loaded = True

    def predict(self, inputs: Sequence[PredictionInput]) -> List[bytes]:
        import numpy as np

        # Ensure the model is loaded
        self.load()
        input_strings: List[str] = []
        for inp in inputs:
            assert isinstance(
                inp.data, dict
            ), f"Input must be dict, got {inp.data}"
            assert (
                "text" in inp.data
            ), f"Input dict must have 'text' key, got {inp.data}"
            assert isinstance(
                inp.data["text"], str
            ), f"Input 'text' must be string, got {inp.data['text']}"
            input_strings.append(inp.data["text"])

        batch_config = inputs[0].data
        assert isinstance(batch_config, dict), "Batch config must be dict"

        batch_args = batch_config.get("args", {})
        assert isinstance(batch_args, dict), "Batch args must be dict"

        if batch_config.get("task") in self.query_prompt_name:
            task = batch_config.get("task")
            batch_args["prompt_name"] = self.query_prompt_name[task]

        # Retrieve tokenizer and max sequence length from the model

        tokenizer = self.model.tokenizer
        max_seq_length = self.model.max_seq_length

        final_embeddings = []

        all_chunks = []
        chunk_map = (
            []
        )  # Keeps track of which original text each chunk belongs to

        # Split texts that exceed the max_seq_length into chunks
        for idx, text in enumerate(input_strings):
            tokens = tokenizer.encode(text, truncation=False)
            if len(tokens) <= max_seq_length:
                all_chunks.append(text)
                chunk_map.append(idx)
            else:
                chunks = split_text_by_tokens(text, tokenizer, max_seq_length)
                all_chunks.extend(chunks)
                chunk_map.extend([idx] * len(chunks))

        # Batch encode the chunks
        if self.pool:
            embeddings = self.model.encode_multi_process(
                all_chunks,
                normalize_embeddings=False,
                pool=self.pool,
                batch_size=len(input_strings),
                **self.inf_args,
                **batch_args,
            )
        else:
            embeddings = self.model.encode(
                all_chunks,
                normalize_embeddings=False,
                batch_size=len(input_strings),
                **self.inf_args,
                **batch_args,
            )

        # Initialize a list of lists to collect embeddings for each input text
        aggregated_embeddings = [[] for _ in input_strings]

        # Aggregate embeddings back to their corresponding original input
        for embedding, original_idx in zip(embeddings, chunk_map):
            aggregated_embeddings[original_idx].append(embedding)

        # Wrap embeddings for each input text into 2D arrays
        for idx, emb_list in enumerate(aggregated_embeddings):
            input_config = inputs[idx].data
            assert isinstance(input_config, dict), "Input config must be dict"
            combine_at = input_config.get(
                "combine_threshold", self.combine_threshold
            )

            # If the text was split into more than combine_at chunks, combine the embeddings
            if len(emb_list) >= combine_at and combine_at != -1:
                combined_embedding = np.mean(emb_list, axis=0)
                # The extra combined embedding encodes the average meaning of the text
                emb_list.append(combined_embedding)

            # Ensure the embedding is wrapped as a two-dimensional array
            final_embeddings.append(serialize_array(np.array(emb_list)))

        assert len(final_embeddings) == len(
            input_strings
        ), "Mismatch in input and output sizes"
        return final_embeddings

    def unload(self) -> None:
        if self._model_loaded:
            if self.pool:
                self.model.stop_multi_process_pool(self.pool)
            del self.model
            del self.pool
            clear_cache()
            self._model_loaded = False

    def __del__(self):
        self.unload()


def split_text_by_tokens(text, tokenizer, max_tokens):
    # Tokenize the entire text
    tokens = tokenizer.encode(text, truncation=False)

    # Split tokens into chunks of max_tokens size
    chunks = [
        tokens[i : i + max_tokens] for i in range(0, len(tokens), max_tokens)
    ]

    # Define the minimum chunk size threshold (e.g., one-third of max_tokens)
    min_chunk_size = max_tokens // 3

    # If the last chunk is smaller than the minimum threshold, rebalance it
    if len(chunks) > 1 and len(chunks[-1]) < min_chunk_size:
        # Calculate how many tokens are needed to meet the minimum size
        tokens_needed = min_chunk_size - len(chunks[-1])

        # Move tokens from the second-to-last chunk to the last chunk
        chunks[-1] = chunks[-2][-tokens_needed:] + chunks[-1]
        chunks[-2] = chunks[-2][:-tokens_needed]

    # Decode the token chunks back into text
    return [
        tokenizer.decode(chunk, skip_special_tokens=True) for chunk in chunks
    ]
