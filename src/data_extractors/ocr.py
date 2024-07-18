import sqlite3
from typing import List, Sequence

import numpy as np
import torch
from chromadb.api import ClientAPI
from doctr.models import ocr_predictor

from src.data_extractors.data_loaders.images import item_image_extractor_np
from src.data_extractors.extractor_job import run_extractor_job
from src.data_extractors.models import OCRModel
from src.data_extractors.text_embeddings import add_item_text
from src.types import ItemWithPath


def run_ocr_extractor_job(
    conn: sqlite3.Connection, cdb: ClientAPI, model_opt: OCRModel
):
    """
    Run a job that processes items in the database using the given batch inference function and item extractor.
    """

    doctr_model = ocr_predictor(
        det_arch=model_opt.detection_model(),
        reco_arch=model_opt.recognition_model(),
        pretrained=True,
    )
    if torch.cuda.is_available():
        doctr_model = doctr_model.cuda().half()

    def process_batch(batch: Sequence[np.ndarray]) -> List[str]:
        result = doctr_model(batch)
        files_texts: List[str] = []
        for page in result.pages:
            file_text = ""
            for block in page.blocks:
                for line in block.lines:
                    for word in line.words:
                        file_text += word.value + " "
                    file_text += "\n"
                file_text += "\n"
            files_texts.append(file_text)
        return files_texts

    def handle_item_result(
        item: ItemWithPath, inputs: Sequence[np.ndarray], outputs: Sequence[str]
    ):
        merged_text = "\n".join(list(set(outputs)))
        add_item_text(
            cdb=cdb,
            item=item,
            model=model_opt,
            language="en",
            text=merged_text,
        )

    return run_extractor_job(
        conn,
        model_opt.setter_id(),
        model_opt.batch_size(),
        item_image_extractor_np,
        process_batch,
        handle_item_result,
    )
