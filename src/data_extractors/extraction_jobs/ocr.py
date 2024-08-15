import sqlite3
from typing import List, Sequence, Tuple

import numpy as np
import torch
from doctr.models import ocr_predictor

from src.data_extractors.data_loaders.images import item_image_loader_numpy
from src.data_extractors.extraction_jobs import run_extraction_job
from src.data_extractors.models import OCRModel
from src.db.extracted_text import insert_extracted_text
from src.types import ItemWithPath


def run_ocr_extractor_job(conn: sqlite3.Connection, model_opt: OCRModel):
    """
    Run a job that processes items in the database using the given batch inference function and item extractor.
    """

    doctr_model = ocr_predictor(
        det_arch=model_opt.detection_model(),
        reco_arch=model_opt.recognition_model(),
        detect_language=True,
        pretrained=True,
    )
    if torch.cuda.is_available():
        doctr_model = doctr_model.cuda().half()

    threshold = model_opt.get_group_threshold(conn)

    def load_images(item: ItemWithPath):
        return item_image_loader_numpy(conn, item)

    def process_batch(
        batch: Sequence[np.ndarray],
    ) -> List[Tuple[str, dict[str, str | float | None], List[float]]]:
        result = doctr_model(batch)
        files_texts: List[str] = []
        languages: List[dict[str, str | float | None]] = []
        word_confidences: List[List[float]] = []
        for page in result.pages:
            file_text = ""
            languages.append(page.language)
            page_word_confidences = []
            for block in page.blocks:
                for line in block.lines:
                    for word in line.words:
                        if threshold and word.confidence < threshold:
                            continue
                        file_text += word.value + " "
                        page_word_confidences.append(word.confidence)
                    file_text += "\n"
                file_text += "\n"
            files_texts.append(file_text)
            word_confidences.append(page_word_confidences)

        assert isinstance(files_texts, list), "files_texts should be a list."
        assert all(
            isinstance(text, str) for text in files_texts
        ), "All elements in files_texts should be strings."

        return list(zip(files_texts, languages, word_confidences))

    def handle_item_result(
        log_id: int,
        item: ItemWithPath,
        _: Sequence[np.ndarray],
        outputs: Sequence[
            Tuple[str, dict[str, str | float | None], List[float]]
        ],
    ):
        # Deduplicate the text from the OCR output
        string_set = set()
        index = 0
        for extracted_string, language, word_confidences in outputs:
            cleaned_string = extracted_string.lower().strip()
            if len(cleaned_string) < 3:
                continue
            if cleaned_string in string_set:
                continue
            string_set.add(cleaned_string)
            avg_confidence = sum(word_confidences) / len(word_confidences)
            assert (
                isinstance(language["confidence"], float)
                or language["confidence"] is None
            ), "Language confidence should be a float or None"

            assert (
                isinstance(language["value"], str) or language["value"] is None
            ), "Language value should be a string or None"

            insert_extracted_text(
                conn,
                item.sha256,
                index=index,
                log_id=log_id,
                text=cleaned_string,
                language=language["value"],
                language_confidence=language["confidence"],
                confidence=avg_confidence,
            )
            index += 1

    return run_extraction_job(
        conn,
        model_opt,
        load_images,
        process_batch,
        handle_item_result,
    )
