import os


def get_threshold_from_env() -> float:
    threshold = os.getenv("SCORE_THRESHOLD")
    if threshold is None:
        return 0.1
    return float(threshold)


def get_timeout_from_env() -> int:
    timeout = os.getenv("TAGSCAN_TIMEOUT")
    if timeout is None:
        return 40
    return int(timeout)


def get_ocr_threshold_from_env() -> float:
    threshold = os.getenv("OCR_WORD_CONFIDENCE_THRESHOLD")
    if threshold is None:
        return 0.41
    return float(threshold)


def get_whisper_avg_logprob_threshold_from_env() -> float | None:
    threshold = os.getenv("WHISPER_AVG_LOGPROB_THRESHOLD")
    if threshold is None:
        return None
    return float(threshold)
