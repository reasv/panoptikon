import logging
from typing import Tuple

import gradio as gr
import numpy as np

from panoptikon.data_extractors.data_loaders.audio import (
    SAMPLE_RATE,
    load_audio,
)
from panoptikon.data_extractors.models import WhisperSTTModel

logger = logging.getLogger(__name__)


def transcribe_audio(
    model_repo: str | None,
    language: str | None,
    batch_size: int,
    audio_tuple: Tuple[int, np.ndarray] | None,
    audio_file: str | None,
) -> Tuple[str, Tuple[int, np.ndarray] | None]:
    if language == "None":
        language = None

    if model_repo is None:
        return "[No model selected]", None
    logger.info(
        f"""
        Transcribing audio with model: {model_repo} \
        and language: {language or 'Unknown'} \
        """
    )

    import faster_whisper
    import torch

    sample_rate, audio = (
        audio_tuple if audio_tuple is not None else (None, None)
    )

    if audio:
        logger.info(f"Sample rate: {sample_rate}")

    if audio is None and audio_file is not None:
        audio = load_audio(audio_file)

    if not audio:
        return "[No audio provided]", None

    device = "cpu"
    if torch.cuda.is_available():
        device = "cuda"

    whisper_model = faster_whisper.WhisperModel(
        model_repo, device=device, compute_type="float16"
    )
    if batch_size > 1:
        whisper_model = faster_whisper.BatchedInferencePipeline(
            model=whisper_model, batch_size=batch_size
        )
    segments, info = whisper_model.transcribe(
        audio[0],
        language=language,
    )
    logger.debug(info)
    logger.debug(segments)
    merged_text = "\n".join([segment.text for segment in segments])
    return merged_text, (SAMPLE_RATE, audio[0])


def create_whisper_ui():
    with gr.Row():
        with gr.Column():
            with gr.Row():
                gr.Markdown(
                    f"""
                        # Whisper: Transcribe Audio
                        Transcribe long-form microphone or audio inputs with the click of a button! Demo uses the OpenAI Whisper
                    """
                )
            with gr.Row():
                with gr.Tabs():
                    with gr.Tab(label="Upload File"):
                        audio_file = gr.File(
                            label="Upload audio file",
                            type="filepath",
                        )
                    with gr.Tab(label="Microphone"):
                        audio = gr.Audio(
                            sources=["upload", "microphone"],
                            type="numpy",
                            label="Audio",
                        )
            with gr.Row():
                model_repo = gr.Dropdown(
                    label="Select Model",
                    choices=[
                        (name, repo)
                        for name, repo in WhisperSTTModel._available_models_mapping().items()
                    ],
                )
                language_selection = gr.Dropdown(
                    label="Select Language",
                    choices=[
                        ("English", "en"),
                        ("Spanish", "es"),
                        ("French", "fr"),
                        ("Japanese", "ja"),
                        ("German", "de"),
                        ("Chinese", "zh"),
                        ("Unknown", "None"),
                    ],
                )
                batch_size = gr.Number(
                    label="Batch Size",
                    value=WhisperSTTModel.default_batch_size(),
                    minimum=1,
                    maximum=128,
                    step=1,
                )
                run_whisper = gr.Button("Transcribe")
        with gr.Column():
            with gr.Row():
                gr.Markdown(
                    """
                    ## Transcription
                    """
                )
            with gr.Row():
                transcribed_audio = gr.Audio(
                    interactive=False,
                    label="Transcribed Audio",
                    type="numpy",
                )
            with gr.Row():
                output = gr.Textbox(
                    label="Transcription",
                    lines=10,
                )
    run_whisper.click(
        fn=transcribe_audio,
        inputs=[model_repo, language_selection, batch_size, audio, audio_file],
        outputs=[output, transcribed_audio],
    )
