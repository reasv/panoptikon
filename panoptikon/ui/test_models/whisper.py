import logging
from typing import Tuple

import gradio as gr
import numpy as np

# from src.data_extractors.data_loaders.audio import SAMPLE_RATE, load_audio
from panoptikon.data_extractors.models import WhisperSTTModel

logger = logging.getLogger(__name__)


def transcribe_audio(
    model_repo: str | None,
    language: str | None,
    batch_size: int,
    audio_tuple: Tuple[int, np.ndarray] | None,
    audio_file: str | None,
) -> Tuple[str, Tuple[int, np.ndarray] | None]:
    if model_repo is None:
        return "[No model selected]", None
    logger.info(
        f"""
        Transcribing audio with model: {model_repo} \
        and language: {language}
        """
    )

    import torch
    import whisperx

    sample_rate, audio = (
        audio_tuple if audio_tuple is not None else (None, None)
    )

    if audio:
        logger.info(f"Sample rate: {sample_rate}")

    if audio is None and audio_file is not None:
        audio = whisperx.load_audio(audio_file)

    if audio is None:
        return "[No audio provided]", None

    device = "cpu"
    if torch.cuda.is_available():
        device = "cuda"

    whisper_model = whisperx.load_model(
        model_repo,
        device=device,
        language=language,
    )

    result = whisper_model.transcribe(
        audio,
        batch_size=batch_size,
        language=language,
    )
    logger.info(result)
    merged_text = "\n".join([segment["text"] for segment in result["segments"]])
    return merged_text, (whisperx.audio.SAMPLE_RATE, audio)


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
