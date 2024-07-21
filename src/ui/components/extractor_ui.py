import datetime
from typing import List, Type

import gradio as gr

import src.data_extractors.models as models
from src.data_extractors.utils import get_chromadb_client
from src.db import get_database_connection, vacuum_database


def shorten_path(path: str, max_length=75) -> str:
    return ("..." + path[-max_length:]) if len(path) > max_length else path


def run_model_job(model_opt: models.ModelOpts, progress_tracker=gr.Progress()):
    print(f"Running job for model {model_opt}")
    conn = get_database_connection()
    cdb = get_chromadb_client()
    cursor = conn.cursor()
    cursor.execute("BEGIN")
    failed, images, videos, other, units = [], 0, 0, 0, 0
    start_time = datetime.datetime.now()
    for progress in model_opt.run_extractor(conn, cdb):
        if type(progress) == models.ExtractorJobProgress:
            # Job is in progress
            progress_tracker(
                (progress.processed_items, progress.total_items),
                desc=(
                    f"ETA: {progress.eta_string} | "
                    + f"Last Item: {shorten_path(progress.item.path)}"
                ),
                unit="files",
            )
        elif type(progress) == models.ExtractorJobReport:
            # Job is complete
            images = progress.images
            videos = progress.videos
            failed = progress.failed_paths
            other = progress.other
            units = progress.units

    end_time = datetime.datetime.now()
    total_time = end_time - start_time
    total_time_pretty = str(total_time).split(".")[0]
    conn.commit()
    failed_str = "\n".join(failed)
    report_str = f"""
    Extraction completed for model {model_opt} in {total_time_pretty}.
    Successfully processed {images} images and {videos} videos, and {other} other file types.
    The model processed a total of {units} individual pieces of data.
    {len(failed)} files failed to process due to errors.
    """
    if len(failed) > 0:
        report_str += f"\nFailed files:\n{failed_str}"
    conn.close()
    return report_str


def delete_model_data(model_opt: models.ModelOpts):
    print(f"Running data deletion job for model {model_opt}")
    conn = get_database_connection()
    cdb = get_chromadb_client()
    cursor = conn.cursor()
    cursor.execute("BEGIN")
    report_str = model_opt.delete_extracted_data(conn, cdb)
    conn.commit()
    vacuum_database(conn)
    conn.close()
    return report_str


def extraction_job_UI(
    model_type: Type[models.ModelOpts],
):
    def run_job(batch: int, chosen_model: List[str]):
        report_string = ""
        for model_name in chosen_model:
            extractor_model = model_type(
                batch_size=batch, model_name=model_name
            )
            report_string += run_model_job(extractor_model)
        return report_string

    def delete_data(chosen_model: List[str]):
        report_string = ""
        for model_name in chosen_model:
            extractor_model = model_type(model_name=model_name)
            report_string += delete_model_data(extractor_model)
        return report_string

    with gr.TabItem(label=model_type.name()) as extractor_tab:
        gr.Markdown(
            f"""
            ## {model_type.name()} Extraction Job
            ### {model_type.description()}

            This will run the {model_type.name()} extractor on the database.
            The extractor will process all items in the database that have not been processed by the selected model yet.
            Data will be extracted from the items and indexed in the database for search and retrieval.
            """
        )

        with gr.Group():
            with gr.Row():
                model_choice = gr.Dropdown(
                    label="Model(s) to Use",
                    multiselect=True,
                    value=[
                        model_type.default_model(),
                    ],
                    choices=[
                        (name, name) for name in model_type.available_models()
                    ],
                )
                batch_size = gr.Slider(
                    label="Batch Size",
                    minimum=1,
                    maximum=128,
                    value=model_type.default_batch_size(),
                )
        with gr.Row():
            run_button = gr.Button("Run Batch Job")
            delete_button = gr.Button(
                "Delete All Data Extracted by Selected Model(s)"
            )
        with gr.Row():
            report_box = gr.Textbox(
                label="Job Report", value="", lines=5, interactive=False
            )

    run_button.click(
        fn=run_job,
        inputs=[batch_size, model_choice],
        outputs=[report_box],
    )
    delete_button.click(
        fn=delete_data,
        inputs=[model_choice],
        outputs=[report_box],
    )


def create_data_extraction_UI():
    with gr.Row():
        with gr.Tabs():
            extraction_job_UI(models.TagsModel)
            extraction_job_UI(models.OCRModel)
            extraction_job_UI(models.WhisperSTTModel)
            extraction_job_UI(models.ImageEmbeddingModel)
