from typing import List, Type

import gradio as gr

import src.data_extractors.models as models
from src.data_extractors.utils import get_chromadb_client
from src.db import get_database_connection, vacuum_database


def run_model_job(model_opt: models.ModelOpts):
    print(f"Running job for model {model_opt}")
    conn = get_database_connection()
    cdb = get_chromadb_client()
    cursor = conn.cursor()
    cursor.execute("BEGIN")
    images, videos, failed = model_opt.run_extractor(conn, cdb)
    conn.commit()
    failed_str = "\n".join(failed)
    report_str = f"""
    Extraction completed for model {model_opt}.
    Successfully processed {images} images and {videos} videos.
    {len(failed)} files failed to process due to errors.
    Failed files:
    {failed_str}
    """
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


def extractor_job_UI(
    model_type: Type[models.ModelOpts],
    report_state: gr.State,
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
        with gr.Row():
            with gr.Group():
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

    run_button.click(
        fn=run_job,
        inputs=[batch_size, model_choice],
        outputs=[report_state],
    )
    delete_button.click(
        fn=delete_data,
        inputs=[model_choice],
        outputs=[report_state],
    )


def create_extractor_UI(
    report_state: gr.State,
):
    with gr.Row():
        with gr.Tabs():
            extractor_job_UI(models.TagsModel, report_state)
            extractor_job_UI(models.OCRModel, report_state)
            extractor_job_UI(models.WhisperSTTModel, report_state)
            extractor_job_UI(models.ImageEmbeddingModel, report_state)
