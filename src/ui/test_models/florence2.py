import logging
import os
import time
from typing import Dict, List, Literal, Tuple
from unittest.mock import patch

import gradio as gr
from transformers.dynamic_module_utils import get_imports

logger = logging.getLogger(__name__)


def fixed_get_imports(filename: str | os.PathLike) -> list[str]:
    # workaround for unnecessary flash_attn requirement
    
    if not str(filename).endswith("modeling_florence2.py"):
        return get_imports(filename)
    imports = get_imports(filename)
    imports.remove("flash_attn")
    return imports


def load_model(model_name, flash_attention):
    import torch
    from transformers import AutoModelForCausalLM, AutoProcessor
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    # Set to True if you want to use Flash Attention instead of SDPA
    if not flash_attention:
        with patch(
            "transformers.dynamic_module_utils.get_imports", fixed_get_imports
        ):  # workaround for unnecessary flash_attn requirement

            model = (
                AutoModelForCausalLM.from_pretrained(
                    model_name,
                    attn_implementation="sdpa",
                    torch_dtype=torch.float16,
                    trust_remote_code=True,
                )
                .to(device)
                .eval()
            )
    else:
        model = (
            AutoModelForCausalLM.from_pretrained(
                model_name,
                attn_implementation="flash_attention_2",
                torch_dtype=torch.float16,
                trust_remote_code=True,
            )
            .to(device)
            .eval()
        )
    processor = AutoProcessor.from_pretrained(model_name, trust_remote_code=True)
    logger.debug(f"Model {model_name} loaded.")
    return model, processor


# Function to run the model on an example
def run_example(task_prompt, text_input, image, model, processor):
    import torch

    if text_input is None:
        prompt = task_prompt
    else:
        prompt = task_prompt + text_input
    
    # Ensure the image is in RGB mode
    if image.mode != "RGB":
        image = image.convert("RGB")

    # Process the inputs and ensure they are in the correct dtype and device
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    inputs = processor(text=prompt, images=image, return_tensors="pt").to(device)
    inputs = {k: v.half() if v.dtype == torch.float else v for k, v in inputs.items()}

    generated_ids = model.generate(
        input_ids=inputs["input_ids"],
        pixel_values=inputs["pixel_values"],
        max_new_tokens=1024,
        num_beams=3,
    )
    
    generated_text = processor.batch_decode(generated_ids, skip_special_tokens=False)[0]
    parsed_answer = processor.post_process_generation(
        generated_text, task=task_prompt, image_size=(image.width, image.height)
    )
    return parsed_answer

# model repo to list of tuples of task prompt and text input
models_jobs: Dict[str, List[Tuple[str, str | None]]] = {
    "microsoft/Florence-2-large-ft": [("<MORE_DETAILED_CAPTION>", None)],
    "gokaygokay/Florence-2-SD3-Captioner": [
        (
            "<DESCRIPTION>",
            "Describe this image in great detail.",
        )
    ],
    "HuggingFaceM4/Florence-2-DocVQA": [("<MORE_DETAILED_CAPTION>", None)],
    "yayayaaa/florence-2-large-ft-moredetailed": [("<MORE_DETAILED_CAPTION>", None)],
    "ljnlonoljpiljm/florence-2-base-ft-keywords-caption-interleaved": [
        (
            "<MORE_DETAILED_CAPTION>",
            None,
        )
    ],
    "andito/Florence-2-large-ft": [("<MORE_DETAILED_CAPTION>", None)],
    "MiaoshouAI/Florence-2-base-PromptGen": [
        ("<MORE_DETAILED_CAPTION>", None),
        ("<GENERATE_PROMPT>", None),
    ],
    "ljnlonoljpiljm/florence-2-large-docci-caption":
    [
        ("<MORE_DETAILED_CAPTION>", None),
    ],
    "aniketVerma07/finetuned_florence_2": [
        ("<MORE_DETAILED_CAPTION>", None),
    ],
    "Oysiyl/Florence-2-FT-OCR-Cauldron-IAM": [
        ("<OCR>", None),
    ],
}
loaded_models = {}

def create_florence_2_ui():
    gr.Markdown("## Florence-2 Models Demo")

    output_state = gr.State([])
    with gr.Row():
        with gr.Column():
            with gr.Row():
                image_input = gr.Image(label="Upload Image", type="pil")
            with gr.Row():
                pick_models = gr.Dropdown(
                    choices=(
                        [model_name for model_name, _ in models_jobs.items()]
                    ),
                    multiselect=True,
                    interactive=True,
                    label="Choose Models",
                )
            with gr.Row():
                task = gr.Dropdown(
                    choices=['caption', 'ocr', 'ocr_region', 'basic_caption', 'detailed_caption', 'most_detailed_caption'],
                    label="Task",
                    multiselect=True,
                    interactive=True,
                    value="most_detailed_caption",
                )
                use_flash_attention = gr.Checkbox(
                    label="Use Flash Attention",
                    value=False,
                )
            with gr.Row():
                submit_button = gr.Button("Process Image")
                unload = gr.Button("Unload All Models")                
            with gr.Row():
                output_time = gr.Textbox(value="", label="Total Processing Time")
        with gr.Column():
            @gr.render(inputs=output_state)
            def show_outputs(generated_text: List[Dict[str, str]]):
                for output in generated_text:
                    label = output["label"] + f" - Time: {output['time']}"
                    with gr.Row():
                        gr.Textbox(value=output["text"], label=label, show_copy_button=True)
    
    def load_all_models(model_list: List[str] | None, use_flash_attn: bool, progress):
        global loaded_models
        if not model_list:
            # Load all models
            model_list = list(models_jobs.keys())

        logger.info("Loading models...")
        time_start_models = time.time()
        progress(0)
        for model_name in model_list:
            if model_name in loaded_models:
                continue
            progress(float(len(loaded_models.items())) / float(len(model_list)), desc=f"Loading {model_name}...")
            loaded_models[model_name] = load_model(model_name, use_flash_attn)

        logger.info(
            f"{len(loaded_models.items())} Models loaded in {round(time.time() - time_start_models, 2)} seconds."
        )
    
    def unload_all_models():
        global loaded_models
        loaded_models = {}
        import torch
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
        logger.info("Models unloaded.")

    def process_image(image: gr.Image, model_list: List[str] | None, task_types: List[Literal['caption', 'ocr', 'ocr_region', 'basic_caption', 'detailed_caption', 'most_detailed_caption']] | None, flash_attention: bool, progress=gr.Progress()):
        if model_list is not None and len(model_list) == 0:
            model_list = None
        if task_types is None or len(task_types) == 0:
            task_types = ['most_detailed_caption']
        load_all_models(model_list, flash_attention, progress=progress)

        results = []
        start_time = time.time()
        total_tasks = sum(len(tasks) for tasks in models_jobs.values())
        completed_tasks = 0
        progress(0)
        for model_name, (model, processor) in loaded_models.items():
            if model_list is not None and model_name not in model_list:
                continue # Skip models not selected
            tasks: List[Tuple[str, str | None]] = []
            for task_type in task_types:
                if task_type == "ocr":
                    # Only run OCR tasks
                    tasks.append(("<OCR>", None))
                if task_type == "ocr_region":
                    # Only run OCR Region tasks
                    tasks.append(("<OCR_WITH_REGION>", None))
                if task_type == "basic_caption":
                    # Only run basic caption tasks
                    tasks.append(("<CAPTION>", None))
                if task_type == "detailed_caption":
                    # Only run detailed caption tasks
                    tasks.append(("<DETAILED_CAPTION>", None))
                if task_type == "most_detailed_caption":
                    tasks.extend(models_jobs[model_name])
                else:
                    tasks.extend(models_jobs[model_name])
    
            for task_prompt, text_input in tasks:
                progress(
                    float(completed_tasks) / float(total_tasks),
                    desc=f"Processing {model_name} - {task_prompt}...",
                )
                task_start_time = time.time()
                taskresult: Dict[str, str] = run_example(
                            task_prompt,
                            text_input,
                            image,
                            model,
                            processor
                        )
                results.append(
                    {
                        "label": f"{model_name} - {task_prompt}",
                        "text": taskresult.get(task_prompt, taskresult),
                        "time": str(round(time.time() - task_start_time, 2))
                    }
                )
                logger.info(
                    f"{model_name} - {task_prompt} - Time: {results[-1]["time"]}"
                )
                
                completed_tasks += 1

        output_time_str = f"Output time: {str(round(time.time() - start_time, 2))}"
        return results, output_time_str

    submit_button.click(
        process_image, inputs=[image_input, pick_models, task, use_flash_attention], outputs=[output_state, output_time]
    )

    unload.click(unload_all_models)
