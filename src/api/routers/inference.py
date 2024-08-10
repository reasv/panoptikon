from fastapi import APIRouter

router = APIRouter(
    prefix="/inference",
    tags=["inference"],
    responses={404: {"description": "Not found"}},
)


@router.get("/predict/{inference_id}")
def predict(inference_id: str):
    return {"inference_id": inference_id}


@router.get("/load/{inference_id}")
def prepare(inference_id: str):
    return {"inference_id": inference_id}


@router.get("/unload/{inference_id}")
def unload(inference_id: str):
    return {"inference_id": inference_id}


@router.get("/list")
def list_inferences():
    return {"inferences": []}
