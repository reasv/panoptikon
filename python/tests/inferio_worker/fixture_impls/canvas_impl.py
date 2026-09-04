"""Test fixture impl that exposes a per-item pixel canvas to introspection.

Run2 R7: a model whose input geometry the registry cannot state statically —
`doctr/dots_ocr`, whose ceiling lives in an `AutoProcessor` config downloaded
with the weights — is knowable only from a loaded process, so the worker
reports the canvas it can read off the instance on its `load` response
(docs/inferio-worker-protocol.md, "Memory sensing").

The tier is chosen by config so one fixture covers all three shapes the
resolver has to answer for:

- `"one"`  → `instance.embedder.max_pixels`, the qwen3-vl shape;
- `"two"`  → `instance.model.processor.max_pixels`, the nemotron/dots.ocr
  shape, which is as deep as the walk goes;
- `"none"` → nothing to find, which must report nothing rather than a guess.

The attributes appear in `load()`, not in `__init__`, because that is where a
real impl builds its processor and because the worker reads them after the
load has returned.
"""


class _Holder:
    def __init__(self, max_pixels: int) -> None:
        self.max_pixels = max_pixels


class _Nested:
    def __init__(self, max_pixels: int) -> None:
        self.processor = _Holder(max_pixels)


class CanvasModel:
    def __init__(self, **config):
        self.config = config

    @classmethod
    def name(cls) -> str:
        return "canvas_test"

    def load(self) -> None:
        tier = self.config.get("canvas_tier", "one")
        if tier == "one":
            self.embedder = _Holder(1_843_200)
        elif tier == "two":
            self.model = _Nested(11_289_600)
        elif tier == "floored":
            # Below the 512x512 floor: an attribute that happens to be named
            # `max_pixels` and holds something else must be refused, not
            # trusted — too small a cap under-prices an item, which
            # over-admits.
            self.embedder = _Holder(4096)

    def predict(self, inputs):
        return [{"echo": None} for _ in inputs]

    def unload(self) -> None:
        pass


IMPL_CLASS = CanvasModel
