# Custom Inferio Implementations

This folder is a Python package for user-defined model implementations for the Inferio system.

## Usage

- Place your custom model modules here (e.g., `my_model.py`).
- Each module should define an `IMPL_CLASS` that subclasses `InferenceModel`.
- You can use relative imports between modules (e.g., `from .other_module import ...`).
- All modules in this folder will be auto-discovered and loaded by Inferio if they define a valid `IMPL_CLASS`.

## Example

```python
# inferio_custom/my_model.py
from inferio.model import InferenceModel

class MyModel(InferenceModel):
    ...

IMPL_CLASS = MyModel
```

See `example.py` in this folder.

## Notes

- This folder must contain an `__init__.py` file (even if empty).
- You can import from `inferio.`, `panoptikon.`, or use relative imports within this package.
