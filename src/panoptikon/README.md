# Panoptikon Backend (Python)

This package hosts the FastAPI backend for Panoptikon.

## PQL build endpoint

`/api/search/pql/build` returns compiled SQL for a PQL query without executing it.
The response includes:

- `extra_columns`: mapping of SQL column labels to alias names for result `extra`.
- `check_path`: whether search results should validate file paths after execution.
- `params`: any byte parameters are base64-encoded as `{"__bytes__": "..."}` for JSON transport.
