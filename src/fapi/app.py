import os

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.templating import Jinja2Templates
from src.db import get_database_connection, get_bookmarks

app = FastAPI()
templates = Jinja2Templates(directory="templates")

def get_all_bookmarks_in_folder(bookmarks_namespace: str, page_size: int = 1000, page: int = 1):
    conn = get_database_connection(force_readonly=True)
    bookmarks, total_bookmarks = get_bookmarks(conn, namespace=bookmarks_namespace, page_size=page_size, page=page)
    conn.close()
    return bookmarks, total_bookmarks

@app.get("/bookmarks/{bookmarks_namespace}/", response_class=HTMLResponse)
async def display_bookmarks(request: Request, bookmarks_namespace: str):
    # Extract "show" parameter from query string
    show = int(request.query_params.get("show", 1000))
    files, total = get_all_bookmarks_in_folder(bookmarks_namespace)
    print(total)
    return templates.TemplateResponse("gallery.html", {
        "request": request,
        "files": files,
        "namespace": bookmarks_namespace,
        "percentages": [5, 10, 20, 25, 33, 40, 50, 60, 66, 80, 100],
        "limit": show
    })

@app.get("/image/{filename:path}")
async def serve_image(filename: str):
    directory = os.path.dirname(filename)
    return FileResponse(os.path.join(directory, os.path.basename(filename)))