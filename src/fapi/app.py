import os
import hashlib
from typing import Any, Dict, List, Optional

from fastapi import Depends, FastAPI, Query, Request
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from src.db import get_database_connection, get_bookmarks, find_paths_by_tags
from src.files import get_files_by_extension, get_image_extensions, get_last_modified_time

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
    show = int(request.query_params.get("show", 0))
    page_size = int(request.query_params.get("page_size", 1000))
    page = int(request.query_params.get("page", 1))
    files, total = get_all_bookmarks_in_folder(bookmarks_namespace, page_size=page_size, page=page)
    print(total)
    return templates.TemplateResponse("gallery.html", {
        "request": request,
        "files": files,
        "namespace": bookmarks_namespace,
        "percentages": [5, 10, 20, 25, 33, 40, 50, 60, 66, 80, 100],
        "limit": show
    })

def get_all_items_with_tags(tags: list, min_confidence: float, page_size: int = 1000, page: int = 1, include_path: str=None):
    conn = get_database_connection(force_readonly=True)
    items, total_items = find_paths_by_tags(conn, tags, min_confidence=min_confidence, page_size=page_size, page=page, include_path=include_path)
    conn.close()
    return items, total_items

@app.get("/search/tags", response_class=HTMLResponse)
async def search_by_tags_html(
        request: Request,
        tags: str = Query("", alias="tags"),
        min_confidence: float = Query(0.25, ge=0.0),
        show: int = Query(0, ge=0),
        include_path: Optional[str] = Query(None),
        page_size: int = Query(100, ge=1),
        page: int = Query(1, ge=1),
    ):
    tags_list = [tag.strip() for tag in tags.split(",") if tag.strip() != ""]
    files_dicts, total = get_all_items_with_tags(tags_list, min_confidence, page_size=page_size, page=page, include_path=include_path)
    files = [(file['sha256'], file['path']) for file in files_dicts]
    print(tags, tags_list)
    print(total)
    return templates.TemplateResponse("gallery.html", {
        "request": request,
        "files": files,
        "percentages": [5, 10, 20, 25, 33, 40, 50, 60, 66, 80, 100],
        "limit": show
    })

@app.get("/api/search/tags", response_class=JSONResponse)
async def search_by_tags_json(
        tags: str = Query("", alias="tags"),
        min_confidence: float = Query(0.25, ge=0.0),
        include_path: Optional[str] = Query(None),
        page_size: int = Query(100, ge=1),
        page: int = Query(1, ge=1),
    ):
    tags_list = [tag.strip() for tag in tags.split(",") if tag.strip() != ""]
    files, total = get_all_items_with_tags(tags_list, min_confidence, page_size, page, include_path)
    print(total)
    return JSONResponse({
        "files": files,
        "tags": tags,
        "total": total,
        "page_size": page_size,
        "page": page
    })

@app.get("/browse/{foldername:path}/", response_class=HTMLResponse)
async def browse_folder(request: Request, foldername: str):
    files_dicts = []
    # Whether or not to include subdirectories
    include_subdirs = request.query_params.get("subdirs", "false") == "true"
    # Convert foldername to have the correct slashes for the current OS
    foldername = os.path.join(os.path.normpath(foldername), "")
    for file_path in get_files_by_extension([foldername], [], get_image_extensions()):
        # Skip files that are not directly in the current directory
        dirname = os.path.join(os.path.dirname(file_path), "")
        if (
            not include_subdirs 
            and dirname != foldername
        ):  
            print(f"Skipping {dirname} because it is not in {foldername}")
            continue

        # Calculate sha256 hash of the file path instead of the file content for speed
        # Since we are browsing a single directory tree, this should be unique
        sha256 = hashlib.sha256(file_path.encode()).hexdigest()
        files_dicts.append(
            {
                'sha256': sha256,
                'path': file_path,
                'last_modified': get_last_modified_time(file_path)
            }
        )
    # Extract sort parameter from query string
    sort = request.query_params.get("sort", "last_modified")
    if sort == "last_modified":
        # Sort files by last modified time
        # Desc sort by default to show the latest files first
        reverse = request.query_params.get("desc", "true") == "true"
        files_dicts.sort(key=lambda x: x['last_modified'], reverse=reverse)
    elif sort == "path":
        # Sort files by path
        # asc sort by default to show the latest files first
        reverse = request.query_params.get("desc", "false") == "true"
        files_dicts.sort(key=lambda x: x['path'], reverse=reverse)

    print(len(files_dicts))

    files = [(file['sha256'], file['path']) for file in files_dicts]
    # Extract "show" parameter from query string
    show = int(request.query_params.get("show", 0))
    return templates.TemplateResponse("gallery.html", {
        "request": request,
        "files": files,
        "percentages": [5, 10, 20, 25, 33, 40, 50, 60, 66, 80, 100],
        "limit": show
    })

@app.get("/file/{filename:path}")
async def serve_image(filename: str):
    directory = os.path.dirname(filename)
    # Cache the file for 30 minutes
    return FileResponse(
        os.path.join(directory, os.path.basename(filename)),
        headers={"Cache-Control": "max-age=1800"}
    )

# Redirect / to /gradio
@app.get("/")
async def redirect_to_gradio():
    return RedirectResponse(url="/gradio/")
