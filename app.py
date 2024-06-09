from flask import Flask, jsonify, render_template, send_from_directory
import os

from src.db import get_database_connection, get_bookmarks
app = Flask(__name__)

def get_all_bookmarks_in_folder(bookmarks_namespace: str, page_size: int = 1000, page: int = 1):
    conn = get_database_connection(force_readonly=True)
    bookmarks, total_bookmarks = get_bookmarks(conn, namespace=bookmarks_namespace, page_size=page_size, page=page)
    conn.close()
    return bookmarks, total_bookmarks

@app.route('/<bookmarks_namespace>/')
def display_bookmarks(bookmarks_namespace):
    bookmarks, total = get_all_bookmarks_in_folder(bookmarks_namespace)
    print(bookmarks)
    return render_template('gallery.html', bookmarks=bookmarks, namespace=bookmarks_namespace)

@app.route('/image/<path:filename>')
def serve_image(filename):
    directory = os.path.dirname(filename)
    return send_from_directory(directory, os.path.basename(filename))

def launch_app():
    app.run(debug=True)

if __name__ == '__main__':
    launch_app()