from __future__ import annotations

from typing import List

import gradio as gr

from src.db import get_database_connection


def arbitrary_query(query: str, query_history: List):
    conn = get_database_connection(force_readonly=True)
    cursor = conn.cursor()
    cursor.execute("BEGIN")
    try:
        cursor.execute(query)
        # Fetch the results
        result = cursor.fetchall()

        # Get column names
        column_names = [description[0] for description in cursor.description]
        n_results = len(result)
    except Exception as e:
        result = [[str(e)]]  # Return the error message
        column_names = ["Error"]
        n_results = 0

    conn.rollback()
    conn.close()
    query_history = query_history or []
    query_history = [[query, n_results]] + query_history
    return {"data": result, "headers": column_names}, query_history


def click_past_queries(query: List[str]):
    return query[0]


def update_dataset(query_history: List):
    return gr.Dataset(samples=query_history)


def create_query_UI():
    initial_history = [
        [f"SELECT * FROM items", 0],
        [f"SELECT * FROM files", 0],
        [f"SELECT * FROM bookmarks", 0],
    ]
    query_history = gr.State(initial_history)
    with gr.TabItem(label="Query Database"):
        query = gr.Textbox(
            label="Enter your SQL query here (Read Only)", lines=20
        )
        run_query = gr.Button("Run Query")
        with gr.Tabs():
            with gr.Tab(label="Results"):
                result = gr.DataFrame(label="Query Result", height=900)
            with gr.Tab(label="Past Queries"):
                query_history_table = gr.Dataset(
                    label="Query History",
                    type="values",
                    samples_per_page=25,
                    headers=["Query", "Results"],
                    components=["textbox", "number"],
                    samples=initial_history,
                )

    run_query.click(
        fn=arbitrary_query,
        inputs=[query, query_history],
        outputs=[result, query_history],
    )

    query_history_table.click(
        fn=click_past_queries,
        inputs=[query_history_table],
        outputs=[query],
    )

    query_history.change(
        fn=update_dataset,
        inputs=[query_history],
        outputs=[query_history_table],
    )
