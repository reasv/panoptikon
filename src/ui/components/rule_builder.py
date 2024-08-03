from __future__ import annotations

from typing import List, Literal, Tuple, Type

import gradio as gr

from src.data_extractors import models
from src.db import get_database_connection
from src.db.rules.rules import add_rule, delete_rule, get_rules, update_rule
from src.db.rules.types import (
    FilterType,
    MimeFilter,
    MinMaxColumnType,
    MinMaxFilter,
    PathFilter,
    RuleItemFilters,
    StoredRule,
    min_max_columns,
)


def update_filter(
    rule: StoredRule,
    dir: Literal["pos", "neg"],
    filter_idx: int,
    filter: FilterType,
):
    if dir == "pos":
        rule.filters.positive[filter_idx] = filter
    else:
        rule.filters.negative[filter_idx] = filter
    conn = get_database_connection(write_lock=True)
    conn.execute("BEGIN TRANSACTION")
    update_rule(conn, rule.id, rule.setters, rule.filters)
    conn.commit()
    rules = get_rules(conn)
    conn.close()
    return rules


def add_filter(
    rule: StoredRule,
    dir: Literal["pos", "neg"],
    filter: FilterType,
):
    if dir == "pos":
        rule.filters.positive.append(filter)
    else:
        rule.filters.negative.append(filter)
    conn = get_database_connection(write_lock=True)
    conn.execute("BEGIN TRANSACTION")
    update_rule(conn, rule.id, rule.setters, rule.filters)
    conn.commit()
    rules = get_rules(conn)
    conn.close()
    return rules


def remove_filter(
    rule: StoredRule,
    dir: Literal["pos", "neg"],
    filter_idx: int,
):
    if dir == "pos":
        del rule.filters.positive[filter_idx]
    else:
        del rule.filters.negative[filter_idx]
    conn = get_database_connection(write_lock=True)
    conn.execute("BEGIN TRANSACTION")
    update_rule(conn, rule.id, rule.setters, rule.filters)
    conn.commit()
    rules = get_rules(conn)
    conn.close()
    return rules


def delete_entire_rule(rule: StoredRule):
    conn = get_database_connection(write_lock=True)
    conn.execute("BEGIN TRANSACTION")
    delete_rule(conn, rule.id)
    conn.commit()
    rules = get_rules(conn)
    conn.close()
    return rules


def remove_setters_from_rule(
    rule: StoredRule, to_remove: List[Tuple[str, str]]
):
    # Convert to_remove to a list of tuples
    to_remove = [(type, setter) for type, setter in to_remove]
    print(rule.setters, to_remove)
    new_setters = [setter for setter in rule.setters if setter not in to_remove]
    conn = get_database_connection(write_lock=True)
    conn.execute("BEGIN TRANSACTION")
    update_rule(conn, rule.id, new_setters, rule.filters)
    conn.commit()
    rules = get_rules(conn)
    conn.close()
    return rules


def add_setters_to_rule(
    rule: StoredRule, to_add: List[Tuple[str, str]]
) -> List[StoredRule]:
    to_add = [(type, setter) for type, setter in to_add]
    new_setters = list(set(rule.setters + to_add))
    conn = get_database_connection(write_lock=True)
    conn.execute("BEGIN TRANSACTION")
    update_rule(conn, rule.id, new_setters, rule.filters)
    conn.commit()
    rules = get_rules(conn)
    conn.close()
    return rules


def create_new_rule(setters: List[Tuple[str, str]]):
    conn = get_database_connection(write_lock=True)
    filters = RuleItemFilters([], [])
    conn.execute("BEGIN TRANSACTION")
    add_rule(conn, setters, filters)
    conn.commit()
    rules = get_rules(conn)
    conn.close()
    return rules


def on_tab_load():
    conn = get_database_connection(write_lock=False)
    rules = get_rules(conn)
    conn.close()
    return rules


def create_rule_builder_UI(app: gr.Blocks, tab: gr.Tab):
    rules_state = gr.State([])
    gr.on(
        triggers=[tab.select, app.load],
        fn=on_tab_load,
        outputs=[rules_state],
        api_name=False,
    )
    with gr.Row():
        with gr.Column():
            gr.Markdown(
                """
                # Rule system
                The rule system allows you to decide which models to run on which files.
                Each rule has a set of models that it applies to, and a set of filters that the files must match.
                Whenever a particular model's data extraction job is run, the rule system will determine which files
                to run the model on based on the rules you have set up.
                Any file that matches **all** of the positive filters and **none** of the negative filters on a particular rule,
                will be processed by all the models associated with that rule.
                ## Rule creation
                To create a new rule, select the model(s) you want to apply the rule to, and click the button.
                A new rule will be created. You can then add filters to the rule to limit which files it applies to.
                Without any filters, the rule will apply to all files in the system.
                Note that each model has its own set of internal filters that are applied before the rule filters.
                These cannot be modified by the user.
                For example, models will filter out file types that they do not support.
                Therefore, there's no need to add such filters. User filters are for additional restrictions.
                Upon execution of a data extraction job,
                your filters are chained with the model's internal filters for each rule that applies to a particular model.
                Any files that match any of the rules for a model will be processed by that model.
                ## Multiple rules per model
                Rules are evaluated independently of each other. A rule does not affect matches for other rules.
                If a model is associated with multiple rules, any files that match **any** of the rules will be processed by the model.
                In other words, the rules are combined with an OR operation.
                A file only needs to match one rule to be processed by a model.
                ## Cronjob scheduling
                Optionally, a cronjob can be enabled to run the data extraction jobs on a schedule.
                In order for a model to be run by the cronjob, it must be associated with at least one rule.
                If you want a model to process all files, you can create a rule with no filters.
                All rules are by default created with no filters.
                All you need to do is select the model in the **Add New Rule** section and click the button.
            """
            )
        with gr.Column():
            create_add_rule(rules_state)

    @gr.render(inputs=[rules_state])
    def builder(rules: List[StoredRule]):
        rule_count = len(rules)
        gr.Markdown(f"# Stored Rules ({rule_count}):")
        for rule in rules:
            create_rule_builder(rule, rules_state)
        if rule_count == 0:
            gr.Markdown("## No rules stored yet.")


def create_add_rule(rules_state: gr.State):

    def create_model_type_tab(model_type: Type[models.ModelOpts]):
        with gr.TabItem(label=model_type.name()):
            with gr.Group():
                with gr.Row():
                    model_choice = gr.Dropdown(
                        label="Model(s):",
                        multiselect=True,
                        value=[
                            model_type.default_model(),
                        ],
                        choices=[
                            (name, name)
                            for name in model_type.available_models()
                        ],
                    )
                with gr.Row():
                    add_models_btn = gr.Button(
                        "Create New Rule for Selected Model(s)"
                    )

                    @add_models_btn.click(
                        inputs=[model_choice], outputs=[rules_state]
                    )
                    def add_models(chosen_models: List[str]):
                        return create_new_rule(
                            [
                                (model_type.data_type(), model_name)
                                for model_name in chosen_models
                            ]
                        )

    gr.Markdown("# Add New Rule")
    with gr.Tabs():
        for model_type in models.ALL_MODEL_OPTS:
            create_model_type_tab(model_type)


def create_rule_builder(rule: StoredRule, rules_state: gr.State):
    with gr.Row():
        gr.Markdown(f"## Rule #{rule.id}")
    with gr.Row():
        delete_rule_btn = gr.Button("Delete Rule", scale=0)
    with gr.Row():
        with gr.Column():
            gr.Markdown("## Models:")
            gr.Markdown("### Rule applies when running the following models:")
            gr.Markdown(
                f"### {', '.join([f'{setter} ({type})' for type, setter in rule.setters])}"
            )
            with gr.Accordion(label="Remove Models", open=False):
                create_remove_models(rule, rules_state)
            with gr.Accordion(label="Add Models", open=False):
                create_add_models(rule, rules_state)
        with gr.Column():
            gr.Markdown("## Item filters:")
            with gr.Accordion(label="Add New Filter", open=False):
                create_add_filter(rules_state, rule)

            pos_count = len(rule.filters.positive)
            with gr.Accordion(
                label=f"Positive filters ({pos_count}):", open=pos_count > 0
            ):
                if rule.filters.positive:
                    gr.Markdown(
                        "## Items MUST MATCH **ALL** of the following filters:"
                    )
                    for i, filter in enumerate(rule.filters.positive):
                        create_filter_edit(rules_state, rule, "pos", i, filter)
            neg_count = len(rule.filters.negative)
            with gr.Accordion(
                label=f"Negative filters ({neg_count}):", open=neg_count > 0
            ):
                if rule.filters.negative:
                    gr.Markdown(
                        "## Items MUST **NOT** MATCH **ANY** of the following filters:"
                    )
                    for i, filter in enumerate(rule.filters.negative):
                        create_filter_edit(rules_state, rule, "neg", i, filter)

    @delete_rule_btn.click(outputs=[rules_state])
    def delete():
        return delete_entire_rule(rule)


def create_remove_models(rule: StoredRule, rules_state: gr.State):
    to_remove_select = gr.Dropdown(
        label="Remove the following models from the rule",
        value=[],
        multiselect=True,
        choices=[(f"{st}|{sn}", (st, sn)) for st, sn in rule.setters],  # type: ignore
    )
    remove_models_btn = gr.Button("Remove Selected Model(s)")

    @remove_models_btn.click(inputs=[to_remove_select], outputs=[rules_state])
    def remove_models(to_remove: List[Tuple[str, str]]):
        return remove_setters_from_rule(rule, to_remove)


def create_add_models(rule: StoredRule, rules_state: gr.State):
    def create_model_type_tab(model_type: Type[models.ModelOpts]):
        with gr.TabItem(label=model_type.name()) as extractor_tab:
            with gr.Group():
                with gr.Row():
                    model_choice = gr.Dropdown(
                        label="Model(s):",
                        multiselect=True,
                        value=[
                            model_type.default_model(),
                        ],
                        choices=[
                            (name, name)
                            for name in model_type.available_models()
                        ],
                    )
                with gr.Row():
                    add_models_btn = gr.Button("Add Selected Model(s)")

                    @add_models_btn.click(
                        inputs=[model_choice], outputs=[rules_state]
                    )
                    def add_models(chosen_model: List[str]):
                        return add_setters_to_rule(
                            rule,
                            [
                                (model_type.data_type(), model_name)
                                for model_name in chosen_model
                            ],
                        )

    with gr.Tabs():
        for model_type in models.ALL_MODEL_OPTS:
            create_model_type_tab(model_type)


def create_add_filter(
    rules_state: gr.State,
    rule: StoredRule,
):
    pos_neg = gr.Dropdown(
        label="Items MUST/MUST NOT match filter:",
        choices=["MUST MATCH", "MUST NOT MATCH"],
        value="MUST MATCH",
    )
    with gr.Tabs():
        with gr.Tab("Path Filter"):
            paths = gr.Dropdown(
                label="File path starts with one of",
                multiselect=True,
                allow_custom_value=True,
                value=[],
            )
            path_filter_btn = gr.Button("Add Path Filter")
        with gr.Tab("MIME Type Filter"):
            mime_types = gr.Dropdown(
                label="MIME Type starts with one of",
                multiselect=True,
                allow_custom_value=True,
                value=[],
            )
            mime_filter_btn = gr.Button("Add MIME Type Filter")
        with gr.Tab("Min Max Filter"):
            with gr.Row():
                column_name = gr.Dropdown(
                    label="Column Name",
                    choices=min_max_columns,
                    value="width",
                )
                minimum = gr.Number(label="Min Value", value=0)
                maximum = gr.Number(label="Max Value", value=0)
            with gr.Row():
                min_max_filter_btn = gr.Button("Add Min Max Filter")

    @path_filter_btn.click(inputs=[pos_neg, paths], outputs=[rules_state])
    def create_path_filter(pos_neg: str, paths: List[str]):
        filter = PathFilter(path_prefixes=paths)
        direction = "pos" if pos_neg == "MUST MATCH" else "neg"
        new_rules = add_filter(rule, direction, filter)
        return new_rules

    @mime_filter_btn.click(inputs=[pos_neg, mime_types], outputs=[rules_state])
    def create_mime_filter(pos_neg: str, mime_types: List[str]):
        filter = MimeFilter(mime_type_prefixes=mime_types)
        direction = "pos" if pos_neg == "MUST MATCH" else "neg"
        new_rules = add_filter(rule, direction, filter)
        return new_rules

    @min_max_filter_btn.click(
        inputs=[pos_neg, column_name, minimum, maximum], outputs=[rules_state]
    )
    def create_min_max_filter(
        pos_neg: str,
        column_name: MinMaxColumnType,
        minimum: float,
        maximum: float,
    ):
        filter = MinMaxFilter(
            min_value=minimum, max_value=maximum, column_name=column_name
        )
        direction = "pos" if pos_neg == "MUST MATCH" else "neg"
        new_rules = add_filter(rule, direction, filter)
        return new_rules


def create_filter_edit(
    rules_state: gr.State,
    rule: StoredRule,
    dir: Literal["pos", "neg"],
    filter_idx: int,
    filter: FilterType,
):
    if isinstance(filter, PathFilter):
        return path_filter_edit(rules_state, rule, dir, filter_idx, filter)
    elif isinstance(filter, MimeFilter):
        return mime_type_filter_edit(rules_state, rule, dir, filter_idx, filter)
    elif isinstance(filter, MinMaxFilter):
        return min_max_filter_edit(rules_state, rule, dir, filter_idx, filter)


def path_filter_edit(
    rules_state: gr.State,
    rule: StoredRule,
    dir: Literal["pos", "neg"],
    filter_idx: int,
    filter: PathFilter,
):
    gr.Markdown(f"### File Path Filter")
    element = gr.Dropdown(
        key=f"rule{rule.id}_{dir}_filter_{filter_idx}",
        label="File path starts with one of",
        multiselect=True,
        allow_custom_value=True,
        value=filter.path_prefixes,
    )
    with gr.Row():
        update_button = gr.Button("Update")
        delete_button = gr.Button("Remove")

    @update_button.click(inputs=[element], outputs=[rules_state])
    def update_path_filter(path_prefixes: List[str]):
        filter.path_prefixes = path_prefixes
        new_rules = update_filter(rule, dir, filter_idx, filter)
        return new_rules

    @delete_button.click(outputs=[rules_state])
    def delete_path_filter():
        new_rules = remove_filter(rule, dir, filter_idx)
        return new_rules

    return element


def mime_type_filter_edit(
    rules_state: gr.State,
    rule: StoredRule,
    dir: Literal["pos", "neg"],
    filter_idx: int,
    filter: MimeFilter,
):
    gr.Markdown(f"### Mime Type Filter")
    element = gr.Dropdown(
        key=f"rule{rule.id}_{dir}_filter_{filter_idx}",
        label="MIME Type starts with one of",
        multiselect=True,
        allow_custom_value=True,
        value=filter.mime_type_prefixes,
    )
    with gr.Row():
        update_button = gr.Button("Update")
        delete_button = gr.Button("Remove")

    @update_button.click(inputs=[element], outputs=[rules_state])
    def update_mime_type_filter(mime_type_prefixes: List[str]):
        filter.mime_type_prefixes = mime_type_prefixes
        new_rules = update_filter(rule, dir, filter_idx, filter)
        return new_rules

    @delete_button.click(outputs=[rules_state])
    def delete_path_filter():
        new_rules = remove_filter(rule, dir, filter_idx)
        return new_rules

    return element


def min_max_filter_edit(
    rules_state: gr.State,
    rule: StoredRule,
    dir: Literal["pos", "neg"],
    filter_idx: int,
    filter: MinMaxFilter,
):
    gr.Markdown(f"### Min Max Filter on: {filter.column_name}")
    with gr.Row():
        min_element = gr.Number(
            key=f"rule{rule.id}_{dir}_filter_{filter_idx}_min",
            label="Min Value",
            value=filter.min_value,
        )
        max_element = gr.Number(
            key=f"rule{rule.id}_{dir}_filter_{filter_idx}_max",
            label="Max Value",
            value=filter.max_value,
        )
    with gr.Row():
        update_button = gr.Button("Update")
        delete_button = gr.Button("Remove")

    @update_button.click(
        inputs=[min_element, max_element], outputs=[rules_state]
    )
    def update_min_max_filter(min_value: float, max_value: float):
        filter.min_value = min_value
        filter.max_value = max_value
        new_rules = update_filter(rule, dir, filter_idx, filter)
        return new_rules

    @delete_button.click(outputs=[rules_state])
    def delete_min_max_filter():
        new_rules = remove_filter(rule, dir, filter_idx)
        return new_rules

    return min_element, max_element
