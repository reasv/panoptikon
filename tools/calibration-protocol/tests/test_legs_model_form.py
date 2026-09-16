"""`--model` takes `group/id`, and a bare id is caught before the leg starts.

`legs.py` forwarded the value verbatim into
`POST /api/jobs/data/extraction?inference_ids=...`, which 400s on a bare id —
after the recorders, the hog and the gateway are already up (Windows pass,
T5). The form is checked while the arguments are.

Run with the managed interpreter:

    python/.venv/bin/python -m pytest tools/calibration-protocol/tests -q
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

LEGS = Path(__file__).resolve().parents[1] / "legs.py"


def _load():
    spec = importlib.util.spec_from_file_location("_calib_legs_model", LEGS)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


legs = _load()


def _run(capsys, *argv) -> str:
    with pytest.raises(SystemExit) as exit:
        legs.main(["--scenario", "S2", "--dry-run", *argv])
    assert exit.value.code == 2
    return capsys.readouterr().err


def test_a_bare_id_is_refused_with_the_form_in_the_message(capsys):
    message = _run(capsys, "--model", "wd-vit-tagger-v3")
    assert "group/id" in message and "wd-vit-tagger-v3" in message


def test_every_id_in_a_chain_is_checked(capsys):
    message = _run(capsys, "--models", "doctr/db_resnet50,all-MiniLM-L6-v2")
    assert "all-MiniLM-L6-v2" in message
    assert "doctr/db_resnet50" not in message


def test_a_trailing_or_leading_slash_is_not_a_group(capsys):
    assert "tags/" in _run(capsys, "--model", "tags/")
    assert "/wd-vit" in _run(capsys, "--model", "/wd-vit")


def test_the_shipped_form_is_accepted(capsys):
    assert legs.main(["--scenario", "S2", "--dry-run", "--models",
                      "doctr/db_resnet50_crnn_mobilenet_v3_small,"
                      "textembed/all-MiniLM-L6-v2"]) == 0


def test_every_scenario_in_the_table_carries_a_usable_id():
    for scenario in legs.SCENARIOS.values():
        for name in scenario.models or (scenario.model,):
            assert legs._INFERENCE_ID.match(name), (scenario.key, name)
