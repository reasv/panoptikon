"""Tests for env-variable templating in the inferio registry config loader.

The Python semantics must match the Rust gateway's gateway/src/env_template.rs
exactly: both sides parse the same shipped registry TOML.
"""

from collections import defaultdict
from pathlib import Path

import pytest

from inferio.config import (
    _substitute_env_str,
    get_base_config_folder,
    load_config_folder,
    substitute_env_templates,
)


def _empty_registry():
    return defaultdict(
        lambda: {
            "inference_ids": {},
            "group_config": {},
            "group_metadata": {},
        }
    )


def test_basic_substitution_and_multiple_per_string(monkeypatch):
    monkeypatch.setenv("ET_BASIC_A", "alpha")
    monkeypatch.setenv("ET_BASIC_B", "beta")
    assert _substitute_env_str("${ET_BASIC_A}") == "alpha"
    assert (
        _substitute_env_str("x-${ET_BASIC_A}-y-${ET_BASIC_B}-z")
        == "x-alpha-y-beta-z"
    )


def test_colon_dash_default_form(monkeypatch):
    monkeypatch.setenv("ET_DEF_SET", "real")
    assert _substitute_env_str("${ET_DEF_SET:-fallback}") == "real"
    monkeypatch.delenv("ET_DEF_UNSET_XYZ", raising=False)
    assert _substitute_env_str("${ET_DEF_UNSET_XYZ:-fallback}") == "fallback"
    monkeypatch.setenv("ET_DEF_EMPTY", "")
    # Shell :- treats set-but-empty like unset.
    assert _substitute_env_str("${ET_DEF_EMPTY:-fallback}") == "fallback"
    assert _substitute_env_str("${ET_DEF_UNSET_XYZ:-}") == ""
    assert (
        _substitute_env_str("${ET_DEF_UNSET_XYZ:-C:\\data dir:$1}")
        == "C:\\data dir:$1"
    )


def test_dash_default_form(monkeypatch):
    monkeypatch.setenv("ET_DASH_SET", "real")
    assert _substitute_env_str("${ET_DASH_SET-fallback}") == "real"
    monkeypatch.delenv("ET_DASH_UNSET_XYZ", raising=False)
    assert _substitute_env_str("${ET_DASH_UNSET_XYZ-fallback}") == "fallback"
    monkeypatch.setenv("ET_DASH_EMPTY", "")
    # Shell - keeps a set-but-empty value.
    assert _substitute_env_str("[${ET_DASH_EMPTY-fallback}]") == "[]"
    assert _substitute_env_str("${ET_DASH_UNSET_XYZ-}") == ""


def test_bare_form_empty_value_is_not_an_error(monkeypatch):
    monkeypatch.setenv("ET_EMPTY", "")
    assert _substitute_env_str("${ET_EMPTY}") == ""


def test_unset_without_default_is_error(monkeypatch):
    monkeypatch.delenv("ET_UNSET_NO_DEFAULT_XYZ", raising=False)
    with pytest.raises(ValueError) as exc:
        _substitute_env_str("${ET_UNSET_NO_DEFAULT_XYZ}")
    assert "ET_UNSET_NO_DEFAULT_XYZ" in str(exc.value)
    assert ":-" in str(exc.value)  # suggests the default form


def test_escapes_and_literal_dollars(monkeypatch):
    assert _substitute_env_str("$${ET_X}") == "${ET_X}"
    assert _substitute_env_str("cost: $5") == "cost: $5"
    assert _substitute_env_str("a$$b") == "a$$b"
    assert _substitute_env_str("trailing $") == "trailing $"
    monkeypatch.setenv("ET_ESC_MIX", "v")
    assert _substitute_env_str("$${ET_ESC_MIX} ${ET_ESC_MIX}") == "${ET_ESC_MIX} v"


def test_no_recursive_expansion(monkeypatch):
    monkeypatch.setenv("ET_RECURSE", "${ET_INNER}")
    assert _substitute_env_str("${ET_RECURSE}") == "${ET_INNER}"


@pytest.mark.parametrize(
    "bad", ["${}", "${1BAD}", "${NAME", "${NAME:x}", "${NAME:-open"]
)
def test_malformed_placeholders_error(bad):
    with pytest.raises(ValueError) as exc:
        _substitute_env_str(bad)
    assert "malformed" in str(exc.value) or "closing" in str(exc.value)


def test_name_dash_text_is_unset_only_default_form(monkeypatch):
    monkeypatch.delenv("ET_SURELY_UNSET_NA", raising=False)
    assert _substitute_env_str("${ET_SURELY_UNSET_NA-ME}") == "ME"


def test_walks_nested_dicts_and_lists(monkeypatch):
    monkeypatch.setenv("ET_TREE", "sub")
    monkeypatch.delenv("ET_TREE_MISSING", raising=False)
    data = {
        "top": "${ET_TREE}",
        "num": 5,
        "flag": True,
        "table": {
            "inner": {
                "key": "a ${ET_TREE} b",
                "list": ["${ET_TREE}", "plain", "${ET_TREE_MISSING:-d}"],
            }
        },
        "array_of_tables": [{"key": "${ET_TREE}"}],
    }
    result = substitute_env_templates(data, Path("test.toml"))
    assert result["top"] == "sub"
    assert result["num"] == 5
    assert result["flag"] is True
    assert result["table"]["inner"]["key"] == "a sub b"
    assert result["table"]["inner"]["list"] == ["sub", "plain", "d"]
    assert result["array_of_tables"][0]["key"] == "sub"


def test_error_names_file_and_variable(monkeypatch):
    monkeypatch.delenv("ET_MISSING_SECRET_XYZ", raising=False)
    data = {"section": {"secret": "${ET_MISSING_SECRET_XYZ}"}}
    with pytest.raises(ValueError) as exc:
        substitute_env_templates(data, Path("conf/gw.toml"))
    text = str(exc.value)
    assert "gw.toml" in text
    assert "ET_MISSING_SECRET_XYZ" in text


def test_loader_applies_templating(tmp_path, monkeypatch):
    monkeypatch.setenv("ET_LOADER_KEY", "sekrit")
    (tmp_path / "a.toml").write_text(
        '[group.g.inference_ids.m]\nconfig.api_key = "${ET_LOADER_KEY}"\n',
        encoding="utf-8",
    )
    config = load_config_folder(tmp_path, _empty_registry())
    assert config["g"]["inference_ids"]["m"]["config"]["api_key"] == "sekrit"


def test_loader_error_names_file_and_variable(tmp_path, monkeypatch):
    monkeypatch.delenv("ET_LOADER_UNSET_XYZ", raising=False)
    (tmp_path / "bad.toml").write_text(
        '[group.g.inference_ids.m]\nconfig.api_key = "${ET_LOADER_UNSET_XYZ}"\n',
        encoding="utf-8",
    )
    with pytest.raises(ValueError) as exc:
        load_config_folder(tmp_path, _empty_registry())
    text = str(exc.value)
    assert "bad.toml" in text
    assert "ET_LOADER_UNSET_XYZ" in text


def test_shipped_registry_secret_wiring(monkeypatch):
    """The shipped registry loads with and without the secret env vars, and
    the api_key values land in the right inference-id configs."""
    # With the vars set, the values flow through.
    monkeypatch.setenv("SAUCENAO_API_KEY", "sn-test-key")
    monkeypatch.setenv("JINA_API_KEY", "jina-test-key")
    config = load_config_folder(get_base_config_folder(), _empty_registry())
    saucenao = config["tagmatch"]["inference_ids"]["danbooru-saucenao"]
    assert saucenao["config"]["api_key"] == "sn-test-key"
    for group, inf_id in [
        ("textembed", "jina-embeddings-v3-api"),
        ("clip", "jina-clip-v2-api"),
        ("tclip", "jina-clip-v2-api"),
    ]:
        inf = config[group]["inference_ids"][inf_id]
        assert inf["config"]["api_key"] == "jina-test-key", (group, inf_id)

    # Without them, `${VAR:-}` defaults to "" and loading must not fail
    # (empty string is falsy, so impls fall through to their env lookup).
    monkeypatch.delenv("SAUCENAO_API_KEY")
    monkeypatch.delenv("JINA_API_KEY")
    config = load_config_folder(get_base_config_folder(), _empty_registry())
    saucenao = config["tagmatch"]["inference_ids"]["danbooru-saucenao"]
    assert saucenao["config"]["api_key"] == ""
    for group, inf_id in [
        ("textembed", "jina-embeddings-v3-api"),
        ("clip", "jina-clip-v2-api"),
        ("tclip", "jina-clip-v2-api"),
    ]:
        inf = config[group]["inference_ids"][inf_id]
        assert inf["config"]["api_key"] == "", (group, inf_id)
