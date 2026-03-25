"""Tests for configuration loading."""

from __future__ import annotations

import pytest

from gitreport.config import Config, load_config


def test_default_config():
    cfg = Config()
    assert cfg.database.path == "gitreport.db"
    assert cfg.ai.provider == "auto"
    assert cfg.report.stale_branch_days == 14
    assert cfg.report.max_diff_tokens == 8_000
    assert cfg.report.default_days == 30
    assert cfg.server.port == 8080


def test_cli_overrides():
    cfg = load_config({"provider": "ollama", "output": "custom.html", "days": 60, "port": 9090})
    assert cfg.ai.provider == "ollama"
    assert cfg.report.output == "custom.html"
    assert cfg.report.default_days == 60
    assert cfg.server.port == 9090


def test_cli_none_overrides():
    """None values in overrides should not change defaults."""
    cfg = load_config({"provider": None, "output": None})
    assert cfg.ai.provider == "auto"
    assert cfg.report.output == "report.html"


def test_toml_loading(tmp_path):
    """Load config from a TOML file."""
    import os
    toml_content = b"""
[database]
path = "custom.db"

[ai]
provider = "claude"

[report]
default_days = 90
"""
    toml_path = tmp_path / "gitreport.toml"
    toml_path.write_bytes(toml_content)

    original_cwd = os.getcwd()
    try:
        os.chdir(tmp_path)
        cfg = load_config()
        assert cfg.database.path == "custom.db"
        assert cfg.ai.provider == "claude"
        assert cfg.report.default_days == 90
    finally:
        os.chdir(original_cwd)


def test_config_frozen():
    """Config should be immutable."""
    cfg = Config()
    with pytest.raises(AttributeError):
        cfg.ai = None  # type: ignore
