"""Tests for CLI argument parsing."""

from __future__ import annotations

import pytest

from gitreport.cli import main


def test_cli_requires_command():
    with pytest.raises(SystemExit):
        main([])


def test_cli_version(capsys):
    with pytest.raises(SystemExit) as exc_info:
        main(["--version"])
    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    assert "0.1.0" in captured.out


def test_cli_sync_requires_repo():
    with pytest.raises(SystemExit):
        main(["sync"])


def test_cli_report_requires_repo():
    with pytest.raises(SystemExit):
        main(["report"])
