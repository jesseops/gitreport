"""Shared fixtures for gitreport tests."""

from __future__ import annotations

import contextlib
import sqlite3

import pytest

from gitreport.config import Config
from gitreport.db import MIGRATIONS, SCHEMA


@pytest.fixture
def in_memory_db() -> sqlite3.Connection:
    """Create an in-memory SQLite database with schema applied."""
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.executescript(SCHEMA)
    for sql in MIGRATIONS:
        with contextlib.suppress(sqlite3.OperationalError):
            con.execute(sql)
    con.commit()
    return con


@pytest.fixture
def default_config(tmp_path) -> Config:
    """Config pointing at a temp database."""
    from dataclasses import replace

    from gitreport.config import Config, DatabaseConfig
    cfg = Config()
    cfg = replace(cfg, database=DatabaseConfig(path=str(tmp_path / "test.db")))
    return cfg


@pytest.fixture
def sample_pr() -> dict:
    """A sample PR dict in GraphQL-normalised shape."""
    return {
        "number": 42,
        "title": "Add feature X",
        "body": "This PR adds feature X to the system.",
        "state": "MERGED",
        "author": {"login": "alice"},
        "createdAt": "2025-03-01T10:00:00Z",
        "updatedAt": "2025-03-05T15:00:00Z",
        "mergedAt": "2025-03-05T14:00:00Z",
        "closedAt": "",
        "baseRefName": "main",
        "headRefName": "feature-x",
        "additions": 150,
        "deletions": 30,
        "comments": 3,
        "reviews": [{"state": "APPROVED"}],
        "reviewDecision": "APPROVED",
        "labels": [{"name": "enhancement"}],
        "milestone": {"title": "v1.0"},
    }


@pytest.fixture
def sample_commit() -> dict:
    """A sample commit dict in GraphQL-normalised shape."""
    return {
        "abbreviatedOid": "abc1234",
        "authors": [{"login": "alice", "name": "Alice"}],
        "committedDate": "2025-03-03T12:00:00Z",
        "message": "fix: resolve edge case in parser",
    }


@pytest.fixture
def sample_period_data() -> dict:
    """Sample period data as returned by query_period."""
    return {
        "pr_stats": {
            "merged": [{
                "number": 42, "title": "Add feature X", "state": "MERGED",
                "author": "alice", "created_at": "2025-03-01T10:00:00Z",
                "merged_at": "2025-03-05T14:00:00Z", "closed_at": "",
                "additions": 150, "deletions": 30, "comment_count": 3,
                "review_count": 1, "review_decision": "APPROVED",
                "labels": ["enhancement"], "milestone": "v1.0",
                "body": "This adds feature X.", "head_branch": "feature-x",
                "base_branch": "main", "updated_at": "2025-03-05T15:00:00Z",
                "files": [{"filename": "src/main.py", "additions": 100, "deletions": 20, "status": "modified"},
                          {"filename": "tests/test_main.py", "additions": 50, "deletions": 10, "status": "modified"}],
                "repo": "owner/repo",
            }],
            "open": [],
            "closed_unmerged": [],
        },
        "user_activity": {
            "alice": {"commits": 5, "prs_opened": 1, "prs_merged": 1, "additions": 150, "deletions": 30},
        },
        "branch_cats": {
            "active": [{"name": "feature-x", "age_days": 3, "last_author": "alice", "is_default": 0}],
            "stale": [],
            "default": [{"name": "main", "is_default": 1}],
        },
        "commits": [
            {"sha": "abc1234", "author": "alice", "committed_at": "2025-03-03T12:00:00Z",
             "message": "fix: resolve edge case in parser", "repo": "owner/repo"},
        ],
        "diffs_by_pr": {},
    }
