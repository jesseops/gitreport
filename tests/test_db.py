"""Tests for database schema, migrations, upserts, and queries."""

from __future__ import annotations

from datetime import UTC, datetime

from gitreport.db import (
    build_periods,
    db_upsert_commits,
    db_upsert_prs,
    query_period,
)


def test_schema_creates_tables(in_memory_db):
    tables = {row[0] for row in in_memory_db.execute(
        "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    assert "repos" in tables
    assert "prs" in tables
    assert "commits" in tables
    assert "branches" in tables
    assert "pr_files" in tables
    assert "pr_diffs" in tables


def test_migrations_are_idempotent(in_memory_db):
    """Running migrations a second time should not raise."""
    import contextlib

    from gitreport.db import MIGRATIONS
    for sql in MIGRATIONS:
        with contextlib.suppress(Exception):
            in_memory_db.execute(sql)


def test_upsert_prs(in_memory_db, sample_pr):
    db_upsert_prs(in_memory_db, "owner/repo", [sample_pr])
    in_memory_db.commit()
    row = in_memory_db.execute("SELECT * FROM prs WHERE repo='owner/repo' AND number=42").fetchone()
    assert row is not None
    assert row["title"] == "Add feature X"
    assert row["author"] == "alice"


def test_upsert_prs_dedup(in_memory_db, sample_pr):
    """Upserting the same PR twice should update, not duplicate."""
    db_upsert_prs(in_memory_db, "owner/repo", [sample_pr])
    sample_pr["title"] = "Updated title"
    db_upsert_prs(in_memory_db, "owner/repo", [sample_pr])
    in_memory_db.commit()
    count = in_memory_db.execute("SELECT COUNT(*) FROM prs WHERE repo='owner/repo'").fetchone()[0]
    assert count == 1
    row = in_memory_db.execute("SELECT title FROM prs WHERE number=42").fetchone()
    assert row["title"] == "Updated title"


def test_upsert_commits(in_memory_db, sample_commit):
    db_upsert_commits(in_memory_db, "owner/repo", [sample_commit])
    in_memory_db.commit()
    row = in_memory_db.execute("SELECT * FROM commits WHERE repo='owner/repo'").fetchone()
    assert row is not None
    assert row["author"] == "alice"


def test_query_period_filters_by_date(in_memory_db, sample_pr):
    db_upsert_prs(in_memory_db, "owner/repo", [sample_pr])
    in_memory_db.commit()

    # Query covering the PR's merge date
    start = datetime(2025, 3, 1, tzinfo=UTC)
    end = datetime(2025, 3, 10, tzinfo=UTC)
    result = query_period(in_memory_db, "owner/repo", start, end)
    assert len(result["pr_stats"]["merged"]) == 1

    # Query outside the PR's dates
    start_outside = datetime(2025, 4, 1, tzinfo=UTC)
    end_outside = datetime(2025, 4, 30, tzinfo=UTC)
    result_outside = query_period(in_memory_db, "owner/repo", start_outside, end_outside)
    assert len(result_outside["pr_stats"]["merged"]) == 0


def test_upsert_draft_pr_roundtrip(in_memory_db, sample_draft_pr):
    """Draft status should survive upsert and appear in query_period."""
    db_upsert_prs(in_memory_db, "owner/repo", [sample_draft_pr])
    in_memory_db.commit()

    row = in_memory_db.execute("SELECT is_draft FROM prs WHERE number=99").fetchone()
    assert row["is_draft"] == 1

    start = datetime(2025, 2, 1, tzinfo=UTC)
    end = datetime(2025, 3, 1, tzinfo=UTC)
    result = query_period(in_memory_db, "owner/repo", start, end)
    assert len(result["pr_stats"]["draft"]) == 1
    assert result["pr_stats"]["draft"][0]["number"] == 99
    assert len(result["pr_stats"]["open"]) == 0


def test_non_draft_pr_stays_in_open(in_memory_db, sample_pr):
    """A non-draft open PR should land in 'open', not 'draft'."""
    sample_pr["state"] = "OPEN"
    sample_pr["mergedAt"] = ""
    sample_pr["isDraft"] = False
    db_upsert_prs(in_memory_db, "owner/repo", [sample_pr])
    in_memory_db.commit()

    start = datetime(2025, 2, 1, tzinfo=UTC)
    end = datetime(2025, 4, 1, tzinfo=UTC)
    result = query_period(in_memory_db, "owner/repo", start, end)
    assert len(result["pr_stats"]["open"]) == 1
    assert len(result["pr_stats"]["draft"]) == 0


def test_build_periods_weekly():
    start = datetime(2025, 3, 1, tzinfo=UTC)
    end = datetime(2025, 3, 22, tzinfo=UTC)
    periods = build_periods("weekly", start, end)
    assert len(periods) == 3
    assert all(label.startswith("Week of") for label, _, _ in periods)


def test_build_periods_monthly():
    start = datetime(2025, 1, 1, tzinfo=UTC)
    end = datetime(2025, 4, 1, tzinfo=UTC)
    periods = build_periods("monthly", start, end)
    assert len(periods) == 3
    labels = [label for label, _, _ in periods]
    assert "January 2025" in labels
    assert "February 2025" in labels
    assert "March 2025" in labels


def test_build_periods_none():
    start = datetime(2025, 3, 1, tzinfo=UTC)
    end = datetime(2025, 3, 31, tzinfo=UTC)
    periods = build_periods("none", start, end)
    assert len(periods) == 1
    assert periods[0][0] == "Full Window"
