"""Tests for AI provider abstraction and prompt building."""

from __future__ import annotations

from gitreport.ai import (
    NoneProvider,
    _commits_block,
    build_prompt_overall,
    build_prompt_period,
    get_provider,
    truncate_body,
)
from gitreport.config import AIConfig


def test_truncate_body():
    assert truncate_body("") == ""
    assert truncate_body("short") == "short"
    long = "x" * 600
    result = truncate_body(long, 500)
    assert len(result) <= 501  # 500 + "…"
    assert result.endswith("…")


def test_none_provider():
    p = NoneProvider()
    assert p.is_available()
    assert p.summarize("anything") is None


def test_get_provider_none(default_config):
    from dataclasses import replace
    cfg = replace(default_config, ai=AIConfig(provider="none"))
    p = get_provider(cfg)
    assert p.name == "none"


def test_build_prompt_period(sample_period_data):
    prompt = build_prompt_period("owner/repo", "Week of Mar 01", sample_period_data)
    assert "owner/repo" in prompt
    assert "Week of Mar 01" in prompt
    assert "MERGED PRs" in prompt
    assert "PR #42" in prompt
    assert "alice" in prompt
    assert "Executive Summary" in prompt


def test_build_prompt_period_deep(sample_period_data):
    sample_period_data["diffs_by_pr"] = {42: "diff content here"}
    prompt = build_prompt_period("owner/repo", "Test", sample_period_data, deep=True)
    assert "diff content included" in prompt
    assert "Full diff:" in prompt


def test_build_prompt_period_with_drafts(sample_period_data):
    """Draft PRs should appear in a separate DRAFT section with [DRAFT] tag."""
    sample_period_data["pr_stats"]["draft"] = [{
        "number": 99, "title": "WIP: Spike on auth", "state": "OPEN",
        "author": "bob", "created_at": "2025-02-15T10:00:00Z",
        "merged_at": "", "closed_at": "", "is_draft": 1,
        "additions": 45, "deletions": 5, "comment_count": 1,
        "review_count": 0, "review_decision": "", "labels": [],
        "milestone": "", "body": "Early exploration.", "head_branch": "spike/auth",
        "base_branch": "main", "updated_at": "2025-02-20T12:00:00Z",
        "files": [], "repo": "owner/repo",
    }]
    prompt = build_prompt_period("owner/repo", "Week of Mar 01", sample_period_data)
    assert "DRAFT PRs (1)" in prompt
    assert "[DRAFT]" in prompt
    assert "PR #99" in prompt
    assert "Spike on auth" in prompt


def test_commits_block_with_pr_context():
    """Commits with pr_number get a [PR #N] tag; those without do not."""
    commits = [
        {"sha": "abc1234f", "author": "alice", "committed_at": "2025-03-03T12:00:00Z",
         "message": "fix: edge case", "pr_number": 42},
        {"sha": "def5678a", "author": "bob", "committed_at": "2025-03-04T12:00:00Z",
         "message": "direct fix on main"},
    ]
    result = _commits_block(commits)
    lines = result.strip().split("\n")
    assert "[PR #42]" in lines[0]
    assert "[PR #" not in lines[1]


def test_build_prompt_period_commits_note(sample_period_data):
    """The commits section should include PR provenance guidance for the AI."""
    prompt = build_prompt_period("owner/repo", "Week of Mar 01", sample_period_data)
    assert "not direct pushes" in prompt
    assert "[PR #42]" in prompt


def test_build_prompt_overall(sample_period_data):
    prompt = build_prompt_overall("owner/repo", "Mar 01 – Mar 31, 2025", sample_period_data)
    assert "owner/repo" in prompt
    assert "Mar 01 – Mar 31, 2025" in prompt
    assert "MOST-CHANGED FILES" in prompt
    assert "CONTRIBUTORS" in prompt
    assert "Overall Trajectory" in prompt
