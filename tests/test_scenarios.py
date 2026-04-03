"""Scenario-based tests exercising the query_period -> prompt pipeline.

Each test sets up realistic DB state and verifies end-to-end behavior:
data goes in via db_upsert_*, comes out via query_period(), and optionally
flows through build_prompt_period() for prompt fidelity checks.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from gitreport.ai import build_prompt_period
from gitreport.db import (
    db_upsert_branches,
    db_upsert_commits,
    db_upsert_prs,
    query_period,
)

from tests.conftest import make_branch, make_commit, make_pr

REPO = "owner/repo"
MAR_START = datetime(2025, 3, 1, tzinfo=UTC)
MAR_END = datetime(2025, 4, 1, tzinfo=UTC)


# ── Helpers ───────────────────────────────────────────────────────────────


def _insert_and_query(db, prs=None, commits=None, branches=None,
                      start=MAR_START, end=MAR_END, default_branch="main",
                      **query_kwargs):
    """Insert data and run query_period in one step."""
    if prs:
        db_upsert_prs(db, REPO, prs)
    if commits:
        db_upsert_commits(db, REPO, commits)
    if branches:
        db_upsert_branches(db, REPO, branches, default_branch)
    db.commit()
    return query_period(db, REPO, start, end, **query_kwargs)


# ═══════════════════════════════════════════════════════════════════════════
# Scenario Group 1: PR Lifecycle & Categorization
# ═══════════════════════════════════════════════════════════════════════════


class TestPRLifecycle:
    """PRs should land in the correct bucket based on state and dates."""

    def test_pr_created_before_period_merged_during(self, in_memory_db):
        pr = make_pr(1, created="2025-02-10T10:00:00Z", merged="2025-03-15T10:00:00Z")
        result = _insert_and_query(in_memory_db, prs=[pr])
        assert len(result["pr_stats"]["merged"]) == 1
        assert result["pr_stats"]["merged"][0]["number"] == 1

    def test_pr_created_during_period_still_open(self, in_memory_db):
        pr = make_pr(2, state="OPEN", created="2025-03-10T10:00:00Z",
                     merged="", is_draft=False)
        result = _insert_and_query(in_memory_db, prs=[pr])
        assert len(result["pr_stats"]["open"]) == 1

    def test_long_lived_open_pr(self, in_memory_db):
        """Open PR created 6 months ago should still appear in current period."""
        pr = make_pr(3, state="OPEN", created="2024-09-01T10:00:00Z",
                     updated="2024-10-01T10:00:00Z", merged="")
        result = _insert_and_query(in_memory_db, prs=[pr])
        assert len(result["pr_stats"]["open"]) == 1

    def test_long_lived_draft_pr(self, in_memory_db):
        """Draft PR created months ago should appear in draft bucket."""
        pr = make_pr(4, state="OPEN", is_draft=True,
                     created="2024-09-01T10:00:00Z", merged="")
        result = _insert_and_query(in_memory_db, prs=[pr])
        assert len(result["pr_stats"]["draft"]) == 1
        assert len(result["pr_stats"]["open"]) == 0

    def test_closed_without_merge(self, in_memory_db):
        pr = make_pr(5, state="CLOSED", merged="",
                     closed="2025-03-20T10:00:00Z")
        result = _insert_and_query(in_memory_db, prs=[pr])
        assert len(result["pr_stats"]["closed_unmerged"]) == 1

    def test_closed_unmerged_outside_period(self, in_memory_db):
        """Closed-unmerged PR from a previous period should not appear."""
        pr = make_pr(6, state="CLOSED", merged="",
                     created="2025-01-01T10:00:00Z",
                     closed="2025-02-15T10:00:00Z")
        result = _insert_and_query(in_memory_db, prs=[pr])
        for bucket in result["pr_stats"].values():
            assert len(bucket) == 0

    def test_mixed_pr_states(self, in_memory_db):
        """Multiple PRs in different states all categorized correctly."""
        prs = [
            make_pr(10, state="MERGED", merged="2025-03-05T10:00:00Z"),
            make_pr(11, state="OPEN", merged="", created="2025-03-10T10:00:00Z"),
            make_pr(12, state="OPEN", is_draft=True, merged="",
                    created="2025-03-10T10:00:00Z"),
            make_pr(13, state="CLOSED", merged="",
                    closed="2025-03-20T10:00:00Z"),
        ]
        result = _insert_and_query(in_memory_db, prs=prs)
        assert len(result["pr_stats"]["merged"]) == 1
        assert len(result["pr_stats"]["open"]) == 1
        assert len(result["pr_stats"]["draft"]) == 1
        assert len(result["pr_stats"]["closed_unmerged"]) == 1

    def test_merged_pr_with_closed_at_no_double_count(self, in_memory_db):
        """PR with both merged_at and closed_at should only appear as merged."""
        pr = make_pr(14, state="MERGED",
                     merged="2025-03-10T10:00:00Z",
                     closed="2025-03-10T10:00:00Z")
        result = _insert_and_query(in_memory_db, prs=[pr])
        assert len(result["pr_stats"]["merged"]) == 1
        assert len(result["pr_stats"]["closed_unmerged"]) == 0

    def test_many_old_open_prs(self, in_memory_db):
        """30 open PRs created over the past year should all appear."""
        prs = [
            make_pr(100 + i, state="OPEN", merged="",
                    created=f"2024-{(i % 12) + 1:02d}-15T10:00:00Z",
                    head_branch=f"feature-{i}")
            for i in range(30)
        ]
        result = _insert_and_query(in_memory_db, prs=prs)
        assert len(result["pr_stats"]["open"]) == 30


# ═══════════════════════════════════════════════════════════════════════════
# Scenario Group 2: Commit–PR Association
# ═══════════════════════════════════════════════════════════════════════════


class TestCommitPRAssociation:
    """Commits should be tagged with PR numbers when detectable."""

    def test_squash_merge_tagged(self, in_memory_db):
        pr = make_pr(42, merged="2025-03-05T10:00:00Z", head_branch="add-widget")
        commit = make_commit("aaa1111", message="Add widget support (#42)",
                             date="2025-03-05T10:00:00Z")
        result = _insert_and_query(in_memory_db, prs=[pr], commits=[commit])
        assert result["commits"][0]["pr_number"] == 42

    def test_merge_commit_tagged(self, in_memory_db):
        pr = make_pr(55, merged="2025-03-05T10:00:00Z", head_branch="feature-x")
        commit = make_commit("bbb2222",
                             message="Merge pull request #55 from org/feature-x",
                             date="2025-03-05T10:00:00Z")
        result = _insert_and_query(in_memory_db, prs=[pr], commits=[commit])
        assert result["commits"][0]["pr_number"] == 55

    def test_merge_into_pattern_with_pr_from_prior_period(self, in_memory_db):
        """Merge-into pattern should match PRs from before the query period
        because _annotate_commits_with_prs now looks up all PRs in the DB."""
        # PR created and opened months ago, NOT in the query period date range
        pr = make_pr(77, state="OPEN", merged="",
                     created="2024-06-01T10:00:00Z",
                     head_branch="my-feature")
        # Commit in March referencing that branch
        commit = make_commit("ccc3333",
                             message="Merge branch 'main' into my-feature",
                             date="2025-03-10T10:00:00Z")
        result = _insert_and_query(in_memory_db, prs=[pr], commits=[commit])
        assert result["commits"][0].get("pr_number") == 77

    def test_direct_push_not_tagged(self, in_memory_db):
        commit = make_commit("ddd4444", message="hotfix: critical bug",
                             date="2025-03-10T10:00:00Z")
        result = _insert_and_query(in_memory_db, commits=[commit])
        assert "pr_number" not in result["commits"][0]

    def test_mixed_tagged_and_untagged(self, in_memory_db):
        prs = [
            make_pr(20, merged="2025-03-05T10:00:00Z"),
            make_pr(21, merged="2025-03-06T10:00:00Z"),
        ]
        commits = [
            make_commit("e01", message="feat: new thing (#20)", date="2025-03-05T10:00:00Z"),
            make_commit("e02", message="feat: another (#21)", date="2025-03-06T10:00:00Z"),
            make_commit("e03", message="fix: typo (no PR)", date="2025-03-07T10:00:00Z"),
            make_commit("e04", message="chore: cleanup", date="2025-03-08T10:00:00Z"),
        ]
        result = _insert_and_query(in_memory_db, prs=prs, commits=commits)
        tagged = [c for c in result["commits"] if "pr_number" in c]
        untagged = [c for c in result["commits"] if "pr_number" not in c]
        assert len(tagged) == 2
        assert len(untagged) == 2


# ═══════════════════════════════════════════════════════════════════════════
# Scenario Group 3: User Activity Aggregation
# ═══════════════════════════════════════════════════════════════════════════


class TestUserActivity:
    """User activity stats should correctly aggregate PRs and commits."""

    def test_user_with_merged_prs_and_commits(self, in_memory_db):
        prs = [
            make_pr(30, author="alice", merged="2025-03-10T10:00:00Z",
                    additions=100, deletions=20),
            make_pr(31, author="alice", merged="2025-03-15T10:00:00Z",
                    additions=50, deletions=10),
        ]
        commits = [
            make_commit(f"f0{i}", author="alice", date=f"2025-03-{10+i:02d}T10:00:00Z")
            for i in range(5)
        ]
        result = _insert_and_query(in_memory_db, prs=prs, commits=commits)
        ua = result["user_activity"]["alice"]
        assert ua["prs_merged"] == 2
        assert ua["commits"] == 5
        assert ua["additions"] == 150
        assert ua["deletions"] == 30

    def test_user_with_commits_but_no_prs(self, in_memory_db):
        commits = [
            make_commit(f"g0{i}", author="bob", date=f"2025-03-{10+i:02d}T10:00:00Z")
            for i in range(3)
        ]
        result = _insert_and_query(in_memory_db, commits=commits)
        ua = result["user_activity"]["bob"]
        assert ua["commits"] == 3
        assert ua["prs_merged"] == 0
        assert ua["additions"] == 0

    def test_pr_author_differs_from_commit_author(self, in_memory_db):
        pr = make_pr(40, author="alice", merged="2025-03-10T10:00:00Z",
                     additions=200, deletions=50)
        commit = make_commit("h01", author="bob", date="2025-03-10T10:00:00Z")
        result = _insert_and_query(in_memory_db, prs=[pr], commits=[commit])
        assert result["user_activity"]["alice"]["prs_merged"] == 1
        assert result["user_activity"]["alice"]["additions"] == 200
        assert result["user_activity"]["bob"]["commits"] == 1
        assert result["user_activity"]["bob"]["prs_merged"] == 0

    def test_no_author_pr_grouped_as_unknown(self, in_memory_db):
        pr = make_pr(50, author="", merged="2025-03-10T10:00:00Z")
        result = _insert_and_query(in_memory_db, prs=[pr])
        assert "unknown" in result["user_activity"]
        assert result["user_activity"]["unknown"]["prs_merged"] == 1

    def test_open_pr_not_counted_in_merged_stats(self, in_memory_db):
        """Open PRs should count as prs_opened but not add to additions/deletions."""
        pr = make_pr(60, state="OPEN", author="alice", merged="",
                     created="2025-03-10T10:00:00Z",
                     additions=500, deletions=100)
        result = _insert_and_query(in_memory_db, prs=[pr])
        ua = result["user_activity"]["alice"]
        assert ua["prs_opened"] == 1
        assert ua["prs_merged"] == 0
        assert ua["additions"] == 0  # not merged, so no line counts


# ═══════════════════════════════════════════════════════════════════════════
# Scenario Group 4: Branch Categorization
# ═══════════════════════════════════════════════════════════════════════════


class TestBranchCategorization:
    """Branches should be categorized as default, active, or stale."""

    def test_default_branch_always_appears(self, in_memory_db):
        branches = [make_branch("main")]
        result = _insert_and_query(in_memory_db, branches=branches)
        assert len(result["branch_cats"]["default"]) == 1
        assert result["branch_cats"]["default"][0]["name"] == "main"

    def test_active_branch(self, in_memory_db):
        # Last commit 3 days before period end
        recent = (MAR_END - timedelta(days=3)).strftime("%Y-%m-%dT%H:%M:%SZ")
        branches = [
            make_branch("main"),
            make_branch("feature-new", last_commit=recent),
        ]
        result = _insert_and_query(in_memory_db, branches=branches)
        active_names = [b["name"] for b in result["branch_cats"]["active"]]
        assert "feature-new" in active_names

    def test_stale_branch(self, in_memory_db):
        # Last commit 30 days before period end
        old = (MAR_END - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
        branches = [
            make_branch("main"),
            make_branch("old-branch", last_commit=old),
        ]
        result = _insert_and_query(in_memory_db, branches=branches)
        stale_names = [b["name"] for b in result["branch_cats"]["stale"]]
        assert "old-branch" in stale_names

    def test_branch_linked_to_open_pr_appears(self, in_memory_db):
        """A branch associated with an open PR should be included even if
        its last commit is outside the period."""
        pr = make_pr(70, state="OPEN", merged="",
                     created="2024-12-01T10:00:00Z",
                     head_branch="long-running")
        # Branch last commit is very old, but PR is open
        branches = [
            make_branch("main"),
            make_branch("long-running", last_commit="2024-12-01T10:00:00Z"),
        ]
        result = _insert_and_query(in_memory_db, prs=[pr], branches=branches)
        all_branch_names = [
            b["name"]
            for cat in result["branch_cats"].values()
            for b in cat
        ]
        assert "long-running" in all_branch_names

    def test_stale_boundary_exact(self, in_memory_db):
        """Branch with last commit exactly stale_branch_days ago should be
        categorized correctly (> means strictly greater, so exactly 14 is active)."""
        exact = (MAR_END - timedelta(days=14)).strftime("%Y-%m-%dT%H:%M:%SZ")
        branches = [
            make_branch("main"),
            make_branch("borderline", last_commit=exact),
        ]
        result = _insert_and_query(in_memory_db, branches=branches,
                                   stale_branch_days=14)
        active_names = [b["name"] for b in result["branch_cats"]["active"]]
        stale_names = [b["name"] for b in result["branch_cats"]["stale"]]
        # age=14, threshold=14 → 14 > 14 is False → should be active
        assert "borderline" in active_names
        assert "borderline" not in stale_names


# ═══════════════════════════════════════════════════════════════════════════
# Scenario Group 5: Time Boundaries
# ═══════════════════════════════════════════════════════════════════════════


class TestTimeBoundaries:
    """Verify inclusive-start, exclusive-end boundary behavior."""

    def test_pr_merged_at_exact_period_start(self, in_memory_db):
        pr = make_pr(80, merged="2025-03-01T00:00:00Z")
        result = _insert_and_query(in_memory_db, prs=[pr])
        assert len(result["pr_stats"]["merged"]) == 1

    def test_pr_merged_at_exact_period_end(self, in_memory_db):
        """Period end is exclusive — PR merged at exactly end should NOT appear."""
        pr = make_pr(81, merged="2025-04-01T00:00:00Z")
        result = _insert_and_query(in_memory_db, prs=[pr])
        assert len(result["pr_stats"]["merged"]) == 0

    def test_commit_at_exact_period_start(self, in_memory_db):
        commit = make_commit("t01", date="2025-03-01T00:00:00Z")
        result = _insert_and_query(in_memory_db, commits=[commit])
        assert len(result["commits"]) == 1

    def test_commit_at_exact_period_end(self, in_memory_db):
        """Period end is exclusive — commit at exactly end should NOT appear."""
        commit = make_commit("t02", date="2025-04-01T00:00:00Z")
        result = _insert_and_query(in_memory_db, commits=[commit])
        assert len(result["commits"]) == 0

    def test_empty_period(self, in_memory_db):
        """Query with no data should return empty structures, not errors."""
        result = _insert_and_query(in_memory_db)
        assert result["pr_stats"]["merged"] == []
        assert result["pr_stats"]["open"] == []
        assert result["pr_stats"]["draft"] == []
        assert result["pr_stats"]["closed_unmerged"] == []
        assert result["commits"] == []
        assert result["user_activity"] == {}


# ═══════════════════════════════════════════════════════════════════════════
# Scenario Group 6: Prompt Fidelity
# ═══════════════════════════════════════════════════════════════════════════


class TestPromptFidelity:
    """The AI prompt should accurately reflect the underlying data."""

    def test_mixed_pr_states_in_prompt(self, in_memory_db):
        prs = [
            make_pr(90, merged="2025-03-05T10:00:00Z", title="Ship feature A"),
            make_pr(91, merged="2025-03-06T10:00:00Z", title="Ship feature B"),
            make_pr(92, state="OPEN", merged="", title="WIP: feature C",
                    created="2025-03-10T10:00:00Z"),
            make_pr(93, state="OPEN", merged="", title="WIP: feature D",
                    created="2025-03-11T10:00:00Z"),
            make_pr(94, state="OPEN", merged="", title="WIP: feature E",
                    created="2025-03-12T10:00:00Z"),
            make_pr(95, state="OPEN", is_draft=True, merged="",
                    title="Draft: spike", created="2025-03-13T10:00:00Z"),
        ]
        result = _insert_and_query(in_memory_db, prs=prs)
        prompt = build_prompt_period(REPO, "March 2025", result)
        assert "MERGED PRs (2)" in prompt
        assert "OPEN / IN REVIEW (3)" in prompt
        assert "DRAFT PRs (1)" in prompt

    def test_commit_pr_tags_in_prompt(self, in_memory_db):
        pr = make_pr(42, merged="2025-03-05T10:00:00Z")
        commits = [
            make_commit("p01", message="feat: widget (#42)",
                        date="2025-03-05T10:00:00Z"),
            make_commit("p02", message="direct push to main",
                        date="2025-03-06T10:00:00Z"),
        ]
        result = _insert_and_query(in_memory_db, prs=[pr], commits=commits)
        prompt = build_prompt_period(REPO, "March 2025", result)
        assert "[PR #42]" in prompt
        # The untagged commit line should NOT have a PR tag
        lines = prompt.split("\n")
        commit_line = [l for l in lines if "direct push to main" in l and "p02" in l]
        assert len(commit_line) == 1
        assert "[PR #" not in commit_line[0]

    def test_old_open_prs_shown_in_prompt(self, in_memory_db):
        """Open PRs from before the period should still appear in the prompt."""
        prs = [
            make_pr(200 + i, state="OPEN", merged="",
                    created=f"2024-{(i % 12) + 1:02d}-15T10:00:00Z",
                    title=f"Old PR {i}", head_branch=f"old-{i}")
            for i in range(5)
        ]
        result = _insert_and_query(in_memory_db, prs=prs)
        prompt = build_prompt_period(REPO, "March 2025", result)
        assert "OPEN / IN REVIEW (5)" in prompt
        assert "MERGED PRs (0)" in prompt

    def test_empty_period_prompt(self, in_memory_db):
        """An empty period should produce a valid prompt with (none) placeholders."""
        result = _insert_and_query(in_memory_db)
        prompt = build_prompt_period(REPO, "March 2025", result)
        assert "MERGED PRs (0)" in prompt
        assert "(none)" in prompt
