"""SQLite schema, migrations, connection, and query helpers."""

from __future__ import annotations

import contextlib
import json
import logging
import re
import sqlite3
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .config import Config

logger = logging.getLogger(__name__)

# Rough chars-per-token estimate for truncation budget
CHARS_PER_TOKEN = 4

SCHEMA = """
CREATE TABLE IF NOT EXISTS repos (
    repo           TEXT PRIMARY KEY,
    default_branch TEXT NOT NULL DEFAULT 'main',
    last_sync      TEXT,
    last_diff_sync TEXT
);

CREATE TABLE IF NOT EXISTS prs (
    repo             TEXT NOT NULL,
    number           INTEGER NOT NULL,
    title            TEXT,
    body             TEXT,
    state            TEXT,
    author           TEXT,
    created_at       TEXT,
    updated_at       TEXT,
    merged_at        TEXT,
    closed_at        TEXT,
    base_branch      TEXT,
    head_branch      TEXT,
    additions        INTEGER DEFAULT 0,
    deletions        INTEGER DEFAULT 0,
    comment_count    INTEGER DEFAULT 0,
    review_count     INTEGER DEFAULT 0,
    review_decision  TEXT,
    labels           TEXT,
    milestone        TEXT,
    is_draft         INTEGER DEFAULT 0,
    PRIMARY KEY (repo, number)
);

CREATE TABLE IF NOT EXISTS pr_files (
    repo      TEXT NOT NULL,
    pr_number INTEGER NOT NULL,
    filename  TEXT NOT NULL,
    status    TEXT,
    additions INTEGER DEFAULT 0,
    deletions INTEGER DEFAULT 0,
    PRIMARY KEY (repo, pr_number, filename)
);

CREATE TABLE IF NOT EXISTS pr_diffs (
    repo      TEXT NOT NULL,
    pr_number INTEGER NOT NULL,
    diff_text TEXT,
    fetched_at TEXT,
    PRIMARY KEY (repo, pr_number)
);

CREATE TABLE IF NOT EXISTS commits (
    repo         TEXT NOT NULL,
    sha          TEXT NOT NULL,
    author       TEXT,
    committed_at TEXT,
    message      TEXT,
    PRIMARY KEY (repo, sha)
);

CREATE TABLE IF NOT EXISTS branches (
    repo           TEXT NOT NULL,
    name           TEXT NOT NULL,
    last_commit_at TEXT,
    last_author    TEXT,
    is_default     INTEGER DEFAULT 0,
    synced_at      TEXT,
    PRIMARY KEY (repo, name)
);

CREATE INDEX IF NOT EXISTS idx_prs_dates    ON prs(repo, created_at, merged_at, updated_at);
CREATE INDEX IF NOT EXISTS idx_commits_date ON commits(repo, committed_at);
CREATE INDEX IF NOT EXISTS idx_pr_files     ON pr_files(repo, pr_number);
"""

MIGRATIONS = [
    "ALTER TABLE prs ADD COLUMN body TEXT",
    "ALTER TABLE prs ADD COLUMN comment_count INTEGER DEFAULT 0",
    "ALTER TABLE prs ADD COLUMN review_count INTEGER DEFAULT 0",
    "ALTER TABLE prs ADD COLUMN review_decision TEXT",
    "ALTER TABLE prs ADD COLUMN labels TEXT",
    "ALTER TABLE prs ADD COLUMN milestone TEXT",
    "ALTER TABLE repos ADD COLUMN last_diff_sync TEXT",
    "CREATE INDEX IF NOT EXISTS idx_prs_dates_v2 ON prs(repo, created_at, merged_at, closed_at)",
    "ALTER TABLE prs ADD COLUMN is_draft INTEGER DEFAULT 0",
]


def utc_now_str() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_dt(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        logger.debug("Could not parse datetime: %r", s)
        return None


def fmt_date(s: str) -> str:
    dt = parse_dt(s)
    return dt.strftime("%b %d") if dt else ""


def db_connect(cfg: Config | None = None) -> sqlite3.Connection:
    path = Path(cfg.database.path) if cfg else Path("gitreport.db")
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    con.executescript(SCHEMA)
    for sql in MIGRATIONS:
        with contextlib.suppress(sqlite3.OperationalError):
            con.execute(sql)
    con.commit()
    return con


# ── Write helpers ──────────────────────────────────────────────────────────

def db_upsert_prs(con: sqlite3.Connection, repo: str, prs: list[dict]) -> None:
    con.executemany("""
        INSERT INTO prs VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(repo,number) DO UPDATE SET
            title=excluded.title, body=excluded.body,
            state=excluded.state, updated_at=excluded.updated_at,
            merged_at=excluded.merged_at, closed_at=excluded.closed_at,
            additions=excluded.additions, deletions=excluded.deletions,
            comment_count=excluded.comment_count,
            review_count=excluded.review_count,
            review_decision=excluded.review_decision,
            labels=excluded.labels, milestone=excluded.milestone,
            is_draft=excluded.is_draft
    """, [(
        repo,
        pr["number"],
        pr.get("title", ""),
        pr.get("body", "") or "",
        pr.get("state", ""),
        (pr.get("author") or {}).get("login", ""),
        pr.get("createdAt", ""),
        pr.get("updatedAt", ""),
        pr.get("mergedAt") or "",
        pr.get("closedAt") or "",
        pr.get("baseRefName", ""),
        pr.get("headRefName", ""),
        pr.get("additions", 0),
        pr.get("deletions", 0),
        pr.get("comments", 0),
        len(pr.get("reviews") or []),
        (pr.get("reviewDecision") or ""),
        json.dumps([lbl["name"] for lbl in (pr.get("labels") or [])]),
        (pr.get("milestone") or {}).get("title", "") if pr.get("milestone") else "",
        1 if pr.get("isDraft") else 0,
    ) for pr in prs])


def db_upsert_pr_files(con: sqlite3.Connection, repo: str, pr_number: int, files: list[dict]) -> None:
    con.executemany("""
        INSERT INTO pr_files VALUES(?,?,?,?,?,?)
        ON CONFLICT(repo,pr_number,filename) DO UPDATE SET
            status=excluded.status,
            additions=excluded.additions,
            deletions=excluded.deletions
    """, [(repo, pr_number, f.get("path", ""), f.get("status", ""),
           int(f.get("additions") or 0), int(f.get("deletions") or 0)) for f in files])


def db_upsert_diff(con: sqlite3.Connection, repo: str, pr_number: int, diff_text: str) -> None:
    now = utc_now_str()
    con.execute("""
        INSERT INTO pr_diffs VALUES(?,?,?,?)
        ON CONFLICT(repo,pr_number) DO UPDATE SET
            diff_text=excluded.diff_text, fetched_at=excluded.fetched_at
    """, (repo, pr_number, diff_text, now))


def db_upsert_commits(con: sqlite3.Connection, repo: str, commits: list[dict]) -> None:
    con.executemany("""
        INSERT INTO commits(repo,sha,author,committed_at,message) VALUES(?,?,?,?,?)
        ON CONFLICT(repo,sha) DO UPDATE SET
            author=excluded.author,
            committed_at=excluded.committed_at,
            message=excluded.message
    """, [(
        repo,
        c.get("abbreviatedOid", ""),
        ((c.get("authors") or [{}])[0]).get("login",
            ((c.get("authors") or [{}])[0]).get("name", "")),
        c.get("committedDate", ""),
        c.get("body") or c.get("message") or c.get("messageHeadline", ""),
    ) for c in commits])


def db_upsert_branches(con: sqlite3.Connection, repo: str, branches: list[dict], default_branch: str) -> None:
    now = utc_now_str()
    con.executemany("""
        INSERT INTO branches VALUES(?,?,?,?,?,?)
        ON CONFLICT(repo,name) DO UPDATE SET
            last_commit_at=excluded.last_commit_at,
            last_author=excluded.last_author,
            is_default=excluded.is_default,
            synced_at=excluded.synced_at
    """, [(
        repo, b["name"], b.get("last_commit", ""), b.get("last_author", ""),
        1 if b["name"] == default_branch else 0, now,
    ) for b in branches])


def db_update_sync(con: sqlite3.Connection, repo: str, default_branch: str, diff_sync: bool = False) -> None:
    now = utc_now_str()
    con.execute("""
        INSERT INTO repos(repo, default_branch, last_sync) VALUES(?,?,?)
        ON CONFLICT(repo) DO UPDATE SET
            default_branch=excluded.default_branch,
            last_sync=excluded.last_sync
    """, (repo, default_branch, now))
    if diff_sync:
        con.execute("UPDATE repos SET last_diff_sync=? WHERE repo=?", (now, repo))


def db_last_sync(con: sqlite3.Connection, repo: str) -> tuple[str | None, str | None]:
    row = con.execute("SELECT last_sync, last_diff_sync FROM repos WHERE repo=?", (repo,)).fetchone()
    return (row["last_sync"], row["last_diff_sync"]) if row else (None, None)


# ── Period slicing ─────────────────────────────────────────────────────────

def build_periods(
    period: str,
    date_from: datetime,
    date_to: datetime,
) -> list[tuple[str, datetime, datetime]]:
    if period == "none":
        return [("Full Window", date_from, date_to)]
    periods: list[tuple[str, datetime, datetime]] = []
    cursor = date_from
    while cursor < date_to:
        if period == "weekly":
            end = cursor + timedelta(weeks=1)
            label = f"Week of {cursor.strftime('%b %d, %Y')}"
        elif period == "biweekly":
            end = cursor + timedelta(weeks=2)
            label = f"Sprint {cursor.strftime('%b %d')}–{min(end, date_to).strftime('%b %d, %Y')}"
        elif period == "monthly":
            end = (cursor.replace(month=cursor.month % 12 + 1, day=1)
                   if cursor.month < 12
                   else cursor.replace(year=cursor.year + 1, month=1, day=1))
            label = cursor.strftime("%B %Y")
        else:
            end, label = date_to, "Full Window"
        periods.append((label, cursor, min(end, date_to)))
        cursor = min(end, date_to)
    return periods


# ── Commit–PR association ──────────────────────────────────────────────────

_MERGE_PR_RE = re.compile(r"^Merge pull request #(\d+) from \S+")
_SQUASH_RE = re.compile(r"\(#(\d+)\)\s*$")
_MERGE_INTO_RE = re.compile(r"^Merge branch '.+' into (.+)$")


def _annotate_commits_with_prs(
    commits: list[dict], prs_raw: list[sqlite3.Row],
    con: sqlite3.Connection | None = None, repo: str = "",
) -> list[dict]:
    """Tag each commit with its associated PR number when detectable."""
    branch_to_pr: dict[str, int] = {}
    if con and repo:
        # Use ALL PRs in the DB for branch→PR mapping so that commits from
        # PRs created before the reporting period can still be matched.
        for row in con.execute(
            "SELECT number, head_branch FROM prs WHERE repo=? AND head_branch IS NOT NULL",
            (repo,),
        ):
            branch_to_pr[row["head_branch"]] = row["number"]
    else:
        for pr in prs_raw:
            if pr["head_branch"]:
                branch_to_pr[pr["head_branch"]] = pr["number"]

    for c in commits:
        msg = (c.get("message") or "").split("\n")[0]
        pr_num: int | None = None

        m = _MERGE_PR_RE.match(msg)
        if m:
            pr_num = int(m.group(1))
        if not pr_num:
            m = _SQUASH_RE.search(msg)
            if m:
                pr_num = int(m.group(1))
        if not pr_num:
            m = _MERGE_INTO_RE.match(msg)
            if m and m.group(1) in branch_to_pr:
                pr_num = branch_to_pr[m.group(1)]

        if pr_num:
            c["pr_number"] = pr_num
    return commits


# ── Query helpers ──────────────────────────────────────────────────────────

def query_period(
    con: sqlite3.Connection,
    repo: str,
    start_dt: datetime,
    end_dt: datetime,
    include_diffs: bool = False,
    max_diff_tokens: int = 8_000,
    stale_branch_days: int = 14,
) -> dict:
    s = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    e = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    prs_raw = con.execute("""
        SELECT * FROM prs WHERE repo=? AND (
            (created_at >= ? AND created_at < ?) OR
            (merged_at  >= ? AND merged_at  < ?) OR
            (closed_at  >= ? AND closed_at  < ?) OR
            state = 'OPEN'
        ) ORDER BY COALESCE(merged_at, created_at) DESC
    """, (repo, s, e, s, e, s, e)).fetchall()

    commits_raw = con.execute("""
        SELECT * FROM commits WHERE repo=?
        AND committed_at >= ? AND committed_at < ?
        ORDER BY committed_at DESC
    """, (repo, s, e)).fetchall()

    branches_raw = con.execute("SELECT * FROM branches WHERE repo=?", (repo,)).fetchall()

    active_branch_names = {pr["head_branch"] for pr in prs_raw if pr["head_branch"]}

    period_branches: list[dict] = []
    for b in [dict(r) for r in branches_raw]:
        in_period = b.get("last_commit_at") and b["last_commit_at"] >= s and b["last_commit_at"] < e
        if b["is_default"] or b["name"] in active_branch_names or in_period:
            period_branches.append(b)

    # Enrich PRs with file stats
    pr_numbers = [r["number"] for r in prs_raw]
    files_by_pr: dict[int, list[dict]] = defaultdict(list)
    if pr_numbers:
        placeholders = ",".join("?" * len(pr_numbers))
        all_files = con.execute(f"""
            SELECT pr_number, filename, additions, deletions, status
            FROM pr_files WHERE repo=? AND pr_number IN ({placeholders})
            ORDER BY (additions+deletions) DESC
        """, [repo] + pr_numbers).fetchall()
        for f in all_files:
            files_by_pr[f["pr_number"]].append(
                {**dict(f), "additions": int(f["additions"] or 0), "deletions": int(f["deletions"] or 0)})

    pr_list: list[dict] = []
    for pr in prs_raw:
        pr = dict(pr)
        pr["files"] = files_by_pr.get(pr["number"], [])
        pr["labels"] = json.loads(pr.get("labels") or "[]")
        pr_list.append(pr)

    # Categorize
    pr_stats: dict[str, list[dict]] = {"merged": [], "open": [], "draft": [], "closed_unmerged": []}
    for pr in pr_list:
        merged_in_period = pr.get("merged_at") and pr["merged_at"] >= s and pr["merged_at"] < e
        closed_in_period = pr.get("closed_at") and pr["closed_at"] >= s and pr["closed_at"] < e

        if merged_in_period:
            pr_stats["merged"].append(pr)
        elif closed_in_period and not pr.get("merged_at"):
            pr_stats["closed_unmerged"].append(pr)
        elif pr.get("is_draft"):
            pr_stats["draft"].append(pr)
        else:
            pr_stats["open"].append(pr)

    # Per-user activity
    ua: dict[str, dict] = defaultdict(lambda: {"commits": 0, "prs_opened": 0, "prs_merged": 0,
                                                "additions": 0, "deletions": 0})
    for pr in pr_list:
        u = pr.get("author") or "unknown"
        if pr.get("created_at") and pr["created_at"] >= s and pr["created_at"] < e:
            ua[u]["prs_opened"] += 1
        merged_in_pd = pr.get("merged_at") and pr["merged_at"] >= s and pr["merged_at"] < e
        if merged_in_pd:
            ua[u]["prs_merged"] += 1
        if merged_in_pd:
            ua[u]["additions"] += pr.get("additions") or 0
            ua[u]["deletions"] += pr.get("deletions") or 0
    for c in commits_raw:
        ua[c["author"] or "unknown"]["commits"] += 1

    # Branch categorization
    bc: dict[str, list[dict]] = {"active": [], "stale": [], "default": []}
    for b in period_branches:
        if b["is_default"]:
            bc["default"].append(b)
            continue
        last = parse_dt(b.get("last_commit_at"))
        if not last:
            bc["stale"].append(b)
            continue
        age = (end_dt - last).days
        b["age_days"] = age
        (bc["stale"] if age > stale_branch_days else bc["active"]).append(b)

    # Diffs
    diffs_by_pr: dict[int, str] = {}
    if include_diffs:
        budget_chars = max_diff_tokens * CHARS_PER_TOKEN
        ordered_prs = pr_stats["merged"] + pr_stats["open"] + pr_stats["draft"] + pr_stats["closed_unmerged"]
        for pr in ordered_prs:
            if budget_chars <= 0:
                break
            row = con.execute(
                "SELECT diff_text FROM pr_diffs WHERE repo=? AND pr_number=?",
                (repo, pr["number"])
            ).fetchone()
            if not row or not row["diff_text"]:
                continue
            diff = row["diff_text"]
            allowed = min(len(diff), budget_chars)
            diffs_by_pr[pr["number"]] = diff[:allowed] + ("…[truncated]" if allowed < len(diff) else "")
            budget_chars -= allowed

    # Annotate commits with PR context
    commits = [dict(c) for c in commits_raw]
    commits = _annotate_commits_with_prs(commits, prs_raw, con=con, repo=repo)

    return {
        "pr_stats": pr_stats,
        "user_activity": dict(ua),
        "branch_cats": bc,
        "commits": commits,
        "diffs_by_pr": diffs_by_pr,
    }
