"""Sync and status commands — pull data from GitHub into local DB."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from .db import (
    db_connect,
    db_last_sync,
    db_update_sync,
    db_upsert_branches,
    db_upsert_commits,
    db_upsert_diff,
    db_upsert_pr_files,
    db_upsert_prs,
)
from .github import check_gh, fetch_prs_graphql, gh, gh_text, parse_rest_commit

if TYPE_CHECKING:
    import argparse
    import sqlite3

    from .config import Config


def _print_db_status(con: sqlite3.Connection, repo: str) -> None:
    pr_n = con.execute("SELECT COUNT(*) FROM prs WHERE repo=?", (repo,)).fetchone()[0]
    cm_n = con.execute("SELECT COUNT(*) FROM commits WHERE repo=?", (repo,)).fetchone()[0]
    br_n = con.execute("SELECT COUNT(*) FROM branches WHERE repo=?", (repo,)).fetchone()[0]
    fi_n = con.execute("SELECT COUNT(DISTINCT pr_number) FROM pr_files WHERE repo=?", (repo,)).fetchone()[0]
    di_n = con.execute("SELECT COUNT(*) FROM pr_diffs WHERE repo=?", (repo,)).fetchone()[0]
    oldest_pr = (con.execute("SELECT MIN(created_at) FROM prs WHERE repo=?", (repo,)).fetchone()[0] or "")[:10]
    oldest_cm = (con.execute("SELECT MIN(committed_at) FROM commits WHERE repo=?", (repo,)).fetchone()[0] or "")[:10]
    print(f"  PRs: {pr_n} (oldest: {oldest_pr}) | Commits: {cm_n} (oldest: {oldest_cm})")
    print(f"  Branches: {br_n} | PRs with file stats: {fi_n} | PRs with diffs: {di_n}")


def cmd_sync(args: argparse.Namespace, cfg: Config) -> None:
    check_gh()
    repo = args.repo
    with_diffs = args.with_diffs
    con = db_connect(cfg)
    try:
        _do_sync(con, args, repo, with_diffs, cfg)
    finally:
        con.close()


def _do_sync(con: sqlite3.Connection, args: argparse.Namespace, repo: str, with_diffs: bool, cfg: Config) -> None:
    db_path = Path(cfg.database.path)

    # Quick path: only backfill commits
    if args.backfill_commits:
        print(f"\nBackfilling all commits for {repo}...")
        commits_path = f"repos/{repo}/commits?per_page=100"
        raw_commits = gh(["api", commits_path, "--paginate"]) or []
        commits = [parse_rest_commit(c) for c in raw_commits]
        print(f"    {len(commits)} commits fetched")
        db_upsert_commits(con, repo, commits)
        con.commit()
        print(f"Backfill complete → {db_path}")
        _print_db_status(con, repo)
        return

    last_sync, last_diff_sync = db_last_sync(con, repo)

    if last_sync and not args.full:
        print(f"Incremental sync (last: {last_sync})  —  use --full to re-fetch everything")
        since = last_sync
    else:
        since = None
        if args.full and last_sync:
            print("Full sync — clearing existing data...")
            for t in ("prs", "commits", "branches", "pr_files", "pr_diffs"):
                con.execute(f"DELETE FROM {t} WHERE repo=?", (repo,))
            con.commit()

    print(f"\nSyncing {repo}...")

    # Repo info
    info = gh(["repo", "view", repo, "--json", "defaultBranchRef"]) or {}
    default_branch = (info.get("defaultBranchRef") or {}).get("name", "main")

    # PRs
    print("  Fetching pull requests (batched GraphQL)...")
    all_prs = fetch_prs_graphql(repo, since)
    prs = [p for p in all_prs if not since or (p.get("updatedAt", "") >= since)]
    print(f"    {len(prs)} PRs fetched (of {len(all_prs)} total)")
    db_upsert_prs(con, repo, prs)

    # Per-PR file stats
    existing_files = {r[0] for r in con.execute(
        "SELECT DISTINCT pr_number FROM pr_files WHERE repo=?", (repo,)).fetchall()}
    prs_needing_files = [p for p in prs if p["number"] not in existing_files or args.full]
    if prs_needing_files:
        print(f"  Fetching file stats for {len(prs_needing_files)} PRs...")
        for i, pr in enumerate(prs_needing_files):
            num = pr["number"]
            files = gh(["api", f"repos/{repo}/pulls/{num}/files", "--paginate", "--jq",
                        "[.[] | {path:.filename, additions:.additions, deletions:.deletions, status:.status}]"])
            if files:
                db_upsert_pr_files(con, repo, num, files)
            if (i + 1) % 20 == 0:
                print(f"    {i+1}/{len(prs_needing_files)} done...")

    # Full diffs (optional)
    if with_diffs:
        existing_diffs = {r[0] for r in con.execute(
            "SELECT pr_number FROM pr_diffs WHERE repo=?", (repo,)).fetchall()}
        prs_needing_diffs = [p for p in all_prs if p["number"] not in existing_diffs or args.full]
        print(f"  Fetching full diffs for {len(prs_needing_diffs)} PRs...")
        for i, pr in enumerate(prs_needing_diffs):
            num = pr["number"]
            diff = gh_text(["pr", "diff", str(num), "--repo", repo])
            db_upsert_diff(con, repo, num, diff)
            if (i + 1) % 10 == 0:
                print(f"    {i+1}/{len(prs_needing_diffs)} diffs done...")

    # Commits
    print("  Fetching commits...")
    commits_path = f"repos/{repo}/commits?per_page=100" + (f"&since={since}" if since else "")
    raw_commits = gh(["api", commits_path, "--paginate"]) or []
    commits = [parse_rest_commit(c) for c in raw_commits]
    print(f"    {len(commits)} commits")
    db_upsert_commits(con, repo, commits)

    # Branches
    print("  Fetching branches...")
    branch_data = gh(["api", f"repos/{repo}/branches", "--paginate",
                       "--jq", '[.[] | {name: .name, sha: .commit.sha}]']) or []
    branches: list[dict] = []
    if branch_data:
        unique_shas = list({b["sha"] for b in branch_data[:80]})
        sha_info: dict[str, dict] = {}
        for sha in unique_shas:
            info = gh(["api", f"repos/{repo}/commits/{sha}",
                       "--jq", '{date: .commit.committer.date, author: .commit.author.name}'], check=False)
            if info:
                sha_info[sha] = info
        for b in branch_data[:80]:
            ci = sha_info.get(b["sha"], {})
            branches.append({
                "name": b["name"],
                "last_commit": ci.get("date", ""),
                "last_author": ci.get("author", ""),
            })
    print(f"    {len(branches)} branches")
    db_upsert_branches(con, repo, branches, default_branch)

    db_update_sync(con, repo, default_branch, diff_sync=with_diffs)
    con.commit()
    print(f"\nSync complete → {db_path}")
    _print_db_status(con, repo)


def cmd_status(args: argparse.Namespace, cfg: Config) -> None:
    db_path = Path(cfg.database.path)
    if not db_path.exists():
        print(f"No DB at {db_path}. Run: gitreport sync --repo owner/repo")
        return
    con = db_connect(cfg)
    try:
        rows = con.execute("SELECT repo, default_branch, last_sync, last_diff_sync FROM repos ORDER BY repo").fetchall()
        if not rows:
            print("DB exists but no repos synced yet.")
            return
        for row in rows:
            print(f"\n{row['repo']}  (default: {row['default_branch']})")
            print(f"  Last sync:      {row['last_sync']}")
            print(f"  Last diff sync: {row['last_diff_sync'] or '(never — run sync --with-diffs)'}")
            _print_db_status(con, row["repo"])
    finally:
        con.close()
