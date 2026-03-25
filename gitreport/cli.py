"""CLI entry point: argparse setup and command dispatch."""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

from . import __version__
from .config import Config, load_config

logger = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="gitreport",
        description="GitHub repo analytics with local SQLite cache and AI summaries",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    parser.add_argument("-q", "--quiet", action="store_true", help="Suppress progress messages")

    sub = parser.add_subparsers(dest="command", required=True)

    # ── sync ──
    p_sync = sub.add_parser("sync", help="Fetch/update GitHub data into local DB")
    p_sync.add_argument("--repo", required=True, help="owner/repo")
    p_sync.add_argument("--full", action="store_true", help="Clear and re-fetch everything")
    p_sync.add_argument("--with-diffs", action="store_true", help="Also fetch and store full PR diff text")
    p_sync.add_argument("--backfill-commits", action="store_true",
                        help="Fetch ALL commits without re-fetching PRs/branches")

    # ── report ──
    p_rep = sub.add_parser("report", help="Generate HTML report from cached data")
    p_rep.add_argument("--repo", required=True)
    p_rep.add_argument("--period", default="none",
                       choices=["none", "weekly", "biweekly", "monthly"])
    p_rep.add_argument("--days", type=int, default=None, help="Lookback in days (default: 30)")
    p_rep.add_argument("--from", dest="date_from", help="Start date YYYY-MM-DD")
    p_rep.add_argument("--to", dest="date_to", help="End date YYYY-MM-DD")
    p_rep.add_argument("--output", default=None)
    p_rep.add_argument("--no-summary", action="store_true", help="Skip AI summaries (stats only)")
    p_rep.add_argument("--provider", default=None,
                       choices=["auto", "claude", "codex", "ollama", "none"],
                       help="AI provider for summaries")
    p_rep.add_argument("--deep", action="store_true",
                       help="Include full diff content in prompts (requires sync --with-diffs)")
    p_rep.add_argument("--max-diff-tokens", type=int, default=None,
                       help="Token budget for diff content per period (default: 8000)")
    p_rep.add_argument("--dump-prompts", action="store_true",
                       help="Print the prompts that would be sent to the AI provider and exit")

    # ── status ──
    sub.add_parser("status", help="Show what's in the local DB")

    # ── serve ──
    p_serve = sub.add_parser("serve", help="Serve reports via local HTTP server")
    p_serve.add_argument("--port", type=int, default=None)
    p_serve.add_argument("--host", default=None)
    p_serve.add_argument("--dir", default=".", help="Directory to serve reports from")

    args = parser.parse_args(argv)

    log_level = logging.DEBUG if args.verbose else logging.WARNING if args.quiet else logging.INFO
    logging.basicConfig(level=log_level, format="%(message)s", stream=sys.stderr)

    # Build CLI overrides dict
    cli_overrides: dict = {}
    if hasattr(args, "provider") and args.provider is not None:
        cli_overrides["provider"] = args.provider
    if hasattr(args, "output") and args.output is not None:
        cli_overrides["output"] = args.output
    if hasattr(args, "max_diff_tokens") and args.max_diff_tokens is not None:
        cli_overrides["max_diff_tokens"] = args.max_diff_tokens
    if hasattr(args, "days") and args.days is not None:
        cli_overrides["days"] = args.days
    if hasattr(args, "port") and args.port is not None:
        cli_overrides["port"] = args.port
    if hasattr(args, "host") and args.host is not None:
        cli_overrides["host"] = args.host
    if hasattr(args, "no_summary") and args.no_summary:
        cli_overrides["provider"] = "none"

    cfg = load_config(cli_overrides)

    dispatch = {
        "sync": _cmd_sync,
        "report": _cmd_report,
        "status": _cmd_status,
        "serve": _cmd_serve,
    }
    dispatch[args.command](args, cfg)


def _cmd_sync(args: argparse.Namespace, cfg: Config) -> None:
    from .sync import cmd_sync
    cmd_sync(args, cfg)


def _cmd_status(args: argparse.Namespace, cfg: Config) -> None:
    from .sync import cmd_status
    cmd_status(args, cfg)


def _cmd_serve(args: argparse.Namespace, cfg: Config) -> None:
    from .server import cmd_serve
    cmd_serve(args, cfg)


def _cmd_report(args: argparse.Namespace, cfg: Config) -> None:
    from .ai import build_prompt_overall, build_prompt_period, get_provider
    from .db import build_periods, db_connect, query_period
    from .render import render_report

    db_path = Path(cfg.database.path)
    if not db_path.exists():
        logger.error("No DB at %s. Run sync first.", db_path)
        sys.exit(1)

    deep = args.deep
    max_diff_tokens = cfg.report.max_diff_tokens
    stale_branch_days = cfg.report.stale_branch_days

    # Resolve provider
    provider = get_provider(cfg)
    no_summary = provider.name == "none"

    if deep:
        con_check = db_connect(cfg)
        diff_count = con_check.execute(
            "SELECT COUNT(*) FROM pr_diffs WHERE repo=?", (args.repo,)
        ).fetchone()[0]
        con_check.close()
        if diff_count == 0:
            logger.warning("--deep requested but no diffs in DB. Run: sync --with-diffs")

    con = db_connect(cfg)
    try:
        repo = args.repo
        row = con.execute("SELECT default_branch, last_sync FROM repos WHERE repo=?", (repo,)).fetchone()
        if not row:
            logger.error("Repo '%s' not found in DB. Run: sync --repo %s", repo, repo)
            sys.exit(1)

        default_branch = row["default_branch"]
        logger.info("Generating report for %s  (last sync: %s)%s%s",
                    repo, row['last_sync'],
                    "  [DEEP]" if deep else "",
                    f"  [provider: {provider.name}]" if not no_summary else "")

        # Date range
        if args.date_from or args.date_to:
            period = args.period
            try:
                date_from = (datetime.strptime(args.date_from, "%Y-%m-%d").replace(tzinfo=UTC)
                             if args.date_from else datetime.now(UTC) - timedelta(days=90))
                date_to = (datetime.strptime(args.date_to, "%Y-%m-%d").replace(tzinfo=UTC)
                           if args.date_to else datetime.now(UTC))
            except ValueError:
                logger.error("Date format must be YYYY-MM-DD")
                sys.exit(1)
        else:
            period = args.period
            date_to = datetime.now(UTC)
            days = cfg.report.default_days
            date_from = date_to - timedelta(days=days)

        periods = build_periods(period, date_from, date_to)
        dump_prompts = args.dump_prompts

        if dump_prompts:
            separator = "=" * 72
            for i, (label, start, end) in enumerate(periods):
                pd = query_period(con, repo, start, end,
                                  include_diffs=deep,
                                  max_diff_tokens=max_diff_tokens,
                                  stale_branch_days=stale_branch_days)
                prompt = build_prompt_period(repo, label, pd, deep=deep, cfg=cfg,
                                            stale_branch_days=stale_branch_days)
                print(f"{separator}")
                print(f"PERIOD PROMPT [{i + 1}/{len(periods)}]: {label}")
                print(f"Characters: {len(prompt):,}")
                print(f"{separator}\n")
                print(prompt)
                print()

            if len(periods) > 1:
                full_pd = query_period(con, repo, date_from, date_to,
                                       include_diffs=False,
                                       stale_branch_days=stale_branch_days)
                window = f"{date_from.strftime('%b %d')} – {date_to.strftime('%b %d, %Y')}"
                prompt = build_prompt_overall(repo, window, full_pd, cfg=cfg)
                print(f"{separator}")
                print(f"OVERALL PROMPT")
                print(f"Characters: {len(prompt):,}")
                print(f"{separator}\n")
                print(prompt)
                print()
            return

        logger.info("  Breakdown: %s  →  %d period(s)", period, len(periods))

        periods_out: list[tuple[str, str, dict, str]] = []
        for i, (label, start, end) in enumerate(periods):
            pid = f"p{i}"
            logger.info("  [%d/%d] %s", i + 1, len(periods), label)
            pd = query_period(con, repo, start, end,
                              include_diffs=deep,
                              max_diff_tokens=max_diff_tokens,
                              stale_branch_days=stale_branch_days)
            summary = ""
            if not no_summary:
                logger.info("    → %s summary...", provider.name)
                result = provider.summarize(
                    build_prompt_period(repo, label, pd, deep=deep, cfg=cfg,
                                        stale_branch_days=stale_branch_days))
                summary = result or ""
            periods_out.append((label, pid, pd, summary))

        # Full-window period data for header totals
        if len(periods) == 1:
            full_pd = periods_out[0][2]
        else:
            full_pd = query_period(con, repo, date_from, date_to,
                                   include_diffs=False,
                                   stale_branch_days=stale_branch_days)

        overall_summary = ""
        if not no_summary and len(periods) > 1:
            logger.info("  → Overall %s summary...", provider.name)
            window = f"{date_from.strftime('%b %d')} – {date_to.strftime('%b %d, %Y')}"
            result = provider.summarize(build_prompt_overall(repo, window, full_pd, cfg=cfg))
            overall_summary = result or ""

        logger.info("Rendering HTML...")
        html = render_report(
            repo=repo, default_branch=default_branch,
            date_from=date_from, date_to=date_to, period=period,
            periods_out=periods_out, overall_summary=overall_summary,
            no_summary=no_summary, deep=deep, full_pd=full_pd,
            cfg=cfg, provider_name=provider.name,
        )

        output_path = cfg.report.output
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html)

        print(f"\nReport saved → {output_path}")
    finally:
        con.close()
