"""Jinja2 template rendering for HTML reports."""

from __future__ import annotations

import importlib.resources
import re
from collections import defaultdict
from datetime import datetime
from typing import TYPE_CHECKING

import jinja2
import mistune

if TYPE_CHECKING:
    from .config import Config


def _get_template_env() -> jinja2.Environment:
    """Create Jinja2 environment loading from package templates/."""
    templates_path = importlib.resources.files("gitreport") / "templates"
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(str(templates_path)),
        autoescape=True,
        trim_blocks=True,
        lstrip_blocks=True,
    )
    # Custom filters
    env.filters["md_to_html"] = _md_to_html
    env.filters["fmt_date"] = _fmt_date
    env.filters["short_label"] = _short_label
    env.globals["now"] = datetime.now
    return env


def _md_to_html(text: str) -> str:
    """Convert markdown to HTML using mistune."""
    if not text:
        return ""
    return mistune.html(text)


def _fmt_date(s: str) -> str:
    from .db import fmt_date
    return fmt_date(s)


def _short_label(label: str) -> str:
    """Abbreviate period labels for trend chart."""
    m = re.match(r'^(\w+)\s+(\d{4})$', label)
    if m:
        return m.group(1)[:3] + " '" + m.group(2)[2:]
    m = re.match(r'^Week of (\w+ \d+)', label)
    if m:
        return m.group(1)
    m = re.match(r'^Sprint (\w+ \d+)', label)
    if m:
        return m.group(1)
    return label[:10]


def _load_static(filename: str) -> str:
    """Load a static file from the package."""
    static_path = importlib.resources.files("gitreport") / "static" / filename
    return static_path.read_text(encoding="utf-8")


def _compute_hotfiles(prs_all: list[dict]) -> list[tuple[str, dict]]:
    """Aggregate most-changed files across all PRs."""
    fc: dict[str, dict] = defaultdict(lambda: {"add": 0, "del": 0, "prs": 0})
    for pr in prs_all:
        for f in pr.get("files", []):
            fc[f["filename"]]["add"] += f["additions"]
            fc[f["filename"]]["del"] += f["deletions"]
            fc[f["filename"]]["prs"] += 1
    if not fc:
        return []
    return sorted(fc.items(), key=lambda x: x[1]["add"] + x[1]["del"], reverse=True)[:15]


def render_report(
    *,
    repo: str,
    default_branch: str,
    date_from: datetime,
    date_to: datetime,
    period: str,
    periods_out: list[tuple[str, str, dict, str]],
    overall_summary: str,
    no_summary: bool,
    deep: bool,
    full_pd: dict | None = None,
    cfg: Config | None = None,
    inline_assets: bool = True,
    provider_name: str = "AI",
) -> str:
    """Render the full HTML report using Jinja2 templates."""
    env = _get_template_env()
    template = env.get_template("base.html.j2")

    owner, name = repo.split("/", 1) if "/" in repo else ("", repo)
    window = f"{date_from.strftime('%b %d, %Y')} – {date_to.strftime('%b %d, %Y')}"
    period_labels = {
        "none": "Full window", "weekly": "Weekly",
        "biweekly": "Bi-weekly / sprint", "monthly": "Monthly",
    }

    # Totals from full-window data
    if full_pd:
        total_merged = len(full_pd["pr_stats"]["merged"])
        total_open = len(full_pd["pr_stats"]["open"])
        total_draft = len(full_pd["pr_stats"].get("draft", []))
        total_abandoned = len(full_pd["pr_stats"]["closed_unmerged"])
        total_commits = len(full_pd["commits"])
    else:
        total_merged = sum(len(pd["pr_stats"]["merged"]) for _, _, pd, _ in periods_out)
        total_open = sum(len(pd["pr_stats"]["open"]) for _, _, pd, _ in periods_out)
        total_draft = sum(len(pd["pr_stats"].get("draft", [])) for _, _, pd, _ in periods_out)
        total_abandoned = sum(len(pd["pr_stats"]["closed_unmerged"]) for _, _, pd, _ in periods_out)
        total_commits = sum(len(pd["commits"]) for _, _, pd, _ in periods_out)

    stale_branch_days = cfg.report.stale_branch_days if cfg else 14
    db_name = cfg.database.path if cfg else "gitreport.db"

    # Prepare period data for templates
    periods_data = []
    for label, pid, pd, summary in periods_out:
        ps = pd["pr_stats"]
        all_prs = ps["merged"] + ps["open"] + ps.get("draft", []) + ps["closed_unmerged"]
        hotfiles = _compute_hotfiles(all_prs)
        max_hotfile_churn = max((s["add"] + s["del"] for _, s in hotfiles), default=1) or 1

        # User table: sorted by activity
        ua = pd["user_activity"]
        max_commits = max((v["commits"] for v in ua.values()), default=1) or 1
        sorted_users = sorted(ua.items(), key=lambda x: x[1]["commits"] + x[1]["prs_merged"], reverse=True)[:20]

        periods_data.append({
            "label": label,
            "pid": pid,
            "pd": pd,
            "summary": summary,
            "hotfiles": hotfiles,
            "max_hotfile_churn": max_hotfile_churn,
            "sorted_users": sorted_users,
            "max_commits": max_commits,
        })

    # Trend data
    trend_data = [(label, pid, len(pd["pr_stats"]["merged"])) for label, pid, pd, _ in periods_out]
    max_trend = max((count for _, _, count in trend_data), default=1) or 1

    # Load static assets
    css_content = _load_static("css/report.css")
    js_content = _load_static("js/report.js")

    return template.render(
        repo=repo,
        owner=owner,
        name=name,
        window=window,
        period=period,
        period_label=period_labels.get(period, period),
        default_branch=default_branch,
        deep=deep,
        no_summary=no_summary,
        total_merged=total_merged,
        total_open=total_open,
        total_draft=total_draft,
        total_abandoned=total_abandoned,
        total_commits=total_commits,
        periods_data=periods_data,
        overall_summary=overall_summary,
        stale_branch_days=stale_branch_days,
        db_name=db_name,
        num_periods=len(periods_out),
        trend_data=trend_data,
        max_trend=max_trend,
        inline_assets=inline_assets,
        css_content=css_content,
        js_content=js_content,
        provider_name=provider_name,
    )
