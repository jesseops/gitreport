"""AI provider abstraction: protocol, implementations, and prompt builders."""

from __future__ import annotations

import json
import logging
import subprocess
import sys
import urllib.error
import urllib.request
from collections import defaultdict
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from .config import Config

logger = logging.getLogger(__name__)


# ── Provider protocol ──────────────────────────────────────────────────────

@runtime_checkable
class SummaryProvider(Protocol):
    name: str

    def is_available(self) -> bool: ...
    def summarize(self, prompt: str) -> str | None: ...


# ── Implementations ────────────────────────────────────────────────────────

class ClaudeProvider:
    name = "claude"

    def is_available(self) -> bool:
        try:
            subprocess.run(["claude", "--version"], capture_output=True, check=True)
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            return False

    def summarize(self, prompt: str) -> str | None:
        try:
            r = subprocess.run(["claude", "-p", "-"], input=prompt,
                               capture_output=True, text=True, check=True)
            return r.stdout.strip()
        except subprocess.CalledProcessError as e:
            return f"Summary failed: {e.stderr.strip()}"


class CodexProvider:
    name = "codex"

    def is_available(self) -> bool:
        try:
            subprocess.run(["codex", "--version"], capture_output=True, check=True)
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            return False

    def summarize(self, prompt: str) -> str | None:
        try:
            r = subprocess.run(["codex", "-q", prompt],
                               capture_output=True, text=True, check=True)
            return r.stdout.strip()
        except subprocess.CalledProcessError as e:
            return f"Summary failed: {e.stderr.strip()}"


class OllamaProvider:
    name = "ollama"

    def __init__(self, base_url: str = "http://localhost:11434", model: str = "llama3.1",
                 max_context: int = 8_000) -> None:
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.max_context = max_context

    def is_available(self) -> bool:
        try:
            req = urllib.request.Request(f"{self.base_url}/api/tags", method="GET")
            with urllib.request.urlopen(req, timeout=3) as resp:
                return resp.status == 200
        except (urllib.error.URLError, OSError):
            return False

    def summarize(self, prompt: str) -> str | None:
        # Truncate prompt to fit within context budget
        max_chars = self.max_context * 4  # rough chars-per-token
        if len(prompt) > max_chars:
            prompt = prompt[:max_chars] + "\n\n[context truncated]"

        payload = json.dumps({
            "model": self.model,
            "prompt": prompt,
            "stream": False,
        }).encode()
        req = urllib.request.Request(
            f"{self.base_url}/api/generate",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                data = json.loads(resp.read())
                return data.get("response", "").strip()
        except (urllib.error.URLError, OSError, json.JSONDecodeError) as e:
            return f"Ollama summary failed: {e}"


class NoneProvider:
    """No-op provider for --no-summary."""
    name = "none"

    def is_available(self) -> bool:
        return True

    def summarize(self, prompt: str) -> str | None:
        return None


# ── Provider resolution ────────────────────────────────────────────────────

def get_provider(cfg: Config) -> SummaryProvider:
    """Resolve provider from config. auto-detect tries claude → codex → ollama."""
    provider_name = cfg.ai.provider

    if provider_name == "none":
        return NoneProvider()

    if provider_name == "claude":
        p = ClaudeProvider()
        if not p.is_available():
            logger.error("claude CLI not found.")
            sys.exit(1)
        return p

    if provider_name == "codex":
        p = CodexProvider()
        if not p.is_available():
            logger.error("codex CLI not found.")
            sys.exit(1)
        return p

    if provider_name == "ollama":
        p = OllamaProvider(
            base_url=cfg.ai.ollama_base_url,
            model=cfg.ai.ollama_model,
            max_context=cfg.ai.ollama_max_context,
        )
        if not p.is_available():
            logger.error("Ollama not reachable at %s", cfg.ai.ollama_base_url)
            sys.exit(1)
        return p

    # auto-detect
    for ProviderClass, kwargs in [
        (ClaudeProvider, {}),
        (CodexProvider, {}),
        (OllamaProvider, {
            "base_url": cfg.ai.ollama_base_url,
            "model": cfg.ai.ollama_model,
            "max_context": cfg.ai.ollama_max_context,
        }),
    ]:
        p = ProviderClass(**kwargs)
        if p.is_available():
            logger.info("  Auto-detected AI provider: %s", p.name)
            return p

    logger.warning("No AI provider found. Summaries will be skipped.")
    return NoneProvider()


# ── Prompt builders ────────────────────────────────────────────────────────

def truncate_body(text: str, max_chars: int = 500) -> str:
    if not text:
        return ""
    text = text.strip()
    if len(text) <= max_chars:
        return text
    return text[:max_chars].rstrip() + "…"


DEFAULT_PERIOD_INSTRUCTIONS = """Write a concise engineering intelligence report for this period:

0. **Executive Summary** — 2-3 sentences capturing the most important takeaway.
   What would a VP of Engineering want to know at a glance?
1. **What Shipped** — specific PRs that landed; what areas/files were touched most
2. **Work In Flight** — active PRs and branches, long-running work, review bottlenecks
3. **Throwaway Work** — abandoned PRs or stale branches; are there patterns?
4. **Competing / Parallel Development** — multiple people or PRs touching the same areas
5. **Risks & Bottlenecks** — PRs with no reviews, large changes, stale work, concerning patterns
6. **Notable Commits** — any commit messages that suggest important context not captured in PRs

Skip sections with nothing interesting. Be specific about PR numbers, titles, and filenames."""

DEFAULT_OVERALL_INSTRUCTIONS = """Write a high-level executive summary:

0. **TL;DR** — 2-3 sentences capturing the overall state of this project.
   What would a VP of Engineering want to know at a glance?
1. **Overall Trajectory** — accelerating, steady, slowing? Evidence?
2. **Major Themes** — dominant areas of work (reference specific files/modules/features)
3. **Team Dynamics** — who owns what, collaboration patterns, any single points of failure
4. **Persistent Risks** — problems that appear to recur across the window
5. **Recommendations** — 2-3 concrete, specific suggestions based on the data"""

DEFAULT_PROMPTS = {
    "period": DEFAULT_PERIOD_INSTRUCTIONS,
    "overall": DEFAULT_OVERALL_INSTRUCTIONS,
}


def _load_prompt_instructions(cfg: Config, prompt_type: str) -> str:
    """Load custom instructions from file if configured, else use defaults.

    If prompts.context is set, it is appended as additional context.
    """
    path = getattr(cfg.prompts, prompt_type, None)
    if path:
        from pathlib import Path
        p = Path(path)
        if p.exists():
            instructions = p.read_text()
        else:
            logger.warning("Custom prompt file not found: %s", path)
            instructions = DEFAULT_PROMPTS[prompt_type]
    else:
        instructions = DEFAULT_PROMPTS[prompt_type]

    if cfg.prompts.context:
        instructions += f"\n\nAdditional context:\n{cfg.prompts.context}"

    return instructions


def _pr_block(pr: dict, include_diff: bool = False, diffs_by_pr: dict | None = None, include_body: bool = True) -> str:
    """Format a single PR for an AI prompt."""
    lines = []
    login = pr.get("author") or "?"
    draft_tag = "[DRAFT] " if pr.get("is_draft") else ""
    lines.append(f"  PR #{pr['number']}: {draft_tag}[{pr.get('title', '')}] by @{login}")

    if pr.get("merged_at"):
        lines.append(f"    Merged: {pr['merged_at'][:10]}")
    elif pr.get("created_at"):
        lines.append(f"    Opened: {pr['created_at'][:10]}"
                     + (f"  Closed: {pr['closed_at'][:10]}" if pr.get("closed_at") else ""))

    meta = []
    if pr.get("additions") or pr.get("deletions"):
        meta.append(f"+{pr.get('additions', 0)}/-{pr.get('deletions', 0)} lines")
    if pr.get("comment_count"):
        meta.append(f"{pr['comment_count']} comments")
    if pr.get("review_count"):
        meta.append(f"{pr['review_count']} reviews")
    if pr.get("review_decision"):
        meta.append(f"review: {pr['review_decision']}")
    if pr.get("labels"):
        meta.append(f"labels: {', '.join(pr['labels'])}")
    if pr.get("milestone"):
        meta.append(f"milestone: {pr['milestone']}")
    if meta:
        lines.append(f"    {' | '.join(meta)}")

    if include_body:
        body = truncate_body(pr.get("body", ""), 2000)
        if body:
            lines.append(f"    Description: {body}")

    files = pr.get("files", [])
    if files:
        total_files = len(files)
        shown = files[:15]
        lines.append(f"    Files changed ({total_files}):")
        for f in shown:
            lines.append(f"      {f['filename']}  +{f['additions']}/-{f['deletions']}"
                         + (f"  [{f['status']}]" if f.get("status") else ""))
        if total_files > 15:
            rest_add = sum(f["additions"] for f in files[15:])
            rest_del = sum(f["deletions"] for f in files[15:])
            lines.append(f"      … {total_files - 15} more files  +{rest_add}/-{rest_del}")

    if include_diff and diffs_by_pr and pr["number"] in diffs_by_pr:
        lines.append("    Full diff:")
        lines.append(diffs_by_pr[pr["number"]])

    return "\n".join(lines)


def _commits_block(commits: list[dict], max_commits: int = 30) -> str:
    if not commits:
        return "  (none)"
    lines = []
    for c in commits[:max_commits]:
        msg = (c.get("message") or "").split("\n")[0][:120]
        pr_tag = f"  [PR #{c['pr_number']}]" if c.get("pr_number") else ""
        lines.append(f"  {c.get('sha', '')[:7]}  @{c.get('author', '?')}  {c.get('committed_at', '')[:10]}{pr_tag}  {msg}")
    if len(commits) > max_commits:
        lines.append(f"  … {len(commits) - max_commits} more commits")
    return "\n".join(lines)


def build_prompt_period(repo: str, label: str, pd: dict, deep: bool = False,
                        cfg: Config | None = None, stale_branch_days: int = 14) -> str:
    pr = pd["pr_stats"]
    bc = pd["branch_cats"]
    ua = pd["user_activity"]
    diffs = pd.get("diffs_by_pr", {})
    include_diff = deep

    max_prs = cfg.report.max_prs if cfg else 20
    cap = max_prs if max_prs > 0 else None  # 0 means no limit

    top = sorted(ua.items(), key=lambda x: x[1]["commits"] + x[1]["prs_merged"], reverse=True)[:10]
    user_lines = "\n".join(
        f"  @{u}: {s['commits']} commits | {s['prs_merged']} PRs merged | +{s['additions']}/-{s['deletions']} lines"
        for u, s in top) or "  (none)"

    merged_block = "\n\n".join(_pr_block(p, include_diff, diffs, include_body=True) for p in pr["merged"][:cap]) or "  (none)"
    open_block = "\n\n".join(_pr_block(p, include_diff, diffs, include_body=False) for p in pr["open"][:cap]) or "  (none)"
    draft_block = "\n\n".join(_pr_block(p, include_diff, diffs, include_body=False) for p in pr.get("draft", [])[:cap]) or "  (none)"
    abandoned_block = "\n\n".join(_pr_block(p, include_diff, diffs, include_body=False) for p in pr["closed_unmerged"][:cap]) or "  (none)"

    if cap is not None:
        for cat, items in [
            ("merged", pr["merged"]),
            ("open", pr["open"]),
            ("draft", pr.get("draft", [])),
            ("closed unmerged", pr["closed_unmerged"]),
        ]:
            if len(items) > cap:
                logger.warning("Period %s: %d/%d %s PRs omitted from AI prompt", label, len(items) - cap, len(items), cat)

    active_br = "\n".join(f"  {b['name']} ({b.get('age_days', '?')}d ago, @{b.get('last_author', '')})"
                          for b in bc["active"][:20]) or "  (none)"
    stale_br = "\n".join(f"  {b['name']} ({b.get('age_days', '?')}d ago)"
                         for b in bc["stale"][:20]) or "  (none)"

    commits_block = _commits_block(pd["commits"])
    depth_note = " (full diff content included)" if deep else " (file stats included)"
    instructions = _load_prompt_instructions(cfg, "period") if cfg else DEFAULT_PERIOD_INSTRUCTIONS

    return f"""Analyze GitHub repository activity{depth_note}.

Repository: {repo}
Period: {label}

━━━ MERGED PRs ({len(pr['merged'])}) ━━━
{merged_block}

━━━ OPEN / IN REVIEW ({len(pr['open'])}) ━━━
{open_block}

━━━ DRAFT PRs ({len(pr.get('draft', []))}) ━━━
{draft_block}
Note: Draft PRs are for early feedback, info sharing, spikes, or placeholders. Flag any that are stale.

━━━ CLOSED WITHOUT MERGING ({len(pr['closed_unmerged'])}) ━━━
{abandoned_block}

━━━ COMMITS ({len(pd['commits'])}) ━━━
Commits tagged [PR #N] were part of that PR's review cycle, not direct pushes to the default branch.
{commits_block}

━━━ ACTIVE BRANCHES ━━━
{active_br}

━━━ STALE BRANCHES (>{stale_branch_days}d) ━━━
{stale_br}

━━━ CONTRIBUTORS ━━━
{user_lines}

{instructions}"""


def build_prompt_overall(repo: str, window: str, pd: dict,
                         cfg: Config | None = None) -> str:
    pr = pd["pr_stats"]
    ua = pd["user_activity"]
    top = sorted(ua.items(), key=lambda x: x[1]["commits"] + x[1]["prs_merged"], reverse=True)[:10]

    file_counts: dict[str, dict] = defaultdict(lambda: {"additions": 0, "deletions": 0, "pr_count": 0})
    for p in pr["merged"] + pr["open"] + pr.get("draft", []) + pr["closed_unmerged"]:
        for f in p.get("files", []):
            file_counts[f["filename"]]["additions"] += f["additions"]
            file_counts[f["filename"]]["deletions"] += f["deletions"]
            file_counts[f["filename"]]["pr_count"] += 1
    hot_files = sorted(file_counts.items(),
                       key=lambda x: x[1]["additions"] + x[1]["deletions"], reverse=True)[:20]
    hot_files_str = "\n".join(
        f"  {fn}: +{s['additions']}/-{s['deletions']} across {s['pr_count']} PRs"
        for fn, s in hot_files) or "  (none)"

    user_lines = "\n".join(
        f"  @{u}: {s['commits']} commits | {s['prs_merged']} PRs merged | +{s['additions']}/-{s['deletions']} lines"
        for u, s in top) or "  (none)"

    instructions = _load_prompt_instructions(cfg, "overall") if cfg else DEFAULT_OVERALL_INSTRUCTIONS

    n_merged = len(pr["merged"])
    n_open = len(pr["open"])
    n_draft = len(pr.get("draft", []))
    n_abandoned = len(pr["closed_unmerged"])

    return f"""Analyze GitHub repository activity across the full reporting window.

Repository: {repo}
Full window: {window}

Totals: {n_merged} merged | {n_open} in review | {n_draft} draft | {n_abandoned} abandoned

━━━ MOST-CHANGED FILES (full window) ━━━
{hot_files_str}

━━━ CONTRIBUTORS (full window) ━━━
{user_lines}

{instructions}"""
