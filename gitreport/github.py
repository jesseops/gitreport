"""gh CLI wrappers: JSON, text, GraphQL, and REST helpers."""

from __future__ import annotations

import json
import logging
import subprocess
import sys

logger = logging.getLogger(__name__)


# ── Basic gh CLI wrappers ──────────────────────────────────────────────────

def gh(args: list[str], check: bool = True) -> dict | list | None:
    try:
        r = subprocess.run(["gh"] + args, capture_output=True, text=True, check=check)
        return json.loads(r.stdout) if r.stdout.strip() else None
    except subprocess.CalledProcessError as e:
        logger.warning("gh error: %s", e.stderr.strip())
        return None
    except json.JSONDecodeError:
        return None


def gh_text(args: list[str]) -> str:
    """Run gh, return raw text output (for diffs etc.)."""
    try:
        r = subprocess.run(["gh"] + args, capture_output=True, text=True, check=True)
        return r.stdout
    except subprocess.CalledProcessError as e:
        logger.warning("gh error: %s", e.stderr.strip())
        return ""


def gh_lines(args: list[str]) -> list[str]:
    try:
        r = subprocess.run(["gh"] + args, capture_output=True, text=True, check=True)
        return [line.strip() for line in r.stdout.splitlines() if line.strip()]
    except subprocess.CalledProcessError as e:
        logger.warning("gh error: %s", e.stderr.strip())
        return []


def check_gh() -> None:
    try:
        subprocess.run(["gh", "auth", "status"], capture_output=True, check=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        logger.error("gh CLI not found or not authenticated. Run: gh auth login")
        sys.exit(1)


# ── GraphQL ────────────────────────────────────────────────────────────────

_PR_GRAPHQL_FIELDS = """
  number
  title
  body
  state
  author { login }
  createdAt
  updatedAt
  mergedAt
  closedAt
  baseRefName
  headRefName
  additions
  deletions
  comments { totalCount }
  reviews(first: 5) { totalCount nodes { state } }
  reviewDecision
  labels(first: 10) { nodes { name } }
  milestone { title }
"""

_PR_BATCH_QUERY = """
query($owner: String!, $name: String!, $states: [PullRequestState!], $first: Int!, $after: String) {
  repository(owner: $owner, name: $name) {
    pullRequests(states: $states, first: $first, after: $after, orderBy: {field: UPDATED_AT, direction: DESC}) {
      pageInfo { hasNextPage endCursor }
      nodes {
""" + _PR_GRAPHQL_FIELDS + """
      }
    }
  }
}
"""


def _graphql(query: str, variables: dict) -> dict | None:
    """Run a gh api graphql call, return parsed response data or None."""
    payload = json.dumps({"query": query, "variables": variables})
    try:
        r = subprocess.run(
            ["gh", "api", "graphql", "--input", "-"],
            input=payload, capture_output=True, text=True, check=True
        )
        data = json.loads(r.stdout)
        if data.get("errors"):
            for err in data["errors"]:
                logger.warning("GraphQL error: %s", err.get("message", ""))
            return None
        return data.get("data")
    except subprocess.CalledProcessError as e:
        logger.warning("gh graphql error: %s", e.stderr.strip())
        return None
    except json.JSONDecodeError as e:
        logger.warning("GraphQL JSON parse error: %s", e)
        return None


def _normalise_pr(node: dict) -> dict:
    """Convert a GraphQL PR node into the flat shape db_upsert_prs expects."""
    return {
        "number":         node["number"],
        "title":          node.get("title") or "",
        "body":           node.get("body") or "",
        "state":          node.get("state") or "",
        "author":         node.get("author") or {},
        "createdAt":      node.get("createdAt") or "",
        "updatedAt":      node.get("updatedAt") or "",
        "mergedAt":       node.get("mergedAt") or "",
        "closedAt":       node.get("closedAt") or "",
        "baseRefName":    node.get("baseRefName") or "",
        "headRefName":    node.get("headRefName") or "",
        "additions":      node.get("additions") or 0,
        "deletions":      node.get("deletions") or 0,
        "comments":       (node.get("comments") or {}).get("totalCount", 0),
        "reviews":        (node.get("reviews") or {}).get("nodes") or [],
        "reviewDecision": node.get("reviewDecision") or "",
        "labels":         [{"name": lbl["name"]} for lbl in ((node.get("labels") or {}).get("nodes") or [])],
        "milestone":      {"title": ((node.get("milestone") or {}).get("title") or "")},
    }


def fetch_prs_graphql(repo: str, since: str | None = None, batch_size: int = 50) -> list[dict]:
    """Fetch all PRs for a repo using cursor-paginated GraphQL batches."""
    if "/" not in repo:
        logger.error("Invalid repo format: %s", repo)
        return []

    owner, name = repo.split("/", 1)
    states = ["OPEN", "CLOSED", "MERGED"]
    all_prs: list[dict] = []
    cursor = None
    page = 0

    while True:
        page += 1
        variables = {
            "owner": owner,
            "name": name,
            "states": states,
            "first": batch_size,
            "after": cursor,
        }
        data = _graphql(_PR_BATCH_QUERY, variables)
        if not data:
            logger.warning("Stopping after page %d (GraphQL error)", page)
            break

        pr_conn = data["repository"]["pullRequests"]
        page_info = pr_conn["pageInfo"]
        nodes = pr_conn["nodes"] or []

        normalised = [_normalise_pr(n) for n in nodes]
        all_prs.extend(normalised)
        logger.info("    page %d: %d PRs  (total so far: %d)", page, len(normalised), len(all_prs))

        if since and normalised:
            oldest_updated = min(p["updatedAt"] for p in normalised if p["updatedAt"])
            if oldest_updated < since:
                break

        if not page_info["hasNextPage"]:
            break

        cursor = page_info["endCursor"]

    return all_prs


def parse_rest_commit(c: dict) -> dict:
    """Convert a REST API commit object into the shape db_upsert_commits expects."""
    return {
        "abbreviatedOid": (c.get("sha") or "")[:7],
        "authors":        [{"login": (c.get("author") or {}).get("login", ""),
                            "name":  ((c.get("commit") or {}).get("author") or {}).get("name", "")}],
        "committedDate":  ((c.get("commit") or {}).get("committer") or {}).get("date", ""),
        "message":        (c.get("commit") or {}).get("message", ""),
    }
