"""Tests for Jinja2 template rendering."""

from __future__ import annotations

from datetime import UTC, datetime

from gitreport.render import render_report


def test_render_report_basic(sample_period_data):
    """Render a basic report and check for expected elements."""
    periods_out = [("Full Window", "p0", sample_period_data, "")]
    html = render_report(
        repo="owner/repo",
        default_branch="main",
        date_from=datetime(2025, 3, 1, tzinfo=UTC),
        date_to=datetime(2025, 3, 31, tzinfo=UTC),
        period="none",
        periods_out=periods_out,
        overall_summary="",
        no_summary=True,
        deep=False,
        full_pd=sample_period_data,
    )
    assert "<!DOCTYPE html>" in html
    assert "owner/repo" in html
    assert "Full Window" in html
    assert "#42" in html
    assert "alice" in html
    assert "report.css" not in html  # inline by default
    assert "<style>" in html


def test_render_report_with_summary(sample_period_data):
    """Render with an AI summary present."""
    summary = "**Great progress** this week. Feature X shipped successfully."
    periods_out = [("Week of Mar 01", "p0", sample_period_data, summary)]
    html = render_report(
        repo="owner/repo",
        default_branch="main",
        date_from=datetime(2025, 3, 1, tzinfo=UTC),
        date_to=datetime(2025, 3, 8, tzinfo=UTC),
        period="weekly",
        periods_out=periods_out,
        overall_summary="",
        no_summary=False,
        deep=False,
        full_pd=sample_period_data,
        provider_name="claude",
    )
    assert "claude Analysis" in html
    assert "Great progress" in html


def test_render_report_linked_assets(sample_period_data):
    """Render with linked (not inline) assets."""
    periods_out = [("Full Window", "p0", sample_period_data, "")]
    html = render_report(
        repo="owner/repo",
        default_branch="main",
        date_from=datetime(2025, 3, 1, tzinfo=UTC),
        date_to=datetime(2025, 3, 31, tzinfo=UTC),
        period="none",
        periods_out=periods_out,
        overall_summary="",
        no_summary=True,
        deep=False,
        full_pd=sample_period_data,
        inline_assets=False,
    )
    assert 'href="/static/css/report.css"' in html
    assert 'src="/static/js/report.js"' in html
    assert "<style>" not in html


def test_render_xss_escaping(sample_period_data):
    """Verify that user-controlled content is escaped."""
    xss_data = sample_period_data.copy()
    xss_data["pr_stats"] = {
        "merged": [{
            **sample_period_data["pr_stats"]["merged"][0],
            "title": '<script>alert("xss")</script>',
            "author": '<img src=x onerror=alert(1)>',
        }],
        "open": [],
        "closed_unmerged": [],
    }
    periods_out = [("Full Window", "p0", xss_data, "")]
    html = render_report(
        repo="owner/repo",
        default_branch="main",
        date_from=datetime(2025, 3, 1, tzinfo=UTC),
        date_to=datetime(2025, 3, 31, tzinfo=UTC),
        period="none",
        periods_out=periods_out,
        overall_summary="",
        no_summary=True,
        deep=False,
        full_pd=xss_data,
    )
    assert '<script>alert("xss")</script>' not in html
    assert "&lt;script&gt;" in html
