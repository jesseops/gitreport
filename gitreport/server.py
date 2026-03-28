"""Dev server — serve generated reports with linked static assets."""

from __future__ import annotations

import html
import importlib.resources
import logging
from functools import partial
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import unquote

if TYPE_CHECKING:
    import argparse

    from .config import Config

logger = logging.getLogger(__name__)


class ReportHandler(SimpleHTTPRequestHandler):
    """Serve only HTML reports and package static assets."""

    static_dir: str = ""
    serve_root: Path = Path(".")

    def do_GET(self) -> None:
        if self.path.startswith("/static/"):
            self._serve_static()
        elif self.path == "/" or self.path == "":
            self._serve_index()
        else:
            self._serve_report()

    def _serve_index(self) -> None:
        reports = sorted(
            self.serve_root.glob("*.html"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        items = "\n".join(
            f'<li><a href="/{html.escape(p.name)}">{html.escape(p.name)}</a></li>'
            for p in reports
        )
        body = f"""\
<!doctype html>
<html><head><meta charset="utf-8"><title>Reports</title>
<style>
  body {{ font-family: system-ui, sans-serif; max-width: 600px; margin: 2rem auto; }}
  li {{ margin: 0.4rem 0; }}
  a {{ color: #0969da; text-decoration: none; }}
  a:hover {{ text-decoration: underline; }}
</style>
</head><body>
<h1>Reports</h1>
{"<ul>" + items + "</ul>" if items else "<p>No HTML reports found.</p>"}
</body></html>"""
        data = body.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _serve_report(self) -> None:
        requested = unquote(self.path.lstrip("/"))
        # Block path traversal: no slashes, no .., must end in .html
        if "/" in requested or "\\" in requested or ".." in requested:
            self.send_error(404)
            return
        if not requested.endswith(".html"):
            self.send_error(404)
            return

        file_path = (self.serve_root / requested).resolve()
        if not file_path.is_relative_to(self.serve_root.resolve()):
            self.send_error(404)
            return
        if not file_path.is_file():
            self.send_error(404)
            return

        content = file_path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def _serve_static(self) -> None:
        rel_path = self.path[len("/static/"):]
        file_path = Path(self.static_dir) / rel_path
        if not file_path.exists() or not file_path.is_file():
            self.send_error(404, f"Static file not found: {rel_path}")
            return

        content = file_path.read_bytes()
        self.send_response(200)

        suffix = file_path.suffix.lower()
        content_types = {
            ".css": "text/css",
            ".js": "application/javascript",
            ".html": "text/html",
            ".svg": "image/svg+xml",
            ".png": "image/png",
        }
        self.send_header("Content-Type", content_types.get(suffix, "application/octet-stream"))
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def log_message(self, format: str, *args: object) -> None:
        logger.info(format, *args)


def cmd_serve(args: argparse.Namespace, cfg: Config) -> None:
    host = cfg.server.host
    port = cfg.server.port
    serve_dir = getattr(args, "dir", ".")

    # Resolve package static directory
    static_path = importlib.resources.files("gitreport") / "static"
    ReportHandler.static_dir = str(static_path)
    ReportHandler.serve_root = Path(serve_dir).resolve()

    handler = partial(ReportHandler, directory=serve_dir)
    server = HTTPServer((host, port), handler)
    print(f"Serving reports from {ReportHandler.serve_root}")
    print(f"  http://{host}:{port}/")
    print("  Press Ctrl+C to stop")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
        server.shutdown()
