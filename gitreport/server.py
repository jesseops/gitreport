"""Dev server — serve generated reports with linked static assets."""

from __future__ import annotations

import importlib.resources
import logging
from functools import partial
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import argparse

    from .config import Config

logger = logging.getLogger(__name__)


class ReportHandler(SimpleHTTPRequestHandler):
    """Serve reports from directory, static assets from package."""

    static_dir: str = ""

    def do_GET(self) -> None:
        if self.path.startswith("/static/"):
            self._serve_static()
        else:
            super().do_GET()

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


def cmd_serve(args: argparse.Namespace, cfg: Config) -> None:
    host = cfg.server.host
    port = cfg.server.port
    serve_dir = getattr(args, "dir", ".")

    # Resolve package static directory
    static_path = importlib.resources.files("gitreport") / "static"
    ReportHandler.static_dir = str(static_path)

    handler = partial(ReportHandler, directory=serve_dir)
    server = HTTPServer((host, port), handler)
    print(f"Serving reports from {Path(serve_dir).resolve()}")
    print(f"  http://{host}:{port}/")
    print("  Press Ctrl+C to stop")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
        server.shutdown()
