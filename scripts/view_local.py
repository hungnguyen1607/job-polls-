#!/usr/bin/env python3
"""View the internship page locally, reading the CSVs straight from engineering-ats/.

    python scripts/view_local.py        # then open http://localhost:8090

Serves web/index.html at / and the feed at /api/feed, bound to localhost only (no token needed).
"""

import csv
import os
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from serve_feed import build_feed  # noqa: E402

PAGE = Path(__file__).resolve().parent.parent / "web" / "index.html"
PORT = int(os.environ.get("VIEW_PORT", "8090"))


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        path = self.path.split("?")[0]
        if path in ("/", "/index.html"):
            return self._send(200, PAGE.read_bytes(), "text/html")
        if path == "/api/feed":
            try:
                return self._send(200, build_feed(), "application/json")
            except (OSError, csv.Error) as e:
                return self._send(503, f'{{"error": "{e}"}}'.encode(), "application/json")
        self._send(404, b"not found", "text/plain")

    def _send(self, status: int, body: bytes, ctype: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", f"{ctype}; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args: object) -> None:
        pass


if __name__ == "__main__":
    print(f"open http://localhost:{PORT}", flush=True)
    ThreadingHTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
