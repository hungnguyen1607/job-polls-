#!/usr/bin/env python3
"""Serve US_internships.csv and canada_internships.csv as one JSON feed for the web page.

    FEED_TOKEN=secret python scripts/serve_feed.py      # GET /feed.json with "Authorization: Bearer secret"

Env: FEED_TOKEN (required), FEED_HOST (default 0.0.0.0), FEED_PORT (default 8787).
"""

import csv
import hmac
import json
import os
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

OUT_DIR = Path(__file__).resolve().parent.parent / "engineering-ats"
FILES = {"us": OUT_DIR / "US_internships.csv", "canada": OUT_DIR / "canada_internships.csv"}
FIELDS = ("company", "title", "location", "posted_on", "posted", "found", "link")
TOKEN = os.environ.get("FEED_TOKEN", "")

_cache: dict[str, object] = {"key": None, "body": b""}


def build_feed() -> bytes:
    mtimes = tuple(p.stat().st_mtime for p in FILES.values())
    if _cache["key"] == mtimes:
        return _cache["body"]  # type: ignore[return-value]
    feed: dict[str, object] = {"updated": datetime.fromtimestamp(max(mtimes), UTC).isoformat(timespec="seconds")}
    for name, path in FILES.items():
        with path.open(newline="", encoding="utf-8") as fh:
            feed[name] = [{k: row.get(k, "") for k in FIELDS} for row in csv.DictReader(fh)]
    body = json.dumps(feed, ensure_ascii=False, separators=(",", ":")).encode()
    _cache.update(key=mtimes, body=body)
    return body


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path.split("?")[0] == "/health":
            return self._send(200, b"ok", "text/plain")
        if self.path.split("?")[0] != "/feed.json":
            return self._send(404, b"not found", "text/plain")
        given = self.headers.get("Authorization", "").removeprefix("Bearer ").strip()
        if not hmac.compare_digest(given, TOKEN):
            return self._send(401, b"unauthorized", "text/plain")
        try:
            body = build_feed()
        except (OSError, csv.Error) as e:  # a poller is mid-write or the files don't exist yet
            return self._send(503, str(e).encode(), "text/plain")
        self._send(200, body, "application/json")

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
    if len(TOKEN) < 16:
        raise SystemExit("set FEED_TOKEN to a random string of 16+ characters")
    host, port = os.environ.get("FEED_HOST", "0.0.0.0"), int(os.environ.get("FEED_PORT", "8787"))
    print(f"serving feed on http://{host}:{port}/feed.json", flush=True)
    ThreadingHTTPServer((host, port), Handler).serve_forever()
