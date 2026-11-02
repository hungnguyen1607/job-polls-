"""Serve web/ statically on :5500, proxying /api/feed to the local serve_feed.py on :8787.
Lets you open the real frontend locally against this machine's live poller data.
"""
import json
import os
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

WEB_DIR = Path(__file__).resolve().parent.parent / "web"
TOKEN = os.environ["FEED_TOKEN"]
FEED_URL = "http://localhost:8787/feed.json"


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path.split("?")[0] == "/api/feed":
            req = urllib.request.Request(FEED_URL, headers={"Authorization": f"Bearer {TOKEN}"})
            try:
                with urllib.request.urlopen(req, timeout=10) as resp:
                    body = resp.read()
            except Exception as e:  # noqa: BLE001
                body = json.dumps({"error": str(e)}).encode()
                self.send_response(502)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)
            return
        path = self.path.split("?")[0]
        if path == "/" or path == "":
            path = "/index.html"
        elif path in ("/tailor", "/tracker"):
            path = f"{path}.html"
        fp = WEB_DIR / path.lstrip("/")
        if not fp.exists() or WEB_DIR not in fp.resolve().parents:
            self.send_response(404)
            self.end_headers()
            return
        body = fp.read_bytes()
        self.send_response(200)
        ctype = "text/html" if fp.suffix == ".html" else "text/plain"
        self.send_header("Content-Type", f"{ctype}; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args: object) -> None:
        pass


if __name__ == "__main__":
    print("serving web/ on http://localhost:5500", flush=True)
    ThreadingHTTPServer(("0.0.0.0", 5500), Handler).serve_forever()
