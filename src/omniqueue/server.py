"""Tiny standard-library HTTP server: JSON API plus the static dashboard."""

from __future__ import annotations

import json
import logging
import mimetypes
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib import resources
from urllib.parse import urlparse

from .collector import Collector

log = logging.getLogger("omniqueue.server")

STATIC_FILES = {"", "index.html", "app.js", "style.css", "favicon.svg"}


def _static(name: str) -> bytes:
    return resources.files("omniqueue").joinpath("static", name).read_bytes()


class Handler(BaseHTTPRequestHandler):
    collector: Collector  # set on the class by make_server
    server_version = "OmniQueue/0.1"

    def log_message(self, fmt: str, *args) -> None:  # quieter default logging
        log.debug(fmt, *args)

    def _send(self, status: HTTPStatus, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _json(self, payload, status: HTTPStatus = HTTPStatus.OK) -> None:
        self._send(status, json.dumps(payload).encode(), "application/json; charset=utf-8")

    def do_GET(self) -> None:  # noqa: N802 - http.server API
        path = urlparse(self.path).path
        if path == "/api/state":
            self._json(self.collector.snapshot())
            return
        if path == "/api/health":
            self._json({"ok": True})
            return
        if path.startswith("/logo/"):
            self._logo(path[len("/logo/"):])
            return
        name = path.lstrip("/") or "index.html"
        if name in STATIC_FILES:
            ctype = mimetypes.guess_type(name)[0] or "application/octet-stream"
            try:
                self._send(HTTPStatus.OK, _static(name), f"{ctype}; charset=utf-8")
            except FileNotFoundError:
                self._json({"error": "missing static file"}, HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        self._json({"error": "not found"}, HTTPStatus.NOT_FOUND)

    def _logo(self, name: str) -> None:
        from urllib.parse import unquote

        file_path = self.collector.logo_path(unquote(name))
        if not file_path:
            self._json({"error": "no logo"}, HTTPStatus.NOT_FOUND)
            return
        try:
            with open(file_path, "rb") as fh:
                body = fh.read()
        except OSError:
            self._json({"error": "unreadable logo"}, HTTPStatus.NOT_FOUND)
            return
        ctype = mimetypes.guess_type(file_path)[0] or "application/octet-stream"
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "max-age=3600")
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        if path == "/api/refresh":
            self.collector.request_refresh()
            self._json({"ok": True, "refreshing": True})
            return
        if path.startswith("/api/forget/"):
            key = path[len("/api/forget/"):]
            removed = self.collector.history.forget(key)
            self.collector.request_refresh()
            self._json({"ok": removed})
            return
        self._json({"error": "not found"}, HTTPStatus.NOT_FOUND)


def make_server(collector: Collector, host: str, port: int) -> ThreadingHTTPServer:
    handler = type("BoundHandler", (Handler,), {"collector": collector})
    return ThreadingHTTPServer((host, port), handler)
