"""Tiny standard-library HTTP server: JSON API plus the static dashboard."""

from __future__ import annotations

import hmac
import json
import logging
import mimetypes
import secrets
from http import HTTPStatus
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib import resources
from urllib.parse import parse_qs, urlparse

from .collector import Collector

log = logging.getLogger("omniqueue.server")

STATIC_FILES = {"", "index.html", "app.js", "theme.js", "style.css", "favicon.svg", "logo.svg", "logo_text.svg", "logo_text_dark.svg"}


def _static(name: str) -> bytes:
    return resources.files("omniqueue").joinpath("static", name).read_bytes()


SECURITY_HEADERS = {
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Referrer-Policy": "no-referrer",
    "Content-Security-Policy": "default-src 'self'; img-src 'self' https: data:; style-src 'self'; "
                               "script-src 'self'; connect-src 'self'; form-action 'none'; base-uri 'none'",
}
COOKIE = "omniqueue_access"


class Handler(BaseHTTPRequestHandler):
    collector: Collector  # set on the class by make_server
    csrf_token: str  # per-process secret embedded in the page; required on every POST
    access_token: str | None  # from the config; required on every request when set
    server_version = "OmniQueue/0.1"

    def log_message(self, fmt: str, *args) -> None:  # quieter default logging
        log.debug(fmt, *args)

    def _send(self, status: HTTPStatus, body: bytes, content_type: str, extra: dict[str, str] | None = None) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        for k, v in SECURITY_HEADERS.items():
            self.send_header(k, v)
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    # -- access control ------------------------------------------------------------
    def _authorised(self) -> bool:
        """When an access token is configured, every request must carry it as a cookie.
        `/?token=...` sets that cookie once (and redirects), so a link can be opened directly."""
        if not self.access_token:
            return True
        cookie = SimpleCookie(self.headers.get("Cookie", ""))
        morsel = cookie.get(COOKIE)
        return morsel is not None and hmac.compare_digest(morsel.value, self.access_token)

    def _try_token_login(self, url) -> bool:
        token = parse_qs(url.query).get("token", [None])[0]
        if not token or not self.access_token or not hmac.compare_digest(token, self.access_token):
            return False
        self._send(HTTPStatus.SEE_OTHER, b"", "text/plain",
                   {"Location": "/", "Set-Cookie": f"{COOKIE}={self.access_token}; Path=/; HttpOnly; SameSite=Strict"})
        return True

    def _csrf_ok(self) -> bool:
        header = self.headers.get("X-OmniQueue-Token", "")
        return hmac.compare_digest(header, self.csrf_token)

    def _json(self, payload, status: HTTPStatus = HTTPStatus.OK, extra: dict[str, str] | None = None) -> None:
        self._send(status, json.dumps(payload).encode(), "application/json; charset=utf-8", extra)

    def _json_if_changed(self, etag: str, build) -> None:
        """Answer 304 when the client already holds this version (If-None-Match)."""
        if self.headers.get("If-None-Match") == etag:
            self.send_response(HTTPStatus.NOT_MODIFIED)
            self.send_header("ETag", etag)
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            return
        self._json(build(), extra={"ETag": etag})

    def do_GET(self) -> None:  # noqa: N802 - http.server API
        url = urlparse(self.path)
        path = url.path
        if path == "/" and self._try_token_login(url):
            return
        if not self._authorised():
            self._send(HTTPStatus.UNAUTHORIZED, b"OmniQueue: access token required (open /?token=...)\n", "text/plain")
            return
        if path == "/api/state":
            self._json_if_changed(self.collector.state_etag(), self.collector.snapshot)
            return
        if path == "/api/load":
            self._json_if_changed(self.collector.load_etag(), self.collector.load_snapshot)
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
                body = _static(name)
            except FileNotFoundError:
                self._json({"error": "missing static file"}, HTTPStatus.INTERNAL_SERVER_ERROR)
                return
            if name == "index.html":
                body = body.replace(b"__OMNIQUEUE_TOKEN__", self.csrf_token.encode())
            self._send(HTTPStatus.OK, body, f"{ctype}; charset=utf-8")
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
        if not self._authorised():
            self._json({"error": "access token required"}, HTTPStatus.UNAUTHORIZED)
            return
        if not self._csrf_ok():
            self._json({"error": "missing or wrong X-OmniQueue-Token"}, HTTPStatus.FORBIDDEN)
            return
        if path == "/api/refresh":
            self.collector.request_refresh()
            self._json({"ok": True, "refreshing": True})
            return
        if path == "/api/load/refresh":
            started = self.collector.request_load()
            self._json({"ok": True, "started": started})
            return
        if path.startswith("/api/forget/"):
            key = path[len("/api/forget/"):]
            removed = self.collector.history.forget(key)
            self.collector.request_refresh()
            self._json({"ok": removed})
            return
        self._json({"error": "not found"}, HTTPStatus.NOT_FOUND)


def make_server(collector: Collector, host: str, port: int, access_token: str | None = None) -> ThreadingHTTPServer:
    handler = type("BoundHandler", (Handler,), {
        "collector": collector,
        "csrf_token": secrets.token_urlsafe(32),
        "access_token": access_token,
    })
    return ThreadingHTTPServer((host, port), handler)
