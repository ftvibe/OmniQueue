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
from .predict import Request, predict
from .projects import ProjectPoller

log = logging.getLogger("omniqueue.server")

STATIC_FILES = {"", "index.html", "widget", "widget.html", "widget.js", "arrays.js", "app.js", "theme.js", "style.css", "favicon.svg", "logo.svg", "logo_text.svg", "logo_text_dark.svg"}


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
    projects: ProjectPoller | None  # slow project poller (None when no cluster lists projects)
    csrf_token: str  # per-process secret embedded in the page; required on every POST
    access_token: str | None  # from the config; required on every request when set
    allowed_hosts: frozenset[str] | None  # Host header values accepted on a loopback listener (None = any)
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
    def _host_ok(self) -> bool:
        """On a loopback listener the Host header must name this machine.  A web page that
        points its own domain at 127.0.0.1 (DNS rebinding) would otherwise be same-origin
        with the dashboard in the visitor's browser and could read the job data."""
        if self.allowed_hosts is None:
            return True
        host = self.headers.get("Host", "").strip().lower()
        if host.startswith("["):  # [::1]:8765
            host = host[1:host.find("]")] if "]" in host else host
        elif host.count(":") == 1:
            host = host.rsplit(":", 1)[0]
        return host in self.allowed_hosts

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
        if not self._host_ok():
            self._send(HTTPStatus.MISDIRECTED_REQUEST, b"OmniQueue: unexpected Host header\n", "text/plain")
            return
        if path == "/" and self._try_token_login(url):
            return
        if not self._authorised():
            self._send(HTTPStatus.UNAUTHORIZED, b"OmniQueue: access token required (open /?token=...)\n", "text/plain")
            return
        if path == "/api/state":
            client = self.headers.get("X-OmniQueue-Client", "")
            if client:
                try:
                    interval = float(self.headers.get("X-OmniQueue-Interval", "0"))
                except ValueError:
                    interval = 0.0
                self.collector.register_viewer(client[:64], interval)
            self._json_if_changed(self.collector.state_etag(), self.collector.snapshot)
            return
        if path == "/api/load":
            self._json_if_changed(self.collector.load_etag(), self.collector.load_snapshot)
            return
        if path == "/api/projects":
            if self.projects is None:
                self._json({"now": 0, "enabled": False, "clusters": [], "projects": []})
                return
            self._json_if_changed(self.projects.etag(), self.projects.snapshot)
            return
        if path == "/api/health":
            self._json({"ok": True})
            return
        if path.startswith("/logo/"):
            self._logo(path[len("/logo/"):])
            return
        name = path.lstrip("/") or "index.html"
        if name == "widget":
            name = "widget.html"
        if name in STATIC_FILES:
            ctype = mimetypes.guess_type(name)[0] or "application/octet-stream"
            try:
                body = _static(name)
            except FileNotFoundError:
                self._json({"error": "missing static file"}, HTTPStatus.INTERNAL_SERVER_ERROR)
                return
            if name in ("index.html", "widget.html"):
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
        if not self._host_ok():
            self._send(HTTPStatus.MISDIRECTED_REQUEST, b"OmniQueue: unexpected Host header\n", "text/plain")
            return
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
        if path == "/api/projects/refresh":
            started = self.projects.request_refresh() if self.projects else False
            self._json({"ok": True, "started": started})
            return
        if path == "/api/projects/queue/refresh":
            body = self._body()
            cluster = str(body.get("cluster"))[:64] if body.get("cluster") else None
            started = self.projects.request_queue_refresh(cluster) if self.projects else False
            self._json({"ok": True, "started": started})
            return
        if path == "/api/experimental/predict":
            self._predict()
            return
        if path.startswith("/api/forget/"):
            key = path[len("/api/forget/"):]
            removed = self.collector.history.forget(key)
            self.collector.request_refresh()
            self._json({"ok": removed})
            return
        self._json({"error": "not found"}, HTTPStatus.NOT_FOUND)


    def _body(self, limit: int = 64 * 1024) -> dict:
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            length = 0
        if length <= 0 or length > limit:
            return {}
        try:
            data = json.loads(self.rfile.read(length).decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            return {}
        return data if isinstance(data, dict) else {}

    def _predict(self) -> None:
        """Experimental: rank clusters/partitions by estimated queue wait for a job."""
        body = self._body()
        try:
            req = Request(
                nodes=max(1, int(body.get("nodes", 1))),
                hours=max(0.01, float(body.get("hours", 1))),
                cores=(max(1, int(body["cores"])) if body.get("cores") else None),
                gpus=max(0, int(body.get("gpus") or 0)),
                projects=[str(x)[:64] for x in body["projects"]][:32] if isinstance(body.get("projects"), list) and body["projects"] else None,
                clusters=[str(x)[:64] for x in body["clusters"]][:32] if isinstance(body.get("clusters"), list) and body["clusters"] else None,
                partitions=[str(x)[:64] for x in body["partitions"]][:32] if isinstance(body.get("partitions"), list) and body["partitions"] else None,
            )
        except (TypeError, ValueError):
            self._json({"error": "nodes, hours and cores must be numbers"}, HTTPStatus.BAD_REQUEST)
            return
        if self.projects is None:
            self._json({"request": req.__dict__, "candidates": [], "excluded": [],
                        "notes": ["no cluster lists `projects` in the config, so no load samples are collected"]})
            return
        self._json(predict(req, self.projects.prediction_data()))


def make_server(collector: Collector, host: str, port: int, access_token: str | None = None,
                projects: ProjectPoller | None = None) -> ThreadingHTTPServer:
    loopback = host in ("127.0.0.1", "::1", "localhost")
    handler = type("BoundHandler", (Handler,), {
        "collector": collector,
        "projects": projects,
        "csrf_token": secrets.token_urlsafe(32),
        "access_token": access_token,
        # on loopback only these names reach the dashboard; a remote listener relies on the access token
        "allowed_hosts": frozenset({"127.0.0.1", "::1", "localhost", host.lower()}) if loopback else None,
    })
    return ThreadingHTTPServer((host, port), handler)
