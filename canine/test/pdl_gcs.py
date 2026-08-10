"""
A fake GCS JSON API, enough of it to exercise Route B.

Not named test_* so pytest does not collect it.

Implements the parts the downloader actually depends on: resumable upload sessions with
the 308/Range status protocol, objects.compose, get, delete, and ranged media reads. The
session semantics are the crux of Route B's resumability claim, so they are modelled
faithfully rather than stubbed:

  * bytes persist at 256 KiB granularity, so an interrupted PUT commits only whole
    granules and the reported offset can lag what was sent;
  * an incomplete upload is invisible -- no object exists until the session completes;
  * persisted bytes are never overwritten, so re-sending a committed range is harmless;
  * a session can be expired to model the 404/410 case.
"""

import http.server
import json
import re
import threading
import urllib.parse

GRANULARITY = 256 * 1024


class FakeGcs:
    """State for the fake service. Thread-safe; the downloader hits it concurrently."""

    def __init__(self):
        self.objects = {}            # name -> bytes
        self.composite = {}          # name -> component count
        self.sessions = {}           # id -> {"name", "buf", "committed", "expired"}
        self.compose_calls = []      # [(destination, [sources])]
        self.deleted = []
        self.lock = threading.Lock()
        self._next_session = 1

        # knobs
        self.commit_granularity = GRANULARITY
        self.fail_next_upload = 0    # return 503 for the next N uploads
        self.truncate_uploads_to = None   # commit only this many bytes per PUT
        # Succeed for this many uploads, then fail everything after. This is the knob
        # that models "the attempt died partway through": some parts are genuinely
        # partially persisted, which fail_next_upload cannot produce because it refuses
        # the *first* N and so leaves nothing committed.
        self.fail_uploads_after = None
        self.uploads_seen = 0

    def new_session(self, name):
        with self.lock:
            session_id = str(self._next_session)
            self._next_session += 1
            self.sessions[session_id] = {
                "name": name, "buf": bytearray(), "committed": 0, "expired": False,
            }
            return session_id

    def expire_session(self, session_id):
        with self.lock:
            if session_id in self.sessions:
                self.sessions[session_id]["expired"] = True

    def object_names(self):
        with self.lock:
            return sorted(self.objects)


def make_handler(state):
    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):
            pass

        # -- helpers --------------------------------------------------------

        def _send(self, status, body=b"", headers=None):
            if isinstance(body, str):
                body = body.encode("utf-8")
            self.send_response(status)
            for key, value in (headers or {}).items():
                self.send_header(key, value)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if body:
                self.wfile.write(body)

        def _json(self, status, payload):
            self._send(status, json.dumps(payload), {"Content-Type": "application/json"})

        def _body(self):
            length = int(self.headers.get("Content-Length") or 0)
            return self.rfile.read(length) if length else b""

        @staticmethod
        def _object_metadata(name, data, components=None):
            import base64
            import hashlib
            payload = {
                "name": name,
                "size": str(len(data)),
                "crc32c": "AAAAAA==",
            }
            if components:
                payload["componentCount"] = components
            else:
                payload["md5Hash"] = base64.b64encode(
                    hashlib.md5(data).digest()).decode()
            return payload

        # -- routing --------------------------------------------------------

        def do_POST(self):
            parsed = urllib.parse.urlsplit(self.path)
            query = urllib.parse.parse_qs(parsed.query)

            if query.get("uploadType") == ["resumable"]:
                self._body()
                name = query.get("name", [""])[0]
                session_id = state.new_session(name)
                self._send(200, b"", {
                    "Location": "http://{}/upload/session/{}".format(
                        self.headers.get("Host"), session_id)
                })
                return

            compose = re.match(r"^/storage/v1/b/([^/]+)/o/(.+)/compose$", parsed.path)
            if compose:
                destination = urllib.parse.unquote(compose.group(2))
                payload = json.loads(self._body().decode("utf-8"))
                sources = [s["name"] for s in payload["sourceObjects"]]
                with state.lock:
                    state.compose_calls.append((destination, list(sources)))
                    missing = [s for s in sources if s not in state.objects]
                    if missing:
                        self._json(404, {"error": {"message": "missing " + missing[0]}})
                        return
                    data = b"".join(state.objects[s] for s in sources)
                    components = sum(
                        state.composite.get(s, 1) for s in sources
                    )
                    state.objects[destination] = data
                    state.composite[destination] = components
                self._json(200, self._object_metadata(destination, data, components))
                return

            self._json(404, {"error": {"message": "unhandled POST " + parsed.path}})

        def do_PUT(self):
            parsed = urllib.parse.urlsplit(self.path)
            match = re.match(r"^/upload/session/(\d+)$", parsed.path)
            if not match:
                self._json(404, {"error": {"message": "no such session"}})
                return
            session_id = match.group(1)

            with state.lock:
                session = state.sessions.get(session_id)
                if session is None or session["expired"]:
                    # a lost or expired session: only this part has to start over
                    self._json(410, {"error": {"message": "session gone"}})
                    return

            body = self._body()
            content_range = (self.headers.get("Content-Range") or "").strip()

            # status query: "bytes */TOTAL" with no body
            status_query = re.match(r"^bytes \*/(\d+)$", content_range)
            if status_query:
                total = int(status_query.group(1))
                with state.lock:
                    committed = session["committed"]
                    if committed >= total and total > 0:
                        self._json(200, self._object_metadata(
                            session["name"], bytes(session["buf"])))
                        return
                if committed == 0:
                    self._send(308)
                else:
                    self._send(308, b"", {"Range": "bytes=0-{}".format(committed - 1)})
                return

            upload = re.match(r"^bytes (\d+)-(\d+)/(\d+)$", content_range)
            if not upload:
                self._json(400, {"error": {"message": "bad Content-Range"}})
                return
            start, end, total = (int(upload.group(i)) for i in (1, 2, 3))

            with state.lock:
                state.uploads_seen += 1
                if state.fail_next_upload > 0:
                    state.fail_next_upload -= 1
                    self._json(503, {"error": {"message": "try again"}})
                    return
                if (state.fail_uploads_after is not None
                        and state.uploads_seen > state.fail_uploads_after):
                    self._json(503, {"error": {"message": "attempt abandoned"}})
                    return

                # persisted bytes are never overwritten; re-sending is harmless
                if start > session["committed"]:
                    self._send(308, b"", {
                        "Range": "bytes=0-{}".format(session["committed"] - 1)
                    } if session["committed"] else {})
                    return

                buf = session["buf"]
                if len(buf) < end + 1:
                    buf.extend(b"\0" * (end + 1 - len(buf)))
                buf[start:end + 1] = body

                sent_to = end + 1
                if state.truncate_uploads_to is not None:
                    sent_to = min(sent_to, start + state.truncate_uploads_to)

                if sent_to >= total:
                    committed = total
                else:
                    # only whole granules persist
                    committed = (sent_to // state.commit_granularity) * \
                        state.commit_granularity
                    committed = max(committed, session["committed"])
                session["committed"] = committed

                if committed >= total:
                    data = bytes(buf[:total])
                    state.objects[session["name"]] = data
                    state.composite[session["name"]] = 1
                    del state.sessions[session_id]
                    self._json(200, self._object_metadata(session["name"], data))
                    return

            self._send(308, b"", {"Range": "bytes=0-{}".format(committed - 1)}
                       if committed else {})

        def do_GET(self):
            parsed = urllib.parse.urlsplit(self.path)
            query = urllib.parse.parse_qs(parsed.query)
            match = re.match(r"^/storage/v1/b/([^/]+)/o/(.+)$", parsed.path)
            if not match:
                self._json(404, {"error": {"message": "unhandled GET " + parsed.path}})
                return
            name = urllib.parse.unquote(match.group(2))

            with state.lock:
                if name not in state.objects:
                    self._json(404, {"error": {"message": "no such object"}})
                    return
                data = state.objects[name]
                components = state.composite.get(name, 1)

            if query.get("alt") == ["media"]:
                header_range = self.headers.get("Range")
                if header_range:
                    spec = header_range.split("=", 1)[1]
                    first, _, last = spec.partition("-")
                    start = int(first)
                    end = min(int(last), len(data) - 1) if last else len(data) - 1
                    self._send(206, data[start:end + 1], {
                        "Content-Range": "bytes {}-{}/{}".format(start, end, len(data))
                    })
                else:
                    self._send(200, data)
                return

            self._json(200, self._object_metadata(
                name, data, components if components > 1 else None))

        def do_DELETE(self):
            parsed = urllib.parse.urlsplit(self.path)
            match = re.match(r"^/storage/v1/b/([^/]+)/o/(.+)$", parsed.path)
            if not match:
                self._send(404)
                return
            name = urllib.parse.unquote(match.group(2))
            with state.lock:
                state.deleted.append(name)
                state.objects.pop(name, None)
                state.composite.pop(name, None)
            self._send(204)

    return Handler


class GcsServer:
    def __init__(self):
        self.state = FakeGcs()
        self._httpd = http.server.ThreadingHTTPServer(
            ("127.0.0.1", 0), make_handler(self.state)
        )
        self._httpd.daemon_threads = True
        threading.Thread(target=self._httpd.serve_forever, daemon=True).start()

    @property
    def root(self):
        return "http://127.0.0.1:{}".format(self._httpd.server_address[1])

    def close(self):
        self._httpd.shutdown()
        self._httpd.server_close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


def patch_client_endpoints(monkeypatch, pdl, server):
    """Point the downloader's GCS client at the fake service and stub out auth."""
    monkeypatch.setattr(pdl, "GCS_API_ROOT", server.root + "/storage/v1")
    monkeypatch.setattr(pdl, "GCS_UPLOAD_ROOT", server.root + "/upload/storage/v1")
    monkeypatch.setattr(pdl.GcsClient, "token", lambda self: "fake-token")
