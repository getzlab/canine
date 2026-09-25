"""
A fake GCS JSON API, enough of it to exercise the bucket-compose route.

Not named test_* so pytest does not collect it.

Implements the parts the downloader actually depends on: resumable upload sessions with
the 308/Range status protocol, objects.compose, get, delete, and ranged media reads. The
session semantics are the crux of the bucket-compose route's resumability claim, so they are modelled
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
import time
import urllib.parse

GRANULARITY = 256 * 1024


class FakeGcs:
    """State for the fake service. Thread-safe; the downloader hits it concurrently."""

    def __init__(self):
        self.objects = {}            # name -> bytes
        self.composite = {}          # name -> component count
        # name -> creation time (epoch s). Real GCS reports timeCreated per generation,
        # and the race handling uses it to tell a winner's fresh object from a stale
        # one. Objects injected by a test without an entry read as epoch 0 -- stale --
        # which is the safe default: a test must opt in to "this just landed".
        self.created = {}
        # name -> the customTime the writer set, or absent if it set none. Recorded so
        # tests can prove every write path stamps it: an object without one is invisible
        # to the bucket's daysSinceCustomTime lifecycle rule and is never deleted.
        self.custom_time = {}
        # Every object write, in order, with the customTime it carried (None if none).
        # Most of what this module writes -- parts, slices, sidecars, intermediates -- is
        # deleted before a test can look at it, so the stamp is checked here, at write.
        self.writes = []
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
        # PUTs that declared their total as "*". Counted so a test can prove the
        # unknown-total path was actually taken rather than assuming it.
        self.unknown_total_puts = 0
        # DELETE of a name matching any of these substrings returns 503 until the
        # count runs out. Models a cleanup request hitting a transient GCS error
        # AFTER the object is complete -- which used to fail the whole job.
        self.fail_deletes_matching = ()
        self.fail_deletes_remaining = 0
        # Objects to remove the first time a media GET touches them, before serving.
        # Models another writer cleaning up a sidecar or part mid-read.
        self.vanish_on_read = set()
        # name -> (shape, decoded bytes) for a gzip-encoded object, whose STORED bytes
        # are state.objects[name]. Shapes follow the matrix measured on the real JSON
        # media endpoint (§13.71), deterministic in every cell:
        #   "no-transform": stored bytes always, ranges honoured;
        #   "ordinary":     stored + ranged with Accept-Encoding: gzip, else decoded
        #                   with the Range ignored;
        #   "typed-gzip":   (Content-Type application/gzip) stored bytes only with
        #                   Accept-Encoding: gzip AND a Range, and then whole (200,
        #                   Range ignored); decoded otherwise.
        self.encoded = {}
        # One entry per media GET: headers and query that matter to the decode.
        self.media_requests = []
        # serve ranged media this many bytes later than asked, header matching the body
        self.shift_range = 0
        # Tokens GCS no longer accepts: a request bearing one gets 401 Invalid Credentials,
        # as an expired access token does (measured, §13.77). Counted per token.
        self.rejected_tokens = set()
        self.rejections = {}
        # The client port of every media GET, one per TCP connection, so a test can tell a
        # kept-alive connection from a new one per request.
        self.media_ports = []
        # Close the connection after every response, as a server does to an idle
        # keep-alive connection: the client's next request on it fails.
        self.close_after_response = False

    def new_session(self, name, custom_time=None):
        with self.lock:
            session_id = str(self._next_session)
            self._next_session += 1
            self.sessions[session_id] = {
                "name": name, "buf": bytearray(), "committed": 0, "expired": False,
                "custom_time": custom_time,
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
            if state.close_after_response:
                # no Connection: close header -- the client only finds out on reuse
                self.close_connection = True

        def _rejected(self):
            """401 for a token the state marks expired; True if the request was answered."""
            auth = self.headers.get("Authorization") or ""
            token = auth[len("Bearer "):] if auth.startswith("Bearer ") else None
            if token is not None and token in state.rejected_tokens:
                with state.lock:
                    state.rejections[token] = state.rejections.get(token, 0) + 1
                self._json(401, {"error": {"code": 401, "message": "Invalid Credentials"}})
                return True
            return False

        def _json(self, status, payload):
            self._send(status, json.dumps(payload), {"Content-Type": "application/json"})

        def _body(self):
            length = int(self.headers.get("Content-Length") or 0)
            return self.rfile.read(length) if length else b""

        @staticmethod
        def _stamp(name, custom_time):
            # A new generation has only the metadata its own write supplied.
            state.writes.append((name, custom_time))
            if custom_time:
                state.custom_time[name] = custom_time
            else:
                state.custom_time.pop(name, None)

        @staticmethod
        def _object_metadata(name, data, components=None):
            import base64
            import datetime
            import hashlib
            created = datetime.datetime.fromtimestamp(
                state.created.get(name, 0), tz=datetime.timezone.utc)
            payload = {
                "name": name,
                "size": str(len(data)),
                "crc32c": "AAAAAA==",
                "timeCreated": created.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            }
            if name in state.custom_time:
                payload["customTime"] = state.custom_time[name]
            if components:
                payload["componentCount"] = components
            else:
                payload["md5Hash"] = base64.b64encode(
                    hashlib.md5(data).digest()).decode()
            return payload

        # -- routing --------------------------------------------------------

        def do_POST(self):
            if self._rejected():
                return
            parsed = urllib.parse.urlsplit(self.path)
            query = urllib.parse.parse_qs(parsed.query)

            if query.get("uploadType") == ["media"]:
                # one-shot media upload: the object appears whole or not at all, which is
                # the atomicity the manifest depends on
                name = query.get("name", [""])[0]
                body = self._body()
                with state.lock:
                    state.objects[name] = body
                    state.composite[name] = 1
                    state.created[name] = time.time()
                self._json(200, self._object_metadata(name, body))
                return

            if query.get("uploadType") == ["multipart"]:
                # metadata part + media part, one request, atomic like media
                boundary = re.search(r"boundary=([^;]+)",
                                     self.headers.get("Content-Type", "")).group(1)
                raw = self._body()
                pieces = raw.split(b"--" + boundary.encode())
                meta_part, media_part = pieces[1], pieces[2]
                meta = json.loads(meta_part.split(b"\r\n\r\n", 1)[1].rsplit(b"\r\n", 1)[0])
                body = media_part.split(b"\r\n\r\n", 1)[1][:-2]   # strip trailing CRLF
                name = meta["name"]
                with state.lock:
                    state.objects[name] = body
                    state.composite[name] = 1
                    state.created[name] = time.time()
                    self._stamp(name, meta.get("customTime"))
                self._json(200, self._object_metadata(name, body))
                return

            if query.get("uploadType") == ["resumable"]:
                raw = self._body()
                name = query.get("name", [""])[0]
                try:
                    init = json.loads(raw.decode("utf-8")) if raw else {}
                except ValueError:
                    init = {}
                session_id = state.new_session(name, init.get("customTime"))
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
                    self._stamp(destination,
                                (payload.get("destination") or {}).get("customTime"))
                    state.created[destination] = time.time()
                self._json(200, self._object_metadata(destination, data, components))
                return

            self._json(404, {"error": {"message": "unhandled POST " + parsed.path}})

        def do_PUT(self):
            if self._rejected():
                return
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
                    if committed >= total:
                        # total == 0 finalizes an empty object, which is how a resumable
                        # session is completed when the last data PUT already sent
                        # everything -- the gunzip pass ends that way whenever the
                        # decoded length lands on a granule boundary.
                        data = bytes(session["buf"][:total])
                        state.objects[session["name"]] = data
                        state.composite[session["name"]] = 1
                        self._stamp(session["name"], session.get("custom_time"))
                        state.created[session["name"]] = time.time()
                        del state.sessions[session_id]
                        self._json(200, self._object_metadata(session["name"], data))
                        return
                if committed == 0:
                    self._send(308)
                else:
                    self._send(308, b"", {"Range": "bytes=0-{}".format(committed - 1)})
                return

            upload = re.match(r"^bytes (\d+)-(\d+)/(\d+|\*)$", content_range)
            if not upload:
                self._json(400, {"error": {"message": "bad Content-Range"}})
                return
            start, end = int(upload.group(1)), int(upload.group(2))
            # "*" means the final length is not known yet, which the gunzip pass needs:
            # the decompressed size cannot be computed without decompressing. GCS accepts
            # such a PUT only when its length is a multiple of the 256 KiB commit
            # granularity, and rejects it otherwise -- modelled here so a caller that
            # buffers wrongly fails loudly instead of silently working against the fake.
            if upload.group(3) == "*":
                with state.lock:
                    state.unknown_total_puts += 1
                if (end + 1 - start) % state.commit_granularity:
                    self._json(400, {"error": {"message": (
                        "chunk of unknown total must be a multiple of {}".format(
                            state.commit_granularity))}})
                    return
                total = None
            else:
                total = int(upload.group(3))

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

                if total is not None and sent_to >= total:
                    committed = total
                else:
                    # only whole granules persist
                    committed = (sent_to // state.commit_granularity) * \
                        state.commit_granularity
                    committed = max(committed, session["committed"])
                session["committed"] = committed

                if total is not None and committed >= total:
                    data = bytes(buf[:total])
                    state.objects[session["name"]] = data
                    state.composite[session["name"]] = 1
                    self._stamp(session["name"], session.get("custom_time"))
                    state.created[session["name"]] = time.time()
                    del state.sessions[session_id]
                    self._json(200, self._object_metadata(session["name"], data))
                    return

            self._send(308, b"", {"Range": "bytes=0-{}".format(committed - 1)}
                       if committed else {})

        def do_GET(self):
            if "alt=media" in self.path:
                with state.lock:
                    state.media_ports.append(self.client_address[1])
            if self._rejected():
                return
            parsed = urllib.parse.urlsplit(self.path)
            query = urllib.parse.parse_qs(parsed.query)
            match = re.match(r"^/storage/v1/b/([^/]+)/o/(.+)$", parsed.path)
            if not match:
                self._json(404, {"error": {"message": "unhandled GET " + parsed.path}})
                return
            name = urllib.parse.unquote(match.group(2))

            with state.lock:
                if query.get("alt") == ["media"] and name in state.vanish_on_read:
                    state.vanish_on_read.discard(name)
                    state.objects.pop(name, None)
                    state.composite.pop(name, None)
                if name not in state.objects:
                    self._json(404, {"error": {"message": "no such object"}})
                    return
                data = state.objects[name]
                components = state.composite.get(name, 1)

            if query.get("alt") == ["media"]:
                header_range = self.headers.get("Range")
                accepts_gzip = "gzip" in (self.headers.get("Accept-Encoding") or "")
                with state.lock:
                    state.media_requests.append({
                        "name": name, "range": header_range, "accepts_gzip": accepts_gzip,
                        "user_project": (query.get("userProject") or [None])[0],
                        "authorized": bool(self.headers.get("Authorization")),
                    })
                if name in state.encoded:
                    shape, decoded = state.encoded[name]
                    stored_ok = (shape == "no-transform"
                                 or (shape == "ordinary" and accepts_gzip)
                                 or (shape == "typed-gzip" and accepts_gzip
                                     and header_range))
                    if not stored_ok:
                        self._send(200, decoded)            # decoded, Range ignored
                        return
                    if shape == "typed-gzip":
                        self._send(200, data, {"Content-Encoding": "gzip"})
                        return
                    if not header_range:
                        self._send(200, data, {"Content-Encoding": "gzip"})
                        return
                if header_range:
                    spec = header_range.split("=", 1)[1]
                    first, _, last = spec.partition("-")
                    start = int(first) + state.shift_range
                    end = min(int(last) + state.shift_range, len(data) - 1) if last \
                        else len(data) - 1
                    extra = {"Content-Range": "bytes {}-{}/{}".format(start, end, len(data))}
                    if name in state.encoded:
                        extra["Content-Encoding"] = "gzip"
                    self._send(206, data[start:end + 1], extra)
                else:
                    self._send(200, data)
                return

            self._json(200, self._object_metadata(
                name, data, components if components > 1 else None))

        def do_DELETE(self):
            if self._rejected():
                return
            parsed = urllib.parse.urlsplit(self.path)
            match = re.match(r"^/storage/v1/b/([^/]+)/o/(.+)$", parsed.path)
            if not match:
                self._send(404)
                return
            name = urllib.parse.unquote(match.group(2))
            with state.lock:
                if (state.fail_deletes_remaining > 0
                        and any(m in name for m in state.fail_deletes_matching)):
                    state.fail_deletes_remaining -= 1
                    self._send(503)
                    return
                state.deleted.append(name)
                state.objects.pop(name, None)
                state.composite.pop(name, None)
                state.custom_time.pop(name, None)
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
