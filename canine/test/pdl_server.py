"""
Test harness for parallel_download.py: a local HTTP server with controllable Range
behavior and a byte counter.

Not named test_* so pytest does not collect it.

The counter is what makes the resumability tests meaningful. "No committed work is
ever discarded" is a claim about how many bytes cross the wire across repeated
attempts, so the tests have to measure that rather than just check the final file.
"""

import http.server
import os
import subprocess
import sys
import threading

PDL_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "localization", "parallel_download.py",
)


class ServerState:
    """Shared, mutable knobs for the handler (which is instantiated per request)."""

    def __init__(self, payload):
        self.payload = payload
        self.sent = 0
        self.requests = 0
        self.range_requests = 0
        self.lock = threading.Lock()

        # knobs
        self.support_range = True      # False => ignore Range, return 200 + whole body
        self.omit_content_length = False
        self.drop_after = None         # close the connection after N bytes per response
        self.content_md5 = None        # advertise this base64 md5 in Content-MD5
        # Mimic GCS decompressive transcoding: Range is silently ignored, the whole object
        # comes back with 200, and BOTH Content-Encoding and Content-Length are omitted.
        # That last part is why transcoding cannot be detected by looking for the encoding
        # header -- it is absent precisely when transcoding is happening.
        self.transcoding = False
        # Mimic a gzip-STORED GCS object (cache-control: no-transform), verified against a
        # real one: the body is served compressed with `content-encoding: gzip`, there is
        # NO content-length at all, the size lives in x-goog-stored-content-length, and
        # the x-goog-hash digests cover the stored bytes. Ranges are honoured normally.
        self.stored_gzip = False
        # Reply with this status to every request instead of serving the payload. Lets a
        # test drive the 403 path, which is what an expired signature or a private object
        # produces in practice.
        self.force_status = None
        self.throttle_bytes = None     # write in blocks of this size...
        self.throttle_delay = 0.0      # ...sleeping this long between them
        self.fail_next = 0             # return 500 for the next N requests
        # Client source ports seen, one entry per accepted connection. Lets a test show
        # that requests do not share a socket, rather than asserting it from the docs.
        self.client_ports = set()

        # Server-side decoding the client did not ask for, both measured on real GCS
        # signed URLs (§13.70). `decoded_payload` is what such a response carries.
        self.decoded_payload = None
        # A gzip-encoded object with Content-Type application/gzip: a request WITHOUT a
        # Range header is served decoded even when it asks for gzip, while any Range gets
        # the stored bytes -- whole, with 200, the Range itself ignored.
        self.decode_unless_ranged = False
        # GCS's anonymous/edge path: decoded no matter what was asked for.
        self.always_decode = False
        # An ordinary gzip-encoded object (the common case): decoded -- Range ignored --
        # for any request that does not carry Accept-Encoding: gzip; stored bytes,
        # ranges honoured, for one that does.
        self.decode_unless_accepts_gzip = False
        # The Range header of every request, in order (None when absent).
        self.seen_ranges = []
        # Both measured on the GDC API (§13.72): its 206s carry `Content-Range: 0-0/N`,
        # with no `bytes ` unit, and it answers every HEAD with 400.
        self.content_range_unit = "bytes "
        self.reject_head = False
        # Serve the range starting this many bytes later than asked, header and body
        # consistent with each other -- so only a client comparing the reported range
        # with the requested one can tell.
        self.shift_range = 0
        self.heads = 0

    def snapshot(self):
        with self.lock:
            return {"sent": self.sent, "requests": self.requests,
                    "range_requests": self.range_requests,
                    "connections": len(self.client_ports)}


def make_handler(state):
    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):
            pass

        def _count(self, n, ranged):
            with state.lock:
                state.client_ports.add(self.client_address[1])
                state.sent += n
                state.requests += 1
                if ranged:
                    state.range_requests += 1

        def do_GET(self):
            import time

            with state.lock:
                forced = state.force_status
            if forced:
                self.send_response(forced)
                body = b"AccessDenied"
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            with state.lock:
                if state.fail_next > 0:
                    state.fail_next -= 1
                    fail = True
                else:
                    fail = False
            if fail:
                self.send_response(500)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return

            total = len(state.payload)
            header_range = self.headers.get("Range")
            with state.lock:
                state.seen_ranges.append(header_range)
            ranged = (header_range is not None and state.support_range
                      and not state.decode_unless_ranged)

            accepts_gzip = "gzip" in (self.headers.get("Accept-Encoding") or "")
            if state.decoded_payload is not None and (
                    state.always_decode
                    or (state.decode_unless_ranged and header_range is None)
                    or (state.decode_unless_accepts_gzip and not accepts_gzip)):
                body = state.decoded_payload
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                if state.stored_gzip:
                    # measured (§13.72): GCS describes the STORED object even on a
                    # response it decoded -- no Content-Encoding, but these two
                    self.send_header("x-goog-stored-content-length", str(total))
                    self.send_header("x-goog-stored-content-encoding", "gzip")
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(body)
                self._count(len(body), False)
                return

            if state.transcoding and "gzip" not in (
                self.headers.get("Accept-Encoding") or ""
            ):
                # ignore Range entirely and stream the whole object, with no
                # Content-Length -- and bill for all of it
                body = state.payload
                self.send_response(200)
                self.end_headers()
                try:
                    self.wfile.write(body)
                    self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError, OSError):
                    pass
                self._count(len(body), False)
                self.close_connection = True
                return

            if ranged:
                spec = header_range.split("=", 1)[1]
                first, _, last = spec.partition("-")
                start = int(first) + state.shift_range
                end = (int(last) if last else total - 1) + state.shift_range
                end = min(end, total - 1)
                body = state.payload[start:end + 1]
                self.send_response(206)
                self.send_header("Content-Range", "{}{}-{}/{}".format(
                    state.content_range_unit, start, end, total))
            else:
                # A server that ignores Range returns the whole object with 200 -- the
                # gzip-transcoded GCS case, which is billed per full object.
                body = state.payload
                self.send_response(200)

            if state.support_range:
                self.send_header("Accept-Ranges", "bytes")
            if state.stored_gzip:
                self.send_header("Content-Encoding", "gzip")
                self.send_header("Cache-Control", "no-transform")
                self.send_header("x-goog-stored-content-length", str(total))
                self.send_header("x-goog-stored-content-encoding", "gzip")
            if state.content_md5:
                self.send_header("Content-MD5", state.content_md5)

            limit = len(body) if state.drop_after is None else min(len(body), state.drop_after)

            if not state.omit_content_length and not state.stored_gzip:
                self.send_header("Content-Length", str(len(body)))
            elif state.stored_gzip:
                # the real service sends no content-length here, so the client must not
                # be able to rely on one
                self.close_connection = True
            self.end_headers()

            written = 0
            block = state.throttle_bytes or len(body) or 1
            try:
                while written < limit:
                    # Capped at `limit`, not just `block`. Without the cap, an unthrottled
                    # response has block == len(body), so the first write sends the WHOLE
                    # body and `drop_after` truncates nothing -- the loop just exits with
                    # everything already on the wire. That silently disarmed
                    # test_dropped_connection_mid_chunk_is_resumed, which was passing on a
                    # transfer that never dropped.
                    piece = body[written:min(written + block, limit)]
                    self.wfile.write(piece)
                    self.wfile.flush()
                    written += len(piece)
                    self._count(len(piece), ranged)
                    if state.throttle_delay:
                        time.sleep(state.throttle_delay)
            except (BrokenPipeError, ConnectionResetError, OSError):
                pass

            if limit < len(body):
                # simulate a mid-response connection drop
                try:
                    self.close_connection = True
                    self.wfile.close()
                except OSError:
                    pass

        def do_HEAD(self):
            with state.lock:
                state.heads += 1
            if state.reject_head:
                self.send_response(400)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            self.do_GET()

    return Handler


class Server:
    def __init__(self, payload):
        self.state = ServerState(payload)
        self._httpd = http.server.ThreadingHTTPServer(
            ("127.0.0.1", 0), make_handler(self.state)
        )
        self._httpd.daemon_threads = True
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)
        self._thread.start()

    @property
    def port(self):
        return self._httpd.server_address[1]

    def url(self, name="object.bin"):
        return "http://127.0.0.1:{}/{}".format(self.port, name)

    def close(self):
        self._httpd.shutdown()
        self._httpd.server_close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


def run_downloader(url, dest, size, *extra, **kwargs):
    """
    Invoke parallel_download.py as a subprocess, the way localization.sh does.

    A subprocess (rather than an in-process call) is essential: the resume tests kill
    it with SIGKILL, which cannot be simulated in-process.
    """
    cmd = [sys.executable, PDL_PATH, "--dest", dest, "--size", str(size)]
    if url is not None:
        cmd += ["--url", url]
    cmd += [str(a) for a in extra]
    proc = subprocess.run(
        cmd, capture_output=True, text=True, timeout=kwargs.pop("timeout", 300),
        env=kwargs.pop("env", None),
    )
    return proc


def spawn_downloader(url, dest, size, *extra, **kwargs):
    """Start the downloader without waiting, so a test can kill it mid-transfer."""
    cmd = [sys.executable, PDL_PATH, "--dest", dest, "--size", str(size)]
    if url is not None:
        cmd += ["--url", url]
    cmd += [str(a) for a in extra]
    return subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        env=kwargs.pop("env", None),
    )
