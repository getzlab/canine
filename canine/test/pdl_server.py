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
        self.throttle_bytes = None     # write in blocks of this size...
        self.throttle_delay = 0.0      # ...sleeping this long between them
        self.fail_next = 0             # return 500 for the next N requests

    def snapshot(self):
        with self.lock:
            return {"sent": self.sent, "requests": self.requests,
                    "range_requests": self.range_requests}


def make_handler(state):
    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):
            pass

        def _count(self, n, ranged):
            with state.lock:
                state.sent += n
                state.requests += 1
                if ranged:
                    state.range_requests += 1

        def do_GET(self):
            import time

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
            ranged = header_range is not None and state.support_range

            if ranged:
                spec = header_range.split("=", 1)[1]
                first, _, last = spec.partition("-")
                start = int(first)
                end = int(last) if last else total - 1
                end = min(end, total - 1)
                body = state.payload[start:end + 1]
                self.send_response(206)
                self.send_header("Content-Range",
                                 "bytes {}-{}/{}".format(start, end, total))
            else:
                # A server that ignores Range returns the whole object with 200 -- the
                # gzip-transcoded GCS case, which is billed per full object.
                body = state.payload
                self.send_response(200)

            if state.support_range:
                self.send_header("Accept-Ranges", "bytes")
            if state.content_md5:
                self.send_header("Content-MD5", state.content_md5)

            limit = len(body) if state.drop_after is None else min(len(body), state.drop_after)

            if not state.omit_content_length:
                self.send_header("Content-Length", str(len(body)))
            self.end_headers()

            written = 0
            block = state.throttle_bytes or len(body) or 1
            try:
                while written < limit:
                    piece = body[written:written + block]
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

        do_HEAD = do_GET

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
