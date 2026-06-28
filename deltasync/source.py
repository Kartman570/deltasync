"""A deterministic, generated HTTP change feed with consistent snapshots."""
import json
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from urllib.parse import parse_qs, urlsplit
from urllib.request import urlopen


def events():
    result = []
    for index in range(1, 41):
        result.append({"id": str(index), "value": {"name": f"Example {index}", "score": index}})
    for index in range(1, 21):
        result.append({"id": str(index), "value": {"name": f"Example {index}", "score": index * 10}})
    for index in range(3, 41, 3):
        result.append({"id": str(index), "value": None})
    for seq, event in enumerate(result, 1):
        event["seq"] = seq
    return result


def snapshot(changes):
    state = {}
    for event in changes:
        if event["value"] is None:
            state.pop(event["id"], None)
        else:
            state[event["id"]] = event
    return list(state.values())


@contextmanager
def serve(changes=None):
    changes = events() if changes is None else changes
    rows = snapshot(changes)

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            request = urlsplit(self.path)
            query = parse_qs(request.query)
            try:
                offset = int(query.get("cursor", ["0"])[0])
                limit = int(query.get("limit", ["7"])[0])
                data = changes if request.path == "/changes" else rows
                if request.path not in ("/changes", "/snapshot"):
                    self.send_error(404)
                    return
                if not 1 <= limit <= 100 or not 0 <= offset <= len(data):
                    raise ValueError("invalid cursor or page size")
                page = data[offset:offset + limit]
                end = offset + len(page)
                body = json.dumps({"items": page, "next": end,
                                   "done": end == len(data), "watermark": len(changes)}).encode()
            except ValueError:
                self.send_error(400)
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def fetch(base, resource, cursor, limit):
    with urlopen(f"{base}/{resource}?cursor={cursor}&limit={limit}", timeout=5) as response:
        return json.load(response)
