import os
import tempfile

# Configure before the app modules are imported (they read settings at import).
os.environ.setdefault("BROKER_URL", "stub")
os.environ.setdefault("DATA_DIR", tempfile.mkdtemp(prefix="feedservice-test-"))
os.environ.setdefault("DATABASE_URL", "postgresql+psycopg://feeds:feeds@localhost:5432/feeds_test")
# Fast retries so the "flaky image" case does not slow the tests down.
os.environ.setdefault("TASK_MIN_BACKOFF_MS", "50")
os.environ.setdefault("TASK_MAX_BACKOFF_MS", "200")

import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from dramatiq import Worker
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url


def _ensure_test_database() -> None:
    url = make_url(os.environ["DATABASE_URL"])
    admin = create_engine(url.set(database="postgres"), isolation_level="AUTOCOMMIT")
    with admin.connect() as conn:
        if not conn.scalar(text("SELECT 1 FROM pg_database WHERE datname = :n"), {"n": url.database}):
            conn.execute(text(f'CREATE DATABASE "{url.database}"'))
    admin.dispose()


_ensure_test_database()

from feedservice import db, tasks  # noqa: E402

PNG = (
    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f"
    b"\x15\xc4\x89\x00\x00\x00\rIDATx\x9cc\xf8\x0f\x00\x00\x01\x01\x00\x05\x18\xd8N\x00\x00\x00"
    b"\x00IEND\xaeB`\x82"
)


@pytest.fixture(autouse=True)
def clean_db():
    db.init_db()
    yield
    with db.engine.begin() as conn:
        conn.execute(text("TRUNCATE feeds, items, images, item_images CASCADE"))


@pytest.fixture
def worker():
    tasks.broker.flush_all()
    w = Worker(tasks.broker, worker_timeout=50, worker_threads=4)
    w.start()
    yield w
    w.stop()


@pytest.fixture
def image_server():
    """Local HTTP server standing in for the customer's image host."""
    hits: dict[str, int] = {}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            hits[self.path] = hits.get(self.path, 0) + 1
            if self.path == "/ok.png":
                self._send(200, "image/png", PNG)
            elif self.path == "/flaky.png":  # fails once, then works
                if hits[self.path] == 1:
                    self._send(503, "text/plain", b"try later")
                else:
                    self._send(200, "image/png", PNG)
            elif self.path == "/page.html":
                self._send(200, "text/html", b"<html>not an image</html>")
            else:
                self._send(404, "text/plain", b"not found")

        def _send(self, code, ctype, body):
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    server.base_url = f"http://127.0.0.1:{server.server_port}"
    server.hits = hits
    yield server
    server.shutdown()
