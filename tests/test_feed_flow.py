"""End-to-end: upload the example feed through the API, let a real Dramatiq
worker process it (stub broker, real PostgreSQL) and read everything back."""

import time
import uuid
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select

from feedservice import tasks
from feedservice.api import app
from feedservice.config import settings
from feedservice.db import Feed, Image, Item, ItemImage, session_scope

from .conftest import PNG

EXAMPLE = Path(__file__).parent.parent / "examples" / "feed_example.xml"
XML = {"Content-Type": "application/xml"}
ORIGIN = "https://www.bodylab.no/images/products/"


def example_feed(base_url: str) -> bytes:
    """The example feed with its (dead) image URLs pointed at the test server."""
    xml = EXAMPLE.read_text(encoding="utf-8")
    for original, local in {
        "mens-t-shirt-black-p.jpg": "ok.png",  # used by all 3 items
        "mens-t-shirt-micro-chip-p.jpg": "flaky.png",  # 503 once, then OK
        "minimum-deluxe-protein-bar-chocolate-chip-cookie-dough-p.png": "missing.png",  # 404
        "protein-marzipan-p.png": "page.html",  # 200 but text/html
    }.items():
        xml = xml.replace(ORIGIN + original, f"{base_url}/{local}")
    return xml.encode()


@pytest.fixture
def client():
    with TestClient(app) as c:
        yield c


def wait_until_finished(client, feed_id, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        body = client.get(f"/feeds/{feed_id}").json()
        if body["status"] != "processing":
            return body
        time.sleep(0.1)
    pytest.fail(f"feed still processing: {body}")


def test_feed_is_processed_in_background(client, worker, image_server):
    resp = client.post("/feeds", content=example_feed(image_server.base_url), headers=XML)
    assert resp.status_code == 202
    feed_id = resp.json()["id"]
    assert resp.headers["location"] == f"/feeds/{feed_id}"

    feed = wait_until_finished(client, feed_id)
    assert feed["status"] == "done"
    assert feed["items"] == 3
    # 4 distinct URLs: the shared main image is stored once.
    assert feed["images"] == {"pending": 0, "downloaded": 2, "failed": 2}
    assert "invalid XML after 3 items" in feed["error"]  # example is missing </rss>

    assert client.get(f"/feeds/{feed_id}/items").json() == ["6445", "6446", "M0296"]

    item = client.get(f"/feeds/{feed_id}/items/M0296").json()
    assert item["title"] == "Bodylab Men''s T-shirt - Black"
    assert (item["price"], item["currency"], item["availability"]) == ("299.00", "NOK", "in stock")
    assert [i["status"] for i in item["images"]] == ["downloaded", "downloaded", "failed"]

    image_ids = client.get(f"/feeds/{feed_id}/images").json()
    assert len(image_ids) == 2
    img = client.get(f"/feeds/{feed_id}/images/{image_ids[0]}")
    assert img.status_code == 200
    assert img.headers["content-type"] == "image/png"
    assert img.content == PNG
    assert "immutable" in img.headers["cache-control"]
    cached = client.get(f"/feeds/{feed_id}/images/{image_ids[0]}", headers={"If-None-Match": img.headers["etag"]})
    assert cached.status_code == 304
    assert image_server.hits["/flaky.png"] == 2  # retried after the 503
    assert image_server.hits["/ok.png"] == 1  # shared image downloaded once


def test_duplicate_messages_do_not_duplicate_data(client, worker, image_server):
    """At-least-once delivery: the same process_feed message delivered twice
    (and therefore every fetch_image message twice) must not create
    duplicates or break the feed status."""
    feed_id = uuid.uuid4()
    settings.feeds_dir.mkdir(parents=True, exist_ok=True)
    (settings.feeds_dir / f"{feed_id}.xml").write_bytes(example_feed(image_server.base_url))
    with session_scope() as s:
        s.add(Feed(id=feed_id))

    tasks.process_feed.send(str(feed_id))
    tasks.process_feed.send(str(feed_id))  # the duplicate
    feed = wait_until_finished(client, feed_id)
    tasks.broker.join(tasks.fetch_image.queue_name)
    worker.join()

    # A late redelivery after the feed is done is a no-op as well.
    tasks.process_feed.send(str(feed_id))
    tasks.broker.join(tasks.process_feed.queue_name)
    worker.join()

    assert feed["status"] == "done"
    assert feed["images"] == {"pending": 0, "downloaded": 2, "failed": 2}
    with session_scope() as s:
        count = lambda model: s.scalar(select(func.count()).select_from(model))  # noqa: E731
        assert (count(Item), count(Image), count(ItemImage)) == (3, 4, 6)
    assert client.get(f"/feeds/{feed_id}").json() == feed


def test_pagination(client, worker, image_server):
    feed_id = client.post("/feeds", content=example_feed(image_server.base_url), headers=XML).json()["id"]
    wait_until_finished(client, feed_id)

    first = client.get(f"/feeds/{feed_id}/items", params={"limit": 2})
    assert first.json() == ["6445", "6446"]
    assert 'rel="next"' in first.headers["link"]
    second = client.get(f"/feeds/{feed_id}/items", params={"limit": 2, "after": "6446"})
    assert second.json() == ["M0296"]
    assert "link" not in second.headers


def test_upload_validation(client):
    assert client.post("/feeds", content=b"<rss/>", headers={"Content-Type": "text/plain"}).status_code == 415
    assert client.post("/feeds", content=b"", headers=XML).status_code == 400
    resp = client.post("/feeds", content=b'{"json": true}', headers=XML)
    assert resp.status_code == 422
    assert resp.json() == {"error": {"status": 422, "message": "body is not XML"}}

    limit = settings.max_feed_bytes
    settings.max_feed_bytes = 10
    try:
        assert client.post("/feeds", content=b"<rss>" + b" " * 100 + b"</rss>", headers=XML).status_code == 413
    finally:
        settings.max_feed_bytes = limit


def test_unknown_resources_return_404(client, worker, image_server):
    missing = uuid.uuid4()
    resp = client.get(f"/feeds/{missing}")
    assert resp.status_code == 404
    assert resp.json() == {"error": {"status": 404, "message": "feed not found"}}
    assert client.get(f"/feeds/{missing}/items").status_code == 404
    assert client.get("/feeds/not-a-uuid").status_code == 422

    feed_id = client.post("/feeds", content=example_feed(image_server.base_url), headers=XML).json()["id"]
    wait_until_finished(client, feed_id)
    assert client.get(f"/feeds/{feed_id}/items/nope").status_code == 404
    assert client.get(f"/feeds/{feed_id}/images/{missing}").status_code == 404
