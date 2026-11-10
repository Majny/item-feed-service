"""End-to-end: upload the example feed through the API, let a real Dramatiq
worker process it (stub broker, real PostgreSQL) and read everything back."""

import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select

from feedservice import tasks
from feedservice.api import app
from feedservice.config import settings
from feedservice.db import Feed, Image, ImageStatus, Item, ItemImage, image_id_for, session_scope

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
    body = None
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
    assert item["images"][2]["error"] == "HTTP 404"

    image_ids = client.get(f"/feeds/{feed_id}/images").json()
    assert len(image_ids) == 2
    img = client.get(f"/feeds/{feed_id}/images/{image_ids[0]}")
    assert img.status_code == 200
    assert img.headers["content-type"] == "image/png"
    assert img.content == PNG
    assert "immutable" in img.headers["cache-control"]
    assert img.headers["x-content-type-options"] == "nosniff"  # customer bytes, never sniffed
    assert "sandbox" in img.headers["content-security-policy"]
    etag = img.headers["etag"]
    for if_none_match in (etag, f'"other", W/{etag}', "*"):
        cached = client.get(f"/feeds/{feed_id}/images/{image_ids[0]}", headers={"If-None-Match": if_none_match})
        assert cached.status_code == 304
    assert image_server.hits["/flaky.png"] == 2  # retried after the 503
    assert image_server.hits["/ok.png"] == 1  # shared image downloaded once
    assert image_server.hits["/missing.png"] == 1  # 404 is permanent: not retried
    assert image_server.hits["/page.html"] == 1  # not an image: not retried


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


def test_upload_validation(client, monkeypatch):
    assert client.post("/feeds", content=b"<rss/>", headers={"Content-Type": "text/plain"}).status_code == 415
    assert client.post("/feeds", content=b"", headers=XML).status_code == 400
    resp = client.post("/feeds", content=b'{"json": true}', headers=XML)
    assert resp.status_code == 422
    assert resp.json() == {"error": {"status": 422, "message": "body is not XML"}}

    monkeypatch.setattr(settings, "max_feed_bytes", 10)
    assert client.post("/feeds", content=b"<rss>" + b" " * 100 + b"</rss>", headers=XML).status_code == 413


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


def test_redelivery_after_a_crash_still_finishes_the_feed(client, worker):
    """The worker committed the image status and died before the feed
    completion check. The redelivered message must run that check, otherwise
    the feed stays "processing" forever."""
    feed_id = uuid.uuid4()
    image_id = image_id_for(feed_id, "http://example.invalid/a.png")
    with session_scope() as s:
        s.add(Feed(id=feed_id, parsed_at=datetime.now(UTC)))
        s.flush()
        s.add(Image(id=image_id, feed_id=feed_id, url="http://example.invalid/a.png", status=ImageStatus.DOWNLOADED))

    tasks.fetch_image.send(str(image_id))
    assert wait_until_finished(client, feed_id)["status"] == "done"


def test_time_limit_on_the_last_attempt_is_recorded(client, worker, image_server, monkeypatch):
    """TimeLimitExceeded is a BaseException; it must still end as a failed
    image (and a finished feed), not as an image pending forever."""
    from dramatiq.middleware import TimeLimitExceeded

    def killed(*_args, **_kwargs):
        raise TimeLimitExceeded()

    monkeypatch.setattr(tasks, "download_image", killed)
    feed_id = client.post("/feeds", content=example_feed(image_server.base_url), headers=XML).json()["id"]
    feed = wait_until_finished(client, feed_id)
    assert feed["status"] == "done"
    assert feed["images"] == {"pending": 0, "downloaded": 0, "failed": 4}
    item = client.get(f"/feeds/{feed_id}/items/M0296").json()
    assert all(i["status"] == "failed" for i in item["images"])


def test_retried_upload_with_idempotency_key_creates_one_feed(client, worker, image_server):
    body = example_feed(image_server.base_url)
    headers = {**XML, "Idempotency-Key": "upload-1"}
    first = client.post("/feeds", content=body, headers=headers)
    retry = client.post("/feeds", content=body, headers=headers)  # e.g. the 202 got lost

    assert first.status_code == retry.status_code == 202
    assert retry.json() == first.json()
    assert retry.headers["idempotent-replayed"] == "true"
    other = client.post("/feeds", content=body, headers={**XML, "Idempotency-Key": "upload-2"})
    assert other.json() != first.json()
    with session_scope() as s:
        assert s.scalar(select(func.count()).select_from(Feed)) == 2


def test_queue_outage_returns_503_and_leaves_nothing_behind(client, monkeypatch):
    def broker_down(*_args, **_kwargs):
        raise ConnectionError("broker down")

    monkeypatch.setattr(tasks.process_feed, "send", broker_down)
    files_before = set(settings.feeds_dir.iterdir())
    resp = client.post("/feeds", content=b"<rss/>", headers=XML)

    assert resp.status_code == 503
    assert resp.headers["retry-after"]
    assert resp.json()["error"]["message"] == "queue unavailable, try again"
    with session_scope() as s:  # the client never got an ID: no orphan row or file
        assert s.scalar(select(func.count()).select_from(Feed)) == 0
    assert set(settings.feeds_dir.iterdir()) == files_before


def test_keyed_retry_after_a_queue_outage_publishes_again(client, worker, image_server, monkeypatch):
    """The first attempt fails to publish (or the client never sees the
    answer). The retry with the same key must not create a second feed and
    must not return a feed that is never processed."""
    send = tasks.process_feed.send

    def broker_down(*_args, **_kwargs):
        raise ConnectionError("broker down")

    headers = {**XML, "Idempotency-Key": "retry-me"}
    body = example_feed(image_server.base_url)
    monkeypatch.setattr(tasks.process_feed, "send", broker_down)
    assert client.post("/feeds", content=body, headers=headers).status_code == 503

    monkeypatch.setattr(tasks.process_feed, "send", send)  # the broker is back
    retry = client.post("/feeds", content=body, headers=headers)
    assert retry.status_code == 202
    assert wait_until_finished(client, retry.json()["id"])["status"] == "done"
    with session_scope() as s:
        assert s.scalar(select(func.count()).select_from(Feed)) == 1


def test_empty_idempotency_key_is_rejected(client):
    resp = client.post("/feeds", content=b"<rss/>", headers={**XML, "Idempotency-Key": ""})
    assert resp.status_code == 422
    assert resp.json()["error"]["details"]


def test_item_repeated_in_a_later_batch_keeps_only_its_last_images(client, worker, image_server, monkeypatch):
    monkeypatch.setattr(tasks, "BATCH_SIZE", 1)
    base = image_server.base_url
    xml = f"""<rss xmlns:g="http://base.google.com/ns/1.0"><channel>
<item><g:id>dup</g:id><g:image_link>{base}/ok.png</g:image_link>
      <g:additional_image_link>{base}/missing.png</g:additional_image_link></item>
<item><g:id>other</g:id></item>
<item><g:id>dup</g:id><title>second</title><g:image_link>{base}/flaky.png</g:image_link></item>
</channel></rss>""".encode()
    feed_id = client.post("/feeds", content=xml, headers=XML).json()["id"]
    assert wait_until_finished(client, feed_id)["status"] == "done"

    item = client.get(f"/feeds/{feed_id}/items/dup").json()
    assert item["title"] == "second"
    # Images of the superseded definition are still fetched (a known, harmless
    # leftover: they are just not linked to any item).
    assert [i["url"] for i in item["images"]] == [f"{base}/flaky.png"]


def test_item_id_with_a_slash_can_be_fetched(client, worker):
    xml = b"""<rss xmlns:g="http://base.google.com/ns/1.0"><channel>
<item><g:id>SKU/1</g:id><title>slashed</title></item></channel></rss>"""
    feed_id = client.post("/feeds", content=xml, headers=XML).json()["id"]
    wait_until_finished(client, feed_id)
    assert client.get(f"/feeds/{feed_id}/items").json() == ["SKU/1"]
    assert client.get(f"/feeds/{feed_id}/items/SKU%2F1").json()["title"] == "slashed"


def rss(*items: str) -> bytes:
    body = "".join(f"<item>{i}</item>" for i in items)
    return f'<rss xmlns:g="http://base.google.com/ns/1.0"><channel>{body}</channel></rss>'.encode()


def test_bad_items_are_skipped_and_the_rest_is_stored(client, worker):
    """Bad data is not a transient error: retrying the feed cannot fix it, and
    one bad item must not cost the customer the rest of the catalog."""
    xml = rss(
        "<g:id>ok-1</g:id><g:price>12.345 KWD</g:price><g:gtin>" + "9" * 70 + "</g:gtin>",
        f"<g:id>{'x' * 300}</g:id>",
        "<g:id>ok-2</g:id><g:price>99999999999999999999.99 USD</g:price>",
    )
    feed_id = client.post("/feeds", content=xml, headers=XML).json()["id"]
    feed = wait_until_finished(client, feed_id)
    assert feed["status"] == "done"
    assert feed["items"] == 2
    assert feed["error"] == "1 items skipped (id longer than 255 or invalid values)"
    ok1 = client.get(f"/feeds/{feed_id}/items/ok-1").json()
    assert (ok1["price"], ok1["currency"]) == ("12.345", "KWD")  # exact, no rounding to 2 places
    assert client.get(f"/feeds/{feed_id}/items/ok-2").json()["price"] == "99999999999999999999.99"


def test_a_value_the_database_rejects_skips_only_that_item(client, worker, monkeypatch):
    # Let a too-long ID through the worker's own check, so the database
    # rejects it (DataError) in the middle of a batch.
    monkeypatch.setattr(tasks, "MAX_ITEM_ID_LEN", 10_000)
    xml = rss("<g:id>a</g:id>", f"<g:id>{'x' * 300}</g:id>", "<g:id>b</g:id>", "<g:id>c</g:id>")
    feed_id = client.post("/feeds", content=xml, headers=XML).json()["id"]
    feed = wait_until_finished(client, feed_id)
    assert (feed["status"], feed["items"]) == ("done", 3)
    assert "1 items skipped" in feed["error"]
    assert client.get(f"/feeds/{feed_id}/items").json() == ["a", "b", "c"]


@pytest.mark.parametrize("body", [b"<rss><channel/></rss>", b"<html><body>hello</body></html>"])
def test_a_document_without_items_fails(client, worker, body):
    feed_id = client.post("/feeds", content=body, headers=XML).json()["id"]
    feed = wait_until_finished(client, feed_id)
    assert (feed["status"], feed["error"]) == ("failed", "no items with an id found")


def test_sweeper_recovers_a_feed_whose_messages_were_lost(client, worker, image_server):
    """E.g. a database outage outlasted the retries: the image message was
    dead-lettered and its row is still pending. Nothing else would move it."""
    from feedservice import sweeper

    feed_id = uuid.uuid4()
    url = f"{image_server.base_url}/ok.png"
    with session_scope() as s:
        s.add(Feed(id=feed_id, parsed_at=datetime.now(UTC)))
        s.flush()
        s.add(Image(id=image_id_for(feed_id, url), feed_id=feed_id, url=url))

    assert sweeper.sweep_once() == []  # fresh: still within the grace period
    assert sweeper.sweep_once(stale_after_s=0) == [feed_id]
    feed = wait_until_finished(client, feed_id)
    assert feed["status"] == "done"
    assert feed["images"] == {"pending": 0, "downloaded": 1, "failed": 0}
    assert sweeper.sweep_once(stale_after_s=0) == []  # finished feeds are left alone


def test_sweeper_leaves_a_feed_that_reports_progress_alone():
    from feedservice import sweeper

    feed_id = uuid.uuid4()
    with session_scope() as s:
        s.add(Feed(id=feed_id, updated_at=datetime(2020, 1, 1, tzinfo=UTC)))  # uploaded long ago
    tasks._heartbeat(feed_id)  # the worker has just stored a batch
    assert sweeper.sweep_once(stale_after_s=60) == []
