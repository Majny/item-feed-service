"""Background tasks (Dramatiq actors) and the broker setup.

Reliability model:
  * RabbitMQ queues are durable and messages persistent; Dramatiq acks a
    message only after the actor finished, so a worker killed mid-task gets
    the message redelivered.
  * Failures are retried with exponential backoff and jitter (Dramatiq
    Retries). After the last attempt the outcome is written to the DB and the
    message is dead-lettered (<queue>.XQ in RabbitMQ).
  * Errors are split into retryable (timeouts, connection errors, 5xx, 429)
    and permanent (other 4xx, not an image, too large). Permanent ones are
    recorded right away instead of being retried.
  * Every task is idempotent, so a redelivery or retry never duplicates data:
    items are upserted by (feed_id, item_id), image IDs are deterministic
    (uuid5 of feed ID + URL) and image files are written atomically. A
    redelivered task whose work is already done still re-runs the (idempotent)
    feed completion check, in case the first delivery died right before it.
  * Dramatiq's TimeLimitExceeded is a BaseException, so it is caught
    explicitly: a task killed by its time limit on the last attempt still
    records the failure instead of leaving its row "pending" forever.
  * One broken image never fails the feed: it is marked "failed" on the image
    row and the feed still finishes.
  * Feed status is derived from DB state (all items stored and no image still
    pending), never from message counting, so duplicate or reordered messages
    cannot corrupt it.
  * Two queues, two worker deployments: "feeds" (parse + upsert) and "images"
    (one message per image). Slow image hosts never block parsing.
"""

import logging
import uuid
from datetime import UTC, datetime
from typing import Any

import dramatiq
from dramatiq.brokers.rabbitmq import RabbitmqBroker
from dramatiq.brokers.stub import StubBroker
from dramatiq.middleware import CurrentMessage, TimeLimitExceeded
from sqlalchemy import delete, exists, func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import DataError

from .config import settings
from .db import Feed, FeedStatus, Image, ImageStatus, Item, ItemImage, image_id_for, session_scope
from .images import PermanentDownloadError, TransientDownloadError, download_image, image_path
from .parser import FeedParseError, ParsedItem, iter_items

log = logging.getLogger(__name__)

if settings.broker_url == "stub":
    broker: dramatiq.Broker = StubBroker()
    broker.emit_after("process_boot")
else:
    # confirm_delivery: the API only answers 202 once RabbitMQ has the message.
    broker = RabbitmqBroker(url=settings.broker_url, confirm_delivery=True)
broker.add_middleware(CurrentMessage())
dramatiq.set_broker(broker)

RETRY_OPTIONS: dict[str, Any] = dict(
    max_retries=settings.max_retries,
    min_backoff=settings.min_backoff_ms,
    max_backoff=settings.max_backoff_ms,
)
BATCH_SIZE = 500
MAX_ITEM_ID_LEN = 255  # items.item_id column
# Below RabbitMQ's default consumer_timeout (30 min): past that RabbitMQ would
# redeliver the unacked message while the first worker is still parsing. The
# feed-worker prefetches only one message per thread for the same reason.
FEED_TIME_LIMIT_MS = 25 * 60 * 1000
# Above settings.image_deadline_s + one read timeout, so a slow download ends
# as a normal TransientDownloadError; the time limit is only a backstop.
IMAGE_TIME_LIMIT_MS = 2 * 60 * 1000


def _is_last_attempt() -> bool:
    message = CurrentMessage.get_current_message()
    return message is not None and message.options.get("retries", 0) >= settings.max_retries


# --------------------------------------------------------------------------- feed


@dramatiq.actor(queue_name="feeds", time_limit=FEED_TIME_LIMIT_MS, **RETRY_OPTIONS)
def process_feed(feed_id: str) -> None:
    fid = uuid.UUID(feed_id)
    with session_scope() as s:
        feed = s.get(Feed, fid)
        if feed is None or feed.status != FeedStatus.PROCESSING:
            log.info("feed=%s: already finished, skipping duplicate delivery", fid)
            return
        already_parsed = feed.parsed_at is not None

    if not already_parsed:
        try:
            _store_items(fid)
        except (Exception, TimeLimitExceeded) as exc:
            if _is_last_attempt():
                log.exception("feed=%s: giving up after retries", fid)
                # The details (driver errors, SQL) stay in the logs; the
                # client gets a stable message it can quote to support.
                _fail_feed(fid, f"internal error while processing, retried and gave up (feed={fid})")
            else:
                log.warning("feed=%s: processing failed, will retry: %r", fid, exc)
            # Dramatiq retries with backoff; after the last attempt the message
            # goes to the dead-letter queue (feeds.XQ) for inspection.
            raise

    # Enqueue (again) every image that is not finished yet. Duplicate messages
    # are harmless because fetch_image skips images that are not pending.
    with session_scope() as s:
        pending = s.scalars(select(Image.id).where(Image.feed_id == fid, Image.status == ImageStatus.PENDING)).all()
    for image_id in pending:
        fetch_image.send(str(image_id))
    _finish_feed_if_complete(fid)


def _store_items(fid: uuid.UUID) -> None:
    path = settings.feeds_dir / f"{fid}.xml"
    parse_error: str | None = None
    count = skipped = 0
    batch: list[ParsedItem] = []

    def flush() -> None:
        nonlocal count, skipped, batch
        rejected = _upsert_batch(fid, batch)
        count += len(batch) - rejected
        skipped += rejected
        batch = []
        _heartbeat(fid)

    _heartbeat(fid)  # parsing started: the sweeper must not pick this feed

    with path.open("rb") as fh:
        try:
            for item in iter_items(fh):
                if len(item.item_id) > MAX_ITEM_ID_LEN:
                    log.warning("feed=%s: skipping item with a %d-char id", fid, len(item.item_id))
                    skipped += 1
                    continue
                batch.append(item)
                if len(batch) >= BATCH_SIZE:
                    flush()
        except FeedParseError as exc:
            # Malformed XML is not something a retry can fix. Items complete
            # before the error are kept; the error is recorded on the feed.
            parse_error = str(exc)
    if batch:
        flush()
    if parse_error:
        log.warning("feed=%s: parse error after %d items: %s", fid, count, parse_error)

    problems = []
    if parse_error:
        problems.append(f"invalid XML after {count} items: {parse_error}")
    if skipped:
        problems.append(f"{skipped} items skipped (id longer than {MAX_ITEM_ID_LEN} or invalid values)")
    if count == 0 and not parse_error and not skipped:
        problems.append("no items with an id found")
    values: dict[str, Any] = {"parsed_at": datetime.now(UTC), "error": "; ".join(problems) or None}
    if count == 0:
        # Nothing usable: an empty or wrong document must not look like a
        # successfully imported (empty) catalog.
        values["status"] = FeedStatus.FAILED
    with session_scope() as s:
        # Only while processing: a concurrent delivery may have finished or
        # failed the feed already, and its outcome must not be overwritten.
        s.execute(update(Feed).where(Feed.id == fid, Feed.status == FeedStatus.PROCESSING).values(**values))
    log.info("feed=%s: stored %d items, skipped %d", fid, count, skipped)


def _upsert_batch(fid: uuid.UUID, batch: list[ParsedItem]) -> int:
    """Store a batch; returns how many items were rejected by the database.

    A value the database refuses (DataError) is a property of the data, so
    retrying cannot fix it. Instead of failing the whole feed, the batch is
    split until the offending items are isolated and skipped.
    """
    try:
        _write_batch(fid, batch)
        return 0
    except DataError as exc:
        if len(batch) == 1:
            log.warning("feed=%s: skipping item %r: %s", fid, batch[0].item_id, exc.orig)
            return 1
        mid = len(batch) // 2
        return _upsert_batch(fid, batch[:mid]) + _upsert_batch(fid, batch[mid:])


def _write_batch(fid: uuid.UUID, batch: list[ParsedItem]) -> None:
    # If an ID appears twice in a feed, the later definition wins (also across
    # batches: the item row is overwritten and its old image links deleted).
    by_id = {item.item_id: item for item in batch}
    item_rows, image_rows, link_rows = [], {}, []
    for it in by_id.values():
        item_rows.append(
            dict(
                feed_id=fid,
                item_id=it.item_id,
                title=it.title,
                description=it.description,
                link=it.link,
                price=it.price,
                sale_price=it.sale_price,
                currency=it.currency,
                condition=it.condition,
                availability=it.availability,
                brand=it.brand,
                gtin=it.gtin,
                item_group_id=it.item_group_id,
            )
        )
        for position, url in enumerate(it.image_urls):
            image_id = image_id_for(fid, url)
            image_rows[image_id] = dict(id=image_id, feed_id=fid, url=url, status=ImageStatus.PENDING)
            link_rows.append(dict(feed_id=fid, item_id=it.item_id, position=position, image_id=image_id))

    with session_scope() as s:
        stmt = insert(Item).values(item_rows)
        s.execute(
            stmt.on_conflict_do_update(
                index_elements=[Item.feed_id, Item.item_id],
                set_={c: stmt.excluded[c] for c in item_rows[0] if c not in ("feed_id", "item_id")},
            )
        )
        s.execute(delete(ItemImage).where(ItemImage.feed_id == fid, ItemImage.item_id.in_(list(by_id))))
        if image_rows:
            s.execute(insert(Image).values(list(image_rows.values())).on_conflict_do_nothing())
            stmt = insert(ItemImage).values(link_rows)
            s.execute(
                stmt.on_conflict_do_update(
                    index_elements=[ItemImage.feed_id, ItemImage.item_id, ItemImage.position],
                    set_={"image_id": stmt.excluded.image_id},
                )
            )


def _heartbeat(fid: uuid.UUID) -> None:
    """Record progress (feeds.updated_at): the sweeper only re-publishes feeds
    without progress, so one that is being parsed is never picked."""
    with session_scope() as s:
        active = (Feed.id == fid, Feed.status == FeedStatus.PROCESSING)
        s.execute(update(Feed).where(*active).values(updated_at=func.now()))


def _fail_feed(fid: uuid.UUID, error: str) -> None:
    with session_scope() as s:
        # Only a processing feed: a concurrent delivery may have finished it.
        s.execute(
            update(Feed)
            .where(Feed.id == fid, Feed.status == FeedStatus.PROCESSING)
            .values(status=FeedStatus.FAILED, error=error)
        )


def _finish_feed_if_complete(fid: uuid.UUID) -> None:
    # Runs in its own transaction *after* the image status was committed, so
    # whichever worker finishes the last image sees all others as finished.
    with session_scope() as s:
        finished = s.execute(
            update(Feed)
            .where(
                Feed.id == fid,
                Feed.status == FeedStatus.PROCESSING,
                Feed.parsed_at.is_not(None),
                ~exists().where(Image.feed_id == fid, Image.status == ImageStatus.PENDING),
            )
            .values(status=FeedStatus.DONE)
            .returning(Feed.id)
        ).first()
    if finished:
        log.info("feed=%s: done", fid)


# -------------------------------------------------------------------------- image


@dramatiq.actor(queue_name="images", time_limit=IMAGE_TIME_LIMIT_MS, **RETRY_OPTIONS)
def fetch_image(image_id: str) -> None:
    iid = uuid.UUID(image_id)
    with session_scope() as s:
        image = s.get(Image, iid)
        if image is None:
            return
        url, fid, current = image.url, image.feed_id, image.status

    if current != ImageStatus.PENDING:
        # Duplicate or redelivered message. The work is done, but an earlier
        # delivery may have died between committing the image status and the
        # completion check, so run the (idempotent) check again.
        _finish_feed_if_complete(fid)
        return

    try:
        result = download_image(url, image_path(iid))
    except PermanentDownloadError as exc:
        log.info("feed=%s image=%s: failed permanently (%s): %s", fid, iid, exc, url)
        _set_image(iid, ImageStatus.FAILED, error=str(exc))
    except (Exception, TimeLimitExceeded) as exc:  # TransientDownloadError, time limit, bugs
        if not _is_last_attempt():
            if not isinstance(exc, (TransientDownloadError, TimeLimitExceeded)):
                raise  # unexpected (disk full, a bug): retried, but logged with a traceback
            log.info("feed=%s image=%s: will retry (%r): %s", fid, iid, exc, url)
            # dramatiq.Retry is the expected-retry path: same backoff and
            # retry counting as any exception, but not logged as an error.
            # delay=None means the normal exponential backoff.
            delay = None
            retry_after = getattr(exc, "retry_after_s", None)
            if retry_after is not None:
                # Respect the server's Retry-After, within our backoff bounds.
                delay = min(max(int(retry_after * 1000), settings.min_backoff_ms), settings.max_backoff_ms)
            raise dramatiq.Retry(str(exc), delay=delay) from exc
        # Record the failure so the feed can finish, then re-raise so the
        # message ends up in the dead-letter queue (images.XQ).
        log.warning("feed=%s image=%s: giving up after retries (%r): %s", fid, iid, exc, url)
        _set_image(iid, ImageStatus.FAILED, error=f"gave up after retries: {_public_reason(exc)}")
        _finish_feed_if_complete(fid)
        raise
    else:
        _set_image(
            iid,
            ImageStatus.DOWNLOADED,
            content_type=result.content_type,
            size_bytes=result.size_bytes,
            error=None,
        )
    _finish_feed_if_complete(fid)


def _public_reason(exc: BaseException) -> str:
    """What the client sees in images.error: our own download errors are
    meant for it ("HTTP 503", "timeout"), anything else stays in the logs."""
    if isinstance(exc, TransientDownloadError):
        return str(exc)
    if isinstance(exc, TimeLimitExceeded):
        return "download took too long"
    return "internal error"


def _set_image(iid: uuid.UUID, status: ImageStatus, **values: object) -> None:
    # Only a pending image changes: if two deliveries of one message race, the
    # first result wins and a later one cannot overwrite it.
    with session_scope() as s:
        s.execute(
            update(Image).where(Image.id == iid, Image.status == ImageStatus.PENDING).values(status=status, **values)
        )
