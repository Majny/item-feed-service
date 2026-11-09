"""Registering an uploaded feed: the rules behind POST /feeds, without HTTP.

The API stores the body as a file and calls `register`. This module decides
what a feed row looks like, when the processing message is published, and
what an Idempotency-Key replay returns. Its errors are plain exceptions; the
API maps them to status codes.
"""

import logging
import uuid

from sqlalchemy import delete, select, update
from sqlalchemy.exc import IntegrityError

from . import tasks
from .db import Feed, FeedStatus, session_scope

log = logging.getLogger(__name__)


class QueueUnavailable(Exception):
    """The processing message could not be published. Safe to retry."""


def register(feed_id: uuid.UUID, idempotency_key: str | None) -> uuid.UUID:
    """Store the feed row, then publish. Returns the ID the client should get:
    `feed_id`, or the ID of the feed a concurrent request with the same key
    created first."""
    # Row first, then the message: the worker must find the feed when the
    # message arrives.
    try:
        with session_scope() as s:
            s.add(Feed(id=feed_id, status=FeedStatus.PROCESSING, idempotency_key=idempotency_key))
    except IntegrityError:
        # Only the idempotency key is unique besides the random UUID: a
        # concurrent request with the same key inserted first.
        existing = replay(idempotency_key) if idempotency_key else None
        if existing is None:
            raise
        return existing
    _enqueue(feed_id, keep_on_failure=idempotency_key is not None)
    return feed_id


def replay(idempotency_key: str) -> uuid.UUID | None:
    """The feed already created for this key, published again if the first
    request's publish failed, crashed or is still in flight. None if the key
    is new."""
    with session_scope() as s:
        feed = s.scalar(select(Feed).where(Feed.idempotency_key == idempotency_key))
        if feed is None:
            return None
        feed_id, enqueued = feed.id, feed.enqueued
    if not enqueued:
        _enqueue(feed_id, keep_on_failure=True)
    return feed_id


def exists(feed_id: uuid.UUID) -> bool:
    with session_scope() as s:
        return s.get(Feed, feed_id) is not None


def _enqueue(feed_id: uuid.UUID, keep_on_failure: bool) -> None:
    try:
        tasks.process_feed.send(str(feed_id))
    except Exception as exc:
        log.exception("feed=%s: could not enqueue", feed_id)
        if not keep_on_failure:
            # Without a key the client can never come back to this ID: remove
            # the row. Should the message have reached RabbitMQ after all, the
            # worker skips it because the row is gone.
            with session_scope() as s:
                s.execute(delete(Feed).where(Feed.id == feed_id))
        # With a key the row and file stay: the retry publishes again.
        raise QueueUnavailable from exc
    if keep_on_failure:
        # Only a keyed replay reads the flag. Without a key, a DB error here
        # must not fail a request whose message is already queued.
        with session_scope() as s:
            s.execute(update(Feed).where(Feed.id == feed_id).values(enqueued=True))
