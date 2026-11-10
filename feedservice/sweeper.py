"""Recovers feeds that stopped making progress.

At-least-once delivery covers worker crashes, but not a message that never
existed or was given up on while the database was unreachable:
  * the API crashed between inserting the feed row and publishing it,
  * a database outage longer than the retry budget dead-lettered image
    messages whose rows are still "pending",
  * the feed task ran out of retries while publishing image messages.

All of them look the same in the database: a "processing" feed where nothing
has changed for a while. The fix is the same too: publish process_feed again.
It is idempotent: it parses the file only if that has not finished, then
re-publishes every pending image and runs the completion check.

Run it as a single loop (`python -m feedservice.sweeper`); several copies are
safe, as each stale feed is claimed by bumping its updated_at in one UPDATE.
"""

import logging
import time
import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy import exists, update

from . import tasks
from .config import settings
from .db import Feed, FeedStatus, Image, init_db, session_scope

log = logging.getLogger(__name__)


def sweep_once(stale_after_s: float | None = None) -> list[uuid.UUID]:
    """Re-publish feeds stuck in "processing"; returns their IDs."""
    stale_after = settings.sweep_stale_after_s if stale_after_s is None else stale_after_s
    cutoff = datetime.now(UTC) - timedelta(seconds=stale_after)
    with session_scope() as s:
        stale = s.scalars(
            update(Feed)
            .where(
                Feed.status == FeedStatus.PROCESSING,
                Feed.updated_at <= cutoff,
                # An image that finished recently keeps the feed alive.
                ~exists().where(Image.feed_id == Feed.id, Image.updated_at > cutoff),
            )
            .values(updated_at=datetime.now(UTC))  # claim: not picked again until stale again
            .returning(Feed.id)
        ).all()
    for feed_id in stale:
        try:
            tasks.process_feed.send(str(feed_id))
            log.warning("feed=%s: no progress for %.0f s, re-published", feed_id, stale_after)
        except Exception:
            # Claimed but not sent: it becomes stale again and is retried.
            log.exception("feed=%s: could not re-publish", feed_id)
    return list(stale)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    init_db()
    log.info(
        "sweeper: every %.0f s, feeds without progress for %.0f s",
        settings.sweep_interval_s,
        settings.sweep_stale_after_s,
    )
    while True:
        try:
            sweep_once()
        except Exception:  # database or broker down: try again next round
            log.exception("sweep failed")
        time.sleep(settings.sweep_interval_s)


if __name__ == "__main__":
    main()
