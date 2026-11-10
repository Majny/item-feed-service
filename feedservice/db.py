"""Database schema and session handling (SQLAlchemy 2.0, PostgreSQL)."""

import enum
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    DateTime,
    Enum,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    create_engine,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker

from .config import settings


class FeedStatus(enum.StrEnum):
    PROCESSING = "processing"
    DONE = "done"
    FAILED = "failed"


class ImageStatus(enum.StrEnum):
    PENDING = "pending"
    DOWNLOADED = "downloaded"
    FAILED = "failed"


class Base(DeclarativeBase):
    pass


def _enum(cls: type[enum.Enum]) -> Enum:
    return Enum(cls, values_callable=lambda e: [m.value for m in e], native_enum=False)


class Feed(Base):
    __tablename__ = "feeds"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    status: Mapped[FeedStatus] = mapped_column(_enum(FeedStatus), default=FeedStatus.PROCESSING)
    # Set once all items are stored; the feed is "done" when this is set and no
    # image is pending any more.
    parsed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error: Mapped[str | None] = mapped_column(Text)
    # Client-supplied Idempotency-Key of the upload: a retried POST with the
    # same key returns this feed instead of creating a second one.
    idempotency_key: Mapped[str | None] = mapped_column(String(255), unique=True)
    # The process_feed message was confirmed by the broker. A keyed retry of
    # an upload whose publish failed or is still in flight publishes again
    # (duplicate messages are harmless, the worker is idempotent). Tracked
    # only for keyed uploads, the only ones that can be replayed.
    enqueued: Mapped[bool] = mapped_column(default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class Item(Base):
    __tablename__ = "items"

    feed_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("feeds.id", ondelete="CASCADE"), primary_key=True)
    # The customer's own ID (<g:id>), unique only within a feed. Longer IDs
    # are skipped by the worker (MAX_ITEM_ID_LEN).
    item_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    title: Mapped[str | None] = mapped_column(Text)
    description: Mapped[str | None] = mapped_column(Text)
    link: Mapped[str | None] = mapped_column(Text)
    # Unscaled numeric: stores the amount exactly as written (3-decimal
    # currencies, no overflow at some arbitrary precision). Free-text fields
    # are Text: customer data has no reliable length limits, and one long
    # value must not make a whole batch fail.
    price: Mapped[Decimal | None] = mapped_column(Numeric)
    sale_price: Mapped[Decimal | None] = mapped_column(Numeric)
    currency: Mapped[str | None] = mapped_column(String(3))  # the parser only accepts 3 letters
    condition: Mapped[str | None] = mapped_column(Text)
    availability: Mapped[str | None] = mapped_column(Text)
    brand: Mapped[str | None] = mapped_column(Text)
    gtin: Mapped[str | None] = mapped_column(Text)
    item_group_id: Mapped[str | None] = mapped_column(Text)


class Image(Base):
    __tablename__ = "images"
    # Every hot query filters by feed and status: the completion check after
    # each image, status counts, the list of downloaded images, re-publishing.
    __table_args__ = (Index("ix_images_feed_status", "feed_id", "status"),)

    # Deterministic: uuid5(feed_id, url). A retried task computes the same ID,
    # so it cannot create a duplicate, and the same URL used by several items
    # of one feed is downloaded only once.
    id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    feed_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("feeds.id", ondelete="CASCADE"))
    url: Mapped[str] = mapped_column(Text)
    status: Mapped[ImageStatus] = mapped_column(_enum(ImageStatus), default=ImageStatus.PENDING)
    content_type: Mapped[str | None] = mapped_column(Text)
    size_bytes: Mapped[int | None] = mapped_column(Integer)
    error: Mapped[str | None] = mapped_column(Text)
    # Lets the sweeper tell a feed whose images are still being worked on
    # from one whose image messages were lost.
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class ItemImage(Base):
    """Which images belong to which item, in feed order (0 = main image)."""

    __tablename__ = "item_images"
    __table_args__ = (
        ForeignKeyConstraint(["feed_id", "item_id"], ["items.feed_id", "items.item_id"], ondelete="CASCADE"),
    )

    feed_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    item_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    position: Mapped[int] = mapped_column(primary_key=True)
    image_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("images.id", ondelete="CASCADE"))


engine = create_engine(settings.database_url, pool_pre_ping=True)
SessionLocal = sessionmaker(engine, expire_on_commit=False)


def init_db() -> None:
    # Good enough for an assignment; a real service would use Alembic migrations.
    Base.metadata.create_all(engine)


@contextmanager
def session_scope() -> Iterator[Session]:
    with SessionLocal() as session, session.begin():
        yield session


def image_id_for(feed_id: uuid.UUID, url: str) -> uuid.UUID:
    return uuid.uuid5(feed_id, url)
