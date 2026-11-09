"""HTTP API (FastAPI). Interactive OpenAPI docs are served at /docs.

Conventions:
  * Errors always look like {"error": {"status": 404, "message": "..."}},
    validation errors add a "details" list. 503 means "safe to retry" and
    carries Retry-After.
  * List endpoints return a plain JSON array (as in the assignment) and are
    paginated with ?limit=&after= (keyset cursor). When there may be more
    results, a `Link: <...>; rel="next"` header points to the next page.
  * POST /feeds accepts an optional Idempotency-Key header, so a client that
    lost the response can retry without creating a second feed.
"""

import logging
import os
import tempfile
import uuid
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from decimal import Decimal
from pathlib import Path
from typing import Annotated

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Response, status
from fastapi.concurrency import run_in_threadpool
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy import func, select, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session
from starlette.exceptions import HTTPException as StarletteHTTPException

from . import feeds
from .config import settings
from .db import (
    Feed,
    FeedStatus,
    Image,
    ImageStatus,
    Item,
    ItemImage,
    SessionLocal,
    init_db,
)
from .images import image_path

log = logging.getLogger(__name__)
XML_CONTENT_TYPES = {"application/xml", "text/xml", "application/rss+xml"}
DEFAULT_PAGE, MAX_PAGE = 1000, 10_000
RETRY_AFTER_S = "5"


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    settings.feeds_dir.mkdir(parents=True, exist_ok=True)
    settings.images_dir.mkdir(parents=True, exist_ok=True)
    init_db()
    yield


app = FastAPI(title="Item Feed service", version="0.1.0", lifespan=lifespan)


# ------------------------------------------------------------------ models


class FeedCreated(BaseModel):
    id: uuid.UUID


class ImageCounts(BaseModel):
    pending: int
    downloaded: int
    failed: int


class FeedOut(BaseModel):
    id: uuid.UUID
    status: FeedStatus = Field(description="`failed` = the document could not be processed at all")
    error: str | None = Field(
        description="Set when something went wrong; on a `done` feed it means the XML was "
        "broken after some items, so the catalog may be incomplete"
    )
    items: int
    images: ImageCounts


class ImageRef(BaseModel):
    id: uuid.UUID
    url: str
    status: ImageStatus


class ItemOut(BaseModel):
    id: str
    title: str | None
    description: str | None
    link: str | None
    price: str | None = Field(description="Decimal as a string (no float rounding), e.g. '299.00'")
    sale_price: str | None
    currency: str | None = Field(description="ISO 4217 code taken from the price, e.g. 'NOK'")
    condition: str | None
    availability: str | None
    brand: str | None
    gtin: str | None
    item_group_id: str | None
    images: list[ImageRef] = Field(description="In feed order; position 0 is the main image")


class ErrorDetail(BaseModel):
    status: int
    message: str
    details: list[dict] | None = None


class ErrorBody(BaseModel):
    error: ErrorDetail


def _errors(*codes: int) -> dict:
    return {code: {"model": ErrorBody} for code in codes}


# ------------------------------------------------------------------ errors


def _error(code: int, message: str, headers: dict | None = None, details: list | None = None) -> JSONResponse:
    body: dict = {"status": code, "message": message}
    if details is not None:
        body["details"] = details
    return JSONResponse({"error": body}, code, headers=headers)


@app.exception_handler(StarletteHTTPException)
async def http_error(_: Request, exc: StarletteHTTPException) -> JSONResponse:
    return _error(exc.status_code, str(exc.detail), getattr(exc, "headers", None))


@app.exception_handler(RequestValidationError)
async def validation_error(_: Request, exc: RequestValidationError) -> JSONResponse:
    details = [{"loc": list(e["loc"]), "msg": e["msg"]} for e in exc.errors()]
    return _error(status.HTTP_422_UNPROCESSABLE_CONTENT, "invalid request", details=details)


@app.exception_handler(OperationalError)
async def db_unavailable(_: Request, exc: OperationalError) -> JSONResponse:
    # Connection refused, failover, ... : transient, so tell the client to retry.
    log.error("database error: %s", exc)
    return _error(
        status.HTTP_503_SERVICE_UNAVAILABLE, "database unavailable, try again", {"Retry-After": RETRY_AFTER_S}
    )


@app.exception_handler(feeds.QueueUnavailable)
async def queue_unavailable(_: Request, exc: feeds.QueueUnavailable) -> JSONResponse:
    return _error(status.HTTP_503_SERVICE_UNAVAILABLE, "queue unavailable, try again", {"Retry-After": RETRY_AFTER_S})


@app.exception_handler(Exception)
async def unexpected_error(_: Request, exc: Exception) -> JSONResponse:
    # The server still logs the traceback (Starlette re-raises after this).
    return _error(status.HTTP_500_INTERNAL_SERVER_ERROR, "internal error")


# ----------------------------------------------------------------- helpers


def get_session() -> Iterator[Session]:
    with SessionLocal() as session:
        yield session


SessionDep = Annotated[Session, Depends(get_session)]
Limit = Annotated[int, Query(ge=1, le=MAX_PAGE, description="Page size")]


def _get_feed(session: Session, feed_id: uuid.UUID) -> Feed:
    feed = session.get(Feed, feed_id)
    if feed is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "feed not found")
    return feed


def _paginate(request: Request, response: Response, ids: list[str], limit: int) -> list[str]:
    """Fetch limit+1 rows; if the extra one exists, there is a next page."""
    page = ids[:limit]
    if len(ids) > limit:
        next_url = request.url.include_query_params(limit=limit, after=page[-1])
        response.headers["Link"] = f'<{next_url}>; rel="next"'
    return page


# ------------------------------------------------------------------- feeds


def _accepted(response: Response, feed_id: uuid.UUID) -> FeedCreated:
    response.headers["Location"] = f"/feeds/{feed_id}"
    return FeedCreated(id=feed_id)


@app.post(
    "/feeds",
    status_code=status.HTTP_202_ACCEPTED,
    responses=_errors(400, 413, 415, 422, 503),
    openapi_extra={
        "requestBody": {
            "required": True,
            "description": "The feed XML as the raw request body (not multipart).",
            "content": {t: {"schema": {"type": "string", "format": "binary"}} for t in sorted(XML_CONTENT_TYPES)},
        }
    },
)
async def upload_feed(
    request: Request,
    response: Response,
    idempotency_key: Annotated[
        str | None,
        Header(
            min_length=1,
            max_length=255,
            description="Optional. Retrying with the same key returns the feed created by the "
            "first request instead of creating a new one.",
        ),
    ] = None,
) -> FeedCreated:
    """Upload a feed as the raw request body (`Content-Type: application/xml`).

    The file is stored and queued; processing happens in the background.
    Poll the URL from the Location header for the status.
    """
    content_type = request.headers.get("content-type", "").split(";")[0].strip().lower()
    if content_type not in XML_CONTENT_TYPES:
        hint = (
            " (send the XML as the raw body, e.g. curl --data-binary @feed.xml)" if "multipart" in content_type else ""
        )
        raise HTTPException(
            status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            f"expected one of {sorted(XML_CONTENT_TYPES)}, got {content_type or 'nothing'}{hint}",
        )
    if idempotency_key is not None:
        # A retry of a request that already got this far: answer like the
        # first time, without reading the body again.
        existing = await run_in_threadpool(feeds.replay, idempotency_key)
        if existing is not None:
            response.headers["Idempotent-Replayed"] = "true"
            return _accepted(response, existing)

    declared = request.headers.get("content-length")
    if declared and declared.isdigit() and int(declared) > settings.max_feed_bytes:
        raise HTTPException(status.HTTP_413_CONTENT_TOO_LARGE, "feed too large")

    feed_id = uuid.uuid4()
    dest = settings.feeds_dir / f"{feed_id}.xml"
    fd, tmp_name = tempfile.mkstemp(dir=settings.feeds_dir, prefix=".upload-")
    size, head = 0, b""
    try:
        # Stream to disk instead of holding the whole body in memory.
        with os.fdopen(fd, "wb") as fh:
            async for chunk in request.stream():
                size += len(chunk)
                if size > settings.max_feed_bytes:
                    raise HTTPException(status.HTTP_413_CONTENT_TOO_LARGE, "feed too large")
                if len(head) < 64:
                    head += chunk[:64]
                fh.write(chunk)
        if size == 0:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "empty body")
        # Cheap sanity check only. Full XML validation happens in the worker
        # (so a big file is not parsed twice); its result shows in the status.
        if not head.lstrip(b"\xef\xbb\xbf \t\r\n").startswith(b"<"):
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "body is not XML")
        os.replace(tmp_name, dest)
    except BaseException:
        Path(tmp_name).unlink(missing_ok=True)
        raise

    # Not BaseException: a cancelled request whose registration finished in
    # the thread must not lose its file.
    try:
        accepted_id = await run_in_threadpool(feeds.register, feed_id, idempotency_key)
    except Exception:
        if idempotency_key is None or not await run_in_threadpool(feeds.exists, feed_id):
            dest.unlink(missing_ok=True)
        raise
    if accepted_id != feed_id:  # lost a race against a concurrent request with the same key
        dest.unlink(missing_ok=True)
        response.headers["Idempotent-Replayed"] = "true"
    return _accepted(response, accepted_id)


@app.get("/feeds/{feed_id}", responses=_errors(404, 422))
def get_feed(feed_id: uuid.UUID, session: SessionDep) -> FeedOut:
    """Status is `processing`, `done` or `failed` (the last one is an extension
    of the assignment: the document could not be processed at all)."""
    feed = _get_feed(session, feed_id)
    items = session.scalar(select(func.count()).select_from(Item).where(Item.feed_id == feed_id))
    images = dict(
        session.execute(select(Image.status, func.count()).where(Image.feed_id == feed_id).group_by(Image.status)).all()
    )
    return FeedOut(
        id=feed.id,
        status=feed.status,
        error=feed.error,
        items=items or 0,
        images=ImageCounts(**{s.value: images.get(s, 0) for s in ImageStatus}),
    )


# ------------------------------------------------------------------- items


@app.get("/feeds/{feed_id}/items", responses=_errors(404, 422))
def list_items(
    feed_id: uuid.UUID,
    request: Request,
    response: Response,
    session: SessionDep,
    limit: Limit = DEFAULT_PAGE,
    after: Annotated[str | None, Query(description="Cursor: last item ID of the previous page")] = None,
) -> list[str]:
    """Item IDs stored so far, sorted. While the feed is still processing this
    can be a partial list (check the feed status)."""
    _get_feed(session, feed_id)
    query = select(Item.item_id).where(Item.feed_id == feed_id)
    if after is not None:
        query = query.where(Item.item_id > after)
    ids = list(session.scalars(query.order_by(Item.item_id).limit(limit + 1)))
    return _paginate(request, response, ids, limit)


@app.get("/feeds/{feed_id}/items/{item_id:path}", responses=_errors(404, 422))
def get_item(feed_id: uuid.UUID, item_id: str, session: SessionDep) -> ItemOut:
    _get_feed(session, feed_id)
    item = session.get(Item, (feed_id, item_id))
    if item is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "item not found")
    images = session.execute(
        select(Image.id, Image.url, Image.status)
        .join(ItemImage, ItemImage.image_id == Image.id)
        .where(ItemImage.feed_id == feed_id, ItemImage.item_id == item_id)
        .order_by(ItemImage.position)
    ).all()

    def money(v: Decimal | None) -> str | None:
        return None if v is None else str(v)

    return ItemOut(
        id=item.item_id,
        title=item.title,
        description=item.description,
        link=item.link,
        price=money(item.price),
        sale_price=money(item.sale_price),
        currency=item.currency,
        condition=item.condition,
        availability=item.availability,
        brand=item.brand,
        gtin=item.gtin,
        item_group_id=item.item_group_id,
        images=[ImageRef(id=i.id, url=i.url, status=i.status) for i in images],
    )


# ------------------------------------------------------------------ images


@app.get("/feeds/{feed_id}/images", responses=_errors(404, 422))
def list_images(
    feed_id: uuid.UUID,
    request: Request,
    response: Response,
    session: SessionDep,
    limit: Limit = DEFAULT_PAGE,
    after: Annotated[uuid.UUID | None, Query(description="Cursor: last image ID of the previous page")] = None,
) -> list[str]:
    """IDs of images that were downloaded and can be fetched, ordered by ID.
    Failed ones (and per-item order) are in GET /feeds/{id}/items/{item_id}."""
    _get_feed(session, feed_id)
    query = select(Image.id).where(Image.feed_id == feed_id, Image.status == ImageStatus.DOWNLOADED)
    if after is not None:
        query = query.where(Image.id > after)
    ids = [str(i) for i in session.scalars(query.order_by(Image.id).limit(limit + 1))]
    return _paginate(request, response, ids, limit)


@app.get(
    "/feeds/{feed_id}/images/{image_id}",
    response_class=FileResponse,
    responses={200: {"content": {"image/*": {}}}, 304: {"description": "Not modified"}, **_errors(404, 422)},
)
def get_image(feed_id: uuid.UUID, image_id: uuid.UUID, request: Request, session: SessionDep) -> Response:
    """The image bytes with the Content-Type the origin server sent.

    Content behind an image ID never changes, so it is cacheable forever and
    the ID itself works as the ETag.
    """
    _get_feed(session, feed_id)
    image = session.get(Image, image_id)
    if image is None or image.feed_id != feed_id or image.status != ImageStatus.DOWNLOADED:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "image not found")
    headers = {"ETag": f'"{image.id}"', "Cache-Control": "public, max-age=31536000, immutable"}
    if request.headers.get("if-none-match") == headers["ETag"]:
        return Response(status_code=status.HTTP_304_NOT_MODIFIED, headers=headers)
    return FileResponse(image_path(image.id), media_type=image.content_type, headers=headers)


# ------------------------------------------------------------------ health


@app.get("/health", include_in_schema=False)
def health() -> dict:
    """Liveness: the process is up and serving requests."""
    return {"status": "ok"}


@app.get("/ready", include_in_schema=False)
def ready(session: SessionDep) -> dict:
    """Readiness: the database answers (an OperationalError becomes 503)."""
    session.execute(text("SELECT 1"))
    return {"status": "ok"}
