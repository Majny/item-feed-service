"""HTTP API (FastAPI). Interactive OpenAPI docs are served at /docs.

Conventions:
  * Errors always look like {"error": {"status": 404, "message": "..."}}.
  * List endpoints return a plain JSON array (as in the assignment) and are
    paginated with ?limit=&after= (keyset cursor). When there may be more
    results, a `Link: <...>; rel="next"` header points to the next page.
"""

import logging
import os
import tempfile
import uuid
from contextlib import asynccontextmanager
from decimal import Decimal
from pathlib import Path
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response, status
from fastapi.concurrency import run_in_threadpool
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse
from sqlalchemy import func, select
from sqlalchemy.orm import Session
from starlette.exceptions import HTTPException as StarletteHTTPException

from . import tasks
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
    session_scope,
)
from .images import image_path

log = logging.getLogger(__name__)
XML_CONTENT_TYPES = {"application/xml", "text/xml", "application/rss+xml"}
DEFAULT_PAGE, MAX_PAGE = 1000, 10_000


@asynccontextmanager
async def lifespan(_: FastAPI):
    settings.feeds_dir.mkdir(parents=True, exist_ok=True)
    settings.images_dir.mkdir(parents=True, exist_ok=True)
    init_db()
    yield


app = FastAPI(title="Item Feed service", version="0.1.0", lifespan=lifespan)


# ------------------------------------------------------------------ errors


def _error(code: int, message: object, headers: dict | None = None) -> JSONResponse:
    return JSONResponse({"error": {"status": code, "message": message}}, code, headers=headers)


@app.exception_handler(StarletteHTTPException)
async def http_error(_: Request, exc: StarletteHTTPException) -> JSONResponse:
    return _error(exc.status_code, exc.detail, getattr(exc, "headers", None))


@app.exception_handler(RequestValidationError)
async def validation_error(_: Request, exc: RequestValidationError) -> JSONResponse:
    details = [{"loc": e["loc"], "msg": e["msg"]} for e in exc.errors()]
    return _error(status.HTTP_422_UNPROCESSABLE_CONTENT, details)


# ----------------------------------------------------------------- helpers


def get_session():
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


@app.post(
    "/feeds",
    status_code=status.HTTP_202_ACCEPTED,
    responses={413: {}, 415: {}, 400: {}, 422: {}, 503: {}},
)
async def upload_feed(request: Request, response: Response) -> dict:
    """Upload a feed as the raw request body (Content-Type: application/xml).

    The file is stored and queued; processing happens in the background.
    Poll the URL from the Location header for the status.
    """
    content_type = request.headers.get("content-type", "").split(";")[0].strip().lower()
    if content_type not in XML_CONTENT_TYPES:
        raise HTTPException(
            status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            f"expected one of {sorted(XML_CONTENT_TYPES)}, got {content_type or 'nothing'}",
        )
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

    await run_in_threadpool(_register_feed, feed_id)
    response.headers["Location"] = f"/feeds/{feed_id}"
    return {"id": str(feed_id)}


def _register_feed(feed_id: uuid.UUID) -> None:
    # Row first, then the message: the worker must find the feed when the
    # message arrives. If publishing fails the client gets 503 and can retry.
    with session_scope() as s:
        s.add(Feed(id=feed_id, status=FeedStatus.PROCESSING))
    try:
        tasks.process_feed.send(str(feed_id))
    except Exception:
        log.exception("could not enqueue feed %s", feed_id)
        with session_scope() as s:
            feed = s.get(Feed, feed_id)
            feed.status, feed.error = FeedStatus.FAILED, "could not enqueue for processing"
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "queue unavailable, try again")


@app.get("/feeds/{feed_id}", responses={404: {}})
def get_feed(feed_id: uuid.UUID, session: SessionDep) -> dict:
    """Status is `processing`, `done` or `failed` (the last one is an extension
    of the assignment: the document could not be processed at all)."""
    feed = _get_feed(session, feed_id)
    items = session.scalar(select(func.count()).select_from(Item).where(Item.feed_id == feed_id))
    images = dict(
        session.execute(
            select(Image.status, func.count()).where(Image.feed_id == feed_id).group_by(Image.status)
        ).all()
    )
    return {
        "id": str(feed.id),
        "status": feed.status.value,
        "error": feed.error,
        "items": items,
        "images": {s.value: images.get(s, 0) for s in ImageStatus},
    }


# ------------------------------------------------------------------- items


@app.get("/feeds/{feed_id}/items", responses={404: {}})
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


@app.get("/feeds/{feed_id}/items/{item_id}", responses={404: {}})
def get_item(feed_id: uuid.UUID, item_id: str, session: SessionDep) -> dict:
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

    return {
        "id": item.item_id,
        "title": item.title,
        "description": item.description,
        "link": item.link,
        "price": money(item.price),
        "sale_price": money(item.sale_price),
        "currency": item.currency,
        "condition": item.condition,
        "availability": item.availability,
        "brand": item.brand,
        "gtin": item.gtin,
        "item_group_id": item.item_group_id,
        "images": [{"id": str(i.id), "url": i.url, "status": i.status.value} for i in images],
    }


# ------------------------------------------------------------------ images


@app.get("/feeds/{feed_id}/images", responses={404: {}})
def list_images(
    feed_id: uuid.UUID,
    request: Request,
    response: Response,
    session: SessionDep,
    limit: Limit = DEFAULT_PAGE,
    after: Annotated[uuid.UUID | None, Query(description="Cursor: last image ID of the previous page")] = None,
) -> list[str]:
    """IDs of images that were downloaded and can be fetched. Failed ones are
    listed per item in GET /feeds/{id}/items/{item_id}."""
    _get_feed(session, feed_id)
    query = select(Image.id).where(Image.feed_id == feed_id, Image.status == ImageStatus.DOWNLOADED)
    if after is not None:
        query = query.where(Image.id > after)
    ids = [str(i) for i in session.scalars(query.order_by(Image.id).limit(limit + 1))]
    return _paginate(request, response, ids, limit)


@app.get(
    "/feeds/{feed_id}/images/{image_id}",
    response_class=FileResponse,
    responses={200: {"content": {"image/*": {}}}, 304: {}, 404: {}},
)
def get_image(feed_id: uuid.UUID, image_id: uuid.UUID, request: Request, session: SessionDep):
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


@app.get("/health", include_in_schema=False)
def health() -> dict:
    return {"status": "ok"}
