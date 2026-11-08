"""Image download: the one place where we consume someone else's HTTP API.

The remote host is treated as unreliable: hard timeouts, a size cap checked
while streaming, a content-type check, limited redirects, and a clear split
between retryable and permanent errors (with Retry-After passed along).
"""

import os
import tempfile
import threading
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path

import httpx

from .config import settings


class PermanentDownloadError(Exception):
    """Retrying will not help (404, not an image, too large, ...)."""


class TransientDownloadError(Exception):
    """Worth retrying later (timeout, connection error, 5xx, 429)."""

    def __init__(self, message: str, retry_after_s: float | None = None):
        super().__init__(message)
        self.retry_after_s = retry_after_s


@dataclass
class DownloadedImage:
    content_type: str
    size_bytes: int


def image_path(image_id: object) -> Path:
    return settings.images_dir / str(image_id)


_client: httpx.Client | None = None
_client_lock = threading.Lock()


def http_client() -> httpx.Client:
    """One pooled client per worker process (httpx.Client is thread-safe)."""
    global _client
    with _client_lock:
        if _client is None:
            _client = httpx.Client(
                timeout=httpx.Timeout(settings.image_timeout_s, connect=5.0),
                limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
                follow_redirects=True,
                max_redirects=5,
                headers={"User-Agent": "feedservice/0.1 (+image fetcher)"},
            )
        return _client


def _retry_after(value: str | None) -> float | None:
    """Retry-After is either seconds or an HTTP date."""
    if not value:
        return None
    if value.strip().isdigit():
        return float(value)
    try:
        return max(0.0, (parsedate_to_datetime(value) - datetime.now(UTC)).total_seconds())
    except (TypeError, ValueError):
        return None


def download_image(url: str, dest: Path) -> DownloadedImage:
    """Stream `url` into `dest`. The file appears atomically or not at all."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        with http_client().stream("GET", url) as resp:
            if resp.status_code == 429 or resp.status_code >= 500:
                raise TransientDownloadError(
                    f"HTTP {resp.status_code}", _retry_after(resp.headers.get("retry-after"))
                )
            if resp.status_code != 200:
                raise PermanentDownloadError(f"HTTP {resp.status_code}")

            content_type = resp.headers.get("content-type", "").split(";")[0].strip().lower()
            if not content_type.startswith("image/"):
                raise PermanentDownloadError(f"not an image: {content_type or 'no content type'}")
            declared = int(resp.headers.get("content-length") or 0)
            if declared > settings.max_image_bytes:
                raise PermanentDownloadError(f"too large: {declared} bytes")

            fd, tmp_name = tempfile.mkstemp(dir=dest.parent, prefix=".part-")
            size = 0
            try:
                with os.fdopen(fd, "wb") as fh:
                    for chunk in resp.iter_bytes(64 * 1024):
                        size += len(chunk)
                        # Content-Length can be missing or lie, so count.
                        if size > settings.max_image_bytes:
                            raise PermanentDownloadError(f"too large: over {size} bytes")
                        fh.write(chunk)
                os.replace(tmp_name, dest)
            except BaseException:
                Path(tmp_name).unlink(missing_ok=True)
                raise
    except httpx.InvalidURL as exc:
        raise PermanentDownloadError(f"invalid URL: {exc}") from exc
    except httpx.HTTPError as exc:  # timeouts, connection errors, protocol errors
        raise TransientDownloadError(f"{type(exc).__name__}: {exc}") from exc
    return DownloadedImage(content_type=content_type, size_bytes=size)
