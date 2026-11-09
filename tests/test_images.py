"""The image downloader against a misbehaving host: what is retried and what is not."""

import pytest

from feedservice import images
from feedservice.images import PermanentDownloadError, TransientDownloadError, download_image


def test_slow_drip_hits_the_download_deadline(image_server, tmp_path, monkeypatch):
    # Each byte arrives well within the read timeout, so only the whole-download
    # deadline can stop it (and it must fire before the task time limit would).
    monkeypatch.setattr(images.settings, "image_deadline_s", 0.5)
    with pytest.raises(TransientDownloadError, match="slower than"):
        download_image(f"{image_server.base_url}/drip.png", tmp_path / "img")
    assert list(tmp_path.iterdir()) == []  # no partial file left behind


def test_redirect_loop_is_permanent(image_server, tmp_path):
    with pytest.raises(PermanentDownloadError, match="TooManyRedirects"):
        download_image(f"{image_server.base_url}/loop.png", tmp_path / "img")
    assert image_server.hits["/loop.png"] == 6  # first request + 5 redirects, no retries
