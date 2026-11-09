from decimal import Decimal
from io import BytesIO
from pathlib import Path

import pytest

from feedservice.parser import FeedParseError, iter_items, parse_price

EXAMPLE = Path(__file__).parent.parent / "examples" / "feed_example.xml"


def test_example_feed_items_are_parsed_even_though_the_document_is_truncated():
    # The example ends with </channel> and never closes <rss>, so the parser
    # yields all three items and then reports the error.
    items = []
    with pytest.raises(FeedParseError), EXAMPLE.open("rb") as fh:
        for item in iter_items(fh):
            items.append(item)

    assert [i.item_id for i in items] == ["M0296", "6446", "6445"]
    first = items[0]
    assert first.title == "Bodylab Men''s T-shirt - Black"
    assert (first.price, first.currency, first.sale_price) == (Decimal("299.00"), "NOK", None)
    assert first.image_urls[0].endswith("mens-t-shirt-black-p.jpg")  # main image first
    assert len(first.image_urls) == 3


def test_xxe_is_rejected():
    doc = b"""<?xml version="1.0"?>
<!DOCTYPE rss [<!ENTITY xxe SYSTEM "file:///etc/passwd">]>
<rss><channel><item><id>1</id><title>&xxe;</title></item></channel></rss>"""
    with pytest.raises(FeedParseError):
        list(iter_items(BytesIO(doc)))


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("299.00 NOK", (Decimal("299.00"), "NOK")),
        ("12,5", (Decimal("12.5"), None)),
        ("", (None, None)),
        ("free", (None, None)),
    ],
)
def test_parse_price(raw, expected):
    assert parse_price(raw) == expected
