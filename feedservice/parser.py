"""Streaming parser for Google Merchant style RSS feeds.

Items are yielded one by one and their elements are freed right after, so
memory stays flat no matter how big the feed is. defusedxml rejects DTDs and
entity declarations, which closes XXE and "billion laughs" style attacks.
"""

import re
from collections.abc import Iterator
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import BinaryIO
from xml.etree.ElementTree import Element, ParseError

from defusedxml import DefusedXmlException
from defusedxml.ElementTree import iterparse

# Feeds use the Google namespace (g:id, g:price, ...) for most fields but plain
# RSS elements for title/description/link. We match on the local name only, so
# both "g:title" and "title" work.
IMAGE_FIELDS = ("image_link", "additional_image_link")
_PRICE_RE = re.compile(r"^\s*(\d+(?:[.,]\d+)?)\s*([A-Za-z]{3})?\s*$")


@dataclass
class ParsedItem:
    item_id: str
    title: str | None = None
    description: str | None = None
    link: str | None = None
    price: Decimal | None = None
    sale_price: Decimal | None = None
    currency: str | None = None
    condition: str | None = None
    availability: str | None = None
    brand: str | None = None
    gtin: str | None = None
    item_group_id: str | None = None
    image_urls: list[str] = field(default_factory=list)


class FeedParseError(Exception):
    """The document is not valid XML (raised after already yielded items)."""


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def parse_price(raw: str | None) -> tuple[Decimal | None, str | None]:
    """'299.00 NOK' -> (Decimal('299.00'), 'NOK'). Unparseable -> (None, None)."""
    if not raw:
        return None, None
    m = _PRICE_RE.match(raw)
    if not m:
        return None, None
    try:
        amount = Decimal(m.group(1).replace(",", "."))
    except InvalidOperation:
        return None, None
    return amount, (m.group(2) or "").upper() or None


def _to_item(elem: Element) -> ParsedItem | None:
    values: dict[str, str] = {}
    images: list[str] = []
    for child in elem:
        name = _local(child.tag)
        text = (child.text or "").strip()
        if not text:
            continue
        if name in IMAGE_FIELDS:
            if text.startswith(("http://", "https://")) and text not in images:
                # The main image always goes first (position 0).
                if name == "image_link":
                    images.insert(0, text)
                else:
                    images.append(text)
        else:
            values.setdefault(name, text)

    item_id = values.get("id")
    if not item_id:
        return None
    price, currency = parse_price(values.get("price"))
    sale_price, sale_currency = parse_price(values.get("sale_price"))
    return ParsedItem(
        item_id=item_id,
        title=values.get("title"),
        description=values.get("description"),
        link=values.get("link"),
        price=price,
        sale_price=sale_price,
        currency=currency or sale_currency,
        condition=values.get("condition"),
        availability=values.get("availability"),
        brand=values.get("brand"),
        gtin=values.get("gtin"),
        item_group_id=values.get("item_group_id"),
        image_urls=images,
    )


def iter_items(source: BinaryIO | str) -> Iterator[ParsedItem]:
    """Yield items from the feed. Items without an ID are skipped.

    Raises FeedParseError if the XML is malformed; items that were complete
    before the error have already been yielded by then.
    """
    try:
        stack: list[Element] = []
        for event, elem in iterparse(source, events=("start", "end"), forbid_dtd=True):
            if event == "start":
                stack.append(elem)
                continue
            stack.pop()
            if _local(elem.tag) == "item":
                item = _to_item(elem)
                if item is not None:
                    yield item
                # Free memory: drop the processed item from its parent.
                if stack:
                    stack[-1].remove(elem)
    except (ParseError, DefusedXmlException) as exc:
        raise FeedParseError(f"{type(exc).__name__}: {exc}") from exc
