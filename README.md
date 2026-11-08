# Item Feed service

A small service that accepts XML item feeds, stores the items in PostgreSQL and
downloads their images in the background.

Stack: Python 3.13, FastAPI, PostgreSQL, RabbitMQ, Dramatiq, httpx, defusedxml.

## Run

```sh
docker compose up --build
```

The API listens on http://localhost:8000. OpenAPI docs are at
http://localhost:8000/docs, the RabbitMQ UI at http://localhost:15672 (guest/guest).

Services in `docker-compose.yml`:

| service        | job                                                       |
|----------------|-----------------------------------------------------------|
| `api`          | upload and read endpoints; never processes a feed itself  |
| `feed-worker`  | consumes queue `feeds`: parses a feed, upserts items, publishes one message per image |
| `image-worker` | consumes queue `images`: downloads one image and stores it |
| `db`           | PostgreSQL                                                |
| `rabbitmq`     | message broker                                            |

Workers scale independently, for example `docker compose up --scale image-worker=4`.

## Tests

```sh
docker compose run --rm --build tests
```

This starts PostgreSQL, creates a `feeds_test` database and runs pytest. The
main test uploads `examples/feed_example.xml` through the API, lets a real
Dramatiq worker process it (in-memory stub broker) against a local HTTP server
that plays the image host (one image OK, one failing once with 503, one 404,
one returning HTML) and then reads everything back through the API. Another
test delivers the same feed message twice and checks that nothing is
duplicated.

## Example requests

The example feed points to images that no longer exist (all return 404), so
`examples/feed_reachable_images.xml` has the same items with working image URLs
plus one URL that always returns 503.

```sh
# Upload (raw body). 202 Accepted, Location header points to the feed.
curl -i -X POST http://localhost:8000/feeds \
  -H "Content-Type: application/xml" \
  --data-binary @examples/feed_reachable_images.xml
# {"id":"e8cc36fe-9598-403e-8c7a-7b6b24b52bdc"}

FEED=e8cc36fe-9598-403e-8c7a-7b6b24b52bdc

# Status: processing | done | failed, plus progress counters
curl http://localhost:8000/feeds/$FEED
# {"id":"...","status":"done","error":null,"items":3,
#  "images":{"pending":0,"downloaded":2,"failed":2}}

# Item IDs (paginated, see below)
curl http://localhost:8000/feeds/$FEED/items
# ["6445","6446","M0296"]
curl -i "http://localhost:8000/feeds/$FEED/items?limit=2"
# Link: <http://localhost:8000/feeds/.../items?limit=2&after=6446>; rel="next"

# One item, including its images and their download status
curl http://localhost:8000/feeds/$FEED/items/M0296

# IDs of downloaded images
curl http://localhost:8000/feeds/$FEED/images

# Image bytes, with the content type of the original
curl -o image.bin -D - http://localhost:8000/feeds/$FEED/images/<IMAGE_ID>
```

## API notes

* `POST /feeds` takes the XML as the raw request body with `Content-Type`
  `application/xml`, `text/xml` or `application/rss+xml`. Responses: `202` with
  `{"id": ...}` and a `Location` header, `415` wrong content type, `400` empty
  body, `422` body that is clearly not XML, `413` over the size limit
  (`MAX_FEED_BYTES`, 100 MB), `503` if the queue is unavailable (safe to retry).
  Full XML validation happens in the worker so a big file is not parsed twice.
* `GET /feeds/{id}` returns `status` as required, plus `items`, `images` counters
  and `error`. `failed` is an extension of the spec's `processing | done`: the
  document could not be processed at all.
* Item and image lists return a plain JSON array as in the assignment. They are
  paginated with `?limit=` (default 1000, max 10000) and `?after=` (keyset
  cursor, the last ID of the previous page). If there may be more data, a
  `Link: <...>; rel="next"` header is set. While a feed is still processing the
  item list can be partial; check the feed status.
* `GET /feeds/{id}/images` lists only downloaded images, since only those can be
  fetched. Failed ones are visible per item with their URL and status.
* Images are served with the content type the origin sent, an `ETag` and
  `Cache-Control: immutable`, because the content behind an image ID never
  changes. `If-None-Match` returns `304`.
* Unknown feed, item or image: `404`. A malformed UUID in the path: `422`.
* All errors have the same shape: `{"error": {"status": 404, "message": "feed not found"}}`.

## Architecture and why

```
client -> api --(file on volume, row in Postgres, message)--> RabbitMQ
                                                          |
                     queue "feeds"  -> feed-worker  -> upsert items, images rows
                                                       -> one message per image
                     queue "images" -> image-worker -> download -> volume + row
```

* **FastAPI**: typed request handling, validation and OpenAPI docs for little
  code. The upload is streamed to disk, so memory does not depend on file size.
* **PostgreSQL**: the data is relational (feeds, items, images, item-image
  links) and `INSERT ... ON CONFLICT` gives idempotent upserts for free.
* **RabbitMQ + Dramatiq**: suggested by the assignment and a good fit. Durable
  queues, persistent messages, ack after the task finished, built-in retries
  with exponential backoff and jitter, and a dead-letter queue (`<queue>.XQ`).
  The API publishes with publisher confirms, so `202` means RabbitMQ has the
  message.
* **Two queues and two workers**: parsing is CPU/DB bound, image downloads are
  slow network calls to hosts we do not control. Separate queues mean a slow or
  dead image host never blocks parsing, and each side scales on its own.
* **Streaming, safe XML parsing**: `defusedxml` `iterparse`. Items are handled
  one by one and freed, inserts go in batches of 500. DTDs are rejected, which
  blocks XXE and entity expansion attacks. Elements are matched by local name,
  so `g:id` and `id` both work.
* **Shared volume for files**: simplest thing that works in compose. In
  production this would be object storage (S3, GCS).

### Data model

* `feeds(id, status, parsed_at, error)`
* `items(feed_id, item_id, title, description, link, price, sale_price, currency, condition, availability, brand, gtin, item_group_id)`,
  primary key `(feed_id, item_id)`. Price `"299.00 NOK"` is split into a
  decimal and a currency code.
* `images(id, feed_id, url, status, content_type, size_bytes, error)`.
  `id = uuid5(feed_id, url)`: deterministic, so the same URL in one feed (the
  example uses one main image for all three items) is stored and downloaded once.
* `item_images(feed_id, item_id, position, image_id)`: `g:image_link` is
  position 0, `g:additional_image_link` follow in order.

## Reliability

Every call that crosses a process boundary (API to broker, broker to worker,
worker to DB, worker to an image host) can fail, time out or happen twice. The
design accepts that instead of trying to prevent it:

* **At-least-once delivery, idempotent consumers.** A message is acked only
  after the task finished, so a worker that crashes mid-task gets the message
  redelivered. Handlers are written so running them twice is harmless: items
  are upserted by `(feed_id, item_id)`, images by `(feed_id, url)`, image files
  are written to a temp file and atomically renamed, and `fetch_image` skips
  images that are no longer pending. The result is effectively-once.
* **Retry only what can succeed later.** Timeouts, connection errors, 5xx and
  429 are retried with exponential backoff and jitter (up to 5 retries, 1 s to
  5 min). `Retry-After` from the image host is respected. 404 and other 4xx,
  a non-image content type or an oversized file fail immediately.
* **After the last retry** the failure is written to the DB (image or feed
  `failed`, with the reason) and the message is moved to the dead-letter queue,
  where it can be inspected or replayed.
* **One bad image does not fail the feed.** The feed status is computed from DB
  state: `done` once all items are stored and no image is pending. It is not a
  counter updated by messages, so duplicate or reordered messages cannot corrupt
  it. The check runs in its own transaction after the image row is committed,
  so whichever worker finishes the last image sees the others too.
* **Broken XML.** The example feed itself is missing the closing `</rss>` tag.
  Items that were complete before the error are kept, the feed ends as `done`
  with the parse error in `error`. A document with no usable items ends as
  `failed`. Parse errors are not retried because a retry cannot fix them.
* **Limits everywhere.** Upload size, image size (checked while streaming, not
  just from `Content-Length`), connect and read timeouts, at most 5 redirects,
  task time limits.

What happens when:

* *The worker dies mid-feed:* the message was not acked, RabbitMQ redelivers it,
  the feed is parsed again and the upserts overwrite the same rows.
* *An image host is down or slow:* timeouts and retries on the `images` queue
  only. Parsing of other feeds continues. After the retries the image is
  `failed` and the feed finishes.
* *The broker is down during upload:* the client gets `503` and can retry. (Not
  atomic, see shortcomings.)
* *The same file is uploaded twice:* two feeds. A feed is a snapshot of the
  catalog at upload time, so this is intended.

## Mapping to a production setup like yours

With **Kafka** instead of RabbitMQ the design stays the same, only the
transport changes:

* One topic per message type (`feed-uploaded`, `image-requested`), keyed by
  `feed_id`, so all messages of a feed land in one partition in order.
* Each worker type is a consumer group; parallelism is bounded by the partition
  count.
* Offsets are committed after the message was processed. This is the same
  at-least-once contract, so the idempotent handlers stay exactly as they are.
* Kafka has no per-message delayed retry or built-in DLQ, so retries would go
  to retry topics with a delay (or a retry table) and then to a `.dlq` topic.
  One slow message must not block its partition.
* The API should not write to the DB and publish to Kafka as two separate
  steps: a transactional outbox table (written in the same DB transaction as
  the feed row, relayed to Kafka) removes the gap.

On **Kubernetes** each of `api`, `feed-worker` and `image-worker` is its own
Deployment with resource limits and graceful shutdown. Workers scale on queue
lag (for example with KEDA), the API on CPU or request rate. Feed and image
files move to object storage.

What I would monitor: queue depth and age of the oldest message per queue,
DLQ size (alert when it is not zero), task duration, retry and failure rate per
task, image download failures by reason, feeds stuck in `processing` longer
than N minutes, API latency and 5xx rate. Logs would be structured and carry
`feed_id` so one feed can be followed across the services, and OpenTelemetry
trace context would be passed in message headers.

## GraphQL?

Not here. The API is a handful of resource reads plus a file upload, and REST
gets HTTP caching for immutable images for free. GraphQL would make sense if a
frontend wanted, say, items with chosen fields and their images in one round
trip. The cost is that caching moves from HTTP to the client, resolvers easily
cause N+1 queries (needs batching such as dataloaders) and queries need depth
or cost limits.

## Known shortcomings

* Upload writes the DB row and then publishes; if publishing fails the feed is
  marked `failed` and the client gets `503`, but the two steps are not atomic.
  A transactional outbox would fix it.
* If an image message is lost entirely (not just failed), its feed stays
  `processing`. A periodic sweeper re-publishing old pending images would close
  that.
* No authentication, rate limiting or per-customer quotas.
* The image downloader fetches any URL from the feed. Production needs SSRF
  protection (block private and link-local addresses, also after redirects).
* Files live on a shared volume; this does not work across machines.
* Schema is created with `create_all` on API start; production would use Alembic
  migrations.
* No metrics endpoint or tracing yet (see the monitoring list above).
* Item fields are fixed columns, as the assignment allows. Unknown feed elements
  are ignored.
* No deletion or retention of old feeds and their images.
