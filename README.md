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

The schema is created on API start (`create_all`, no migrations), so after a
schema change start from clean volumes: `docker compose down -v`.

Configuration (environment variables, defaults in `feedservice/config.py`):
`DATABASE_URL`, `BROKER_URL`, `DATA_DIR`, `MAX_FEED_BYTES` (100 MB),
`MAX_IMAGE_BYTES` (10 MB), `IMAGE_TIMEOUT_S` (10, per read; connect is 5 s),
`IMAGE_DEADLINE_S` (60, reading the response body), `TASK_MAX_RETRIES` (5),
`TASK_MIN_BACKOFF_MS` / `TASK_MAX_BACKOFF_MS` (1 s / 5 min).

## Tests

```sh
docker compose run --rm --build tests
```

This starts PostgreSQL, creates a `feeds_test` database and runs pytest. The
main test uploads `examples/feed_example.xml` through the API, lets a real
Dramatiq worker process it (in-memory stub broker) against a local HTTP server
that plays the image host (one image OK, one failing once with 503, one 404,
one returning HTML) and then reads everything back through the API.

The other tests cover the failure cases from the reliability section below:
the same feed message delivered twice, a redelivery after a worker died between
two steps, a task killed by its time limit on the last attempt, a retried upload
with an `Idempotency-Key`, a broker outage during upload, an image host that
drips bytes, and a redirect loop.

Lint and format check (config in `pyproject.toml`):

```sh
ruff check . && ruff format --check .
```

## Example requests

The example feed points to images that no longer exist (all return 404), so
`examples/feed_reachable_images.xml` has the same items with two working image URLs,
one original URL that still returns 404 and one that always returns 503 (and
the closing `</rss>` the original is missing).

```sh
# Upload (raw body). 202 Accepted, Location header points to the feed.
curl -i -X POST http://localhost:8000/feeds \
  -H "Content-Type: application/xml" \
  --data-binary @examples/feed_reachable_images.xml
# {"id":"e8cc36fe-9598-403e-8c7a-7b6b24b52bdc"}

FEED=e8cc36fe-9598-403e-8c7a-7b6b24b52bdc

# Status: processing | done | failed, plus progress counters.
# The always-503 image is retried with backoff, so this feed stays
# "processing" for about a minute before it is "done".
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
IMAGE=$(curl -s http://localhost:8000/feeds/$FEED/images | python3 -c 'import sys,json; print(json.load(sys.stdin)[0])')
curl -o image.bin -D - http://localhost:8000/feeds/$FEED/images/$IMAGE

# Safe retry of an upload: the same key returns the same feed ID
curl -i -X POST http://localhost:8000/feeds \
  -H "Content-Type: application/xml" -H "Idempotency-Key: catalog-2026-11-10" \
  --data-binary @examples/feed_example.xml
```

## API notes

* `POST /feeds` takes the XML as the raw request body with `Content-Type`
  `application/xml`, `text/xml` or `application/rss+xml`. It is streamed to
  disk as it arrives; multipart (`curl -F`) is not accepted. Responses: `202`
  with `{"id": ...}` and a `Location` header, `415` wrong content type, `400`
  empty body, `422` body that is clearly not XML, `413` over the size limit
  (`MAX_FEED_BYTES`, 100 MB), `503` with `Retry-After` if the queue or the
  database is unavailable (safe to retry). Full XML
  validation happens in the worker so a big file is not parsed twice.
* `POST /feeds` accepts an optional `Idempotency-Key` header. A retry with the
  same key (the client timed out, the `202` got lost) returns the feed of the
  first request with `Idempotent-Replayed: true`, without reading the body
  again. If the first request's publish failed (`503`) or is still in flight,
  the retry publishes again; a duplicate message is harmless. Without a key
  every upload is a new feed.
* `GET /feeds/{id}` returns `status` as required, plus `items`, `images` counters
  and `error`. `failed` is an extension of the spec's `processing | done`: the
  document could not be processed at all.
* Item `price` and `sale_price` are decimal strings (`"299.00"`), not JSON
  numbers, to avoid float rounding; `currency` is a separate field.
* Item and image lists return a plain JSON array as in the assignment. They are
  paginated with `?limit=` (default 1000, max 10000) and `?after=` (keyset
  cursor, the last ID of the previous page). If there may be more data, a
  `Link: <...>; rel="next"` header is set. While a feed is still processing the
  item list can be partial; check the feed status.
* `GET /feeds/{id}/images` lists only downloaded images, since only those can be
  fetched, ordered by image ID. Failed ones, and the per-item order, are
  visible on the item with their URL and status.
* Images are served with the content type the origin sent, an `ETag` and
  `Cache-Control: immutable`, because the content behind an image ID never
  changes. `If-None-Match` returns `304`.
* Unknown feed, item or image: `404`. A malformed UUID in the path: `422`.
* All errors have the same shape: `{"error": {"status": 404, "message": "feed not found"}}`.
  Validation errors add `"details"` with the offending fields.
* `GET /health` is liveness (the process serves requests), `GET /ready` is
  readiness (the database answers, otherwise `503`).

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

### Code layout

Dependencies point one way: `api`, `feeds` and `tasks` use the modules below them,
`parser` and `images` know nothing about the database or the queue.

| Module | Responsibility |
|---|---|
| `api.py` | HTTP layer: routes, streaming the upload to disk, the (simple) read queries, response models, mapping errors to status codes |
| `feeds.py` | Registering an upload: feed row, publish, `Idempotency-Key` replay, cleanup after a failed publish |
| `tasks.py` | The two Dramatiq actors: parse and store items, download one image, retry/give-up decisions, feed completion |
| `parser.py` | Streaming, XXE-safe XML to `ParsedItem`; pure, no I/O besides reading the file |
| `images.py` | Downloading one image: timeouts, size and type checks, transient vs. permanent errors |
| `db.py` | Schema and sessions |
| `config.py` | Settings from environment variables |

Module-level singletons (settings, engine, broker) instead of dependency
injection: idiomatic for a service this size, and tests swap them with
`monkeypatch`. With more code, the next step would be passing the downloader
and the publisher into `tasks` and `feeds` explicitly.

### Data model

* `feeds(id, status, parsed_at, error, idempotency_key UNIQUE, enqueued, created_at, updated_at)`
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
  images that are no longer pending. A redelivered image message whose work is
  already done still re-runs the feed completion check, because the first
  delivery may have died between committing the image and that check. The
  stored result is effectively-once; the image host can still see a repeated
  `GET`.
* **Retry only what can succeed later.** Timeouts, connection errors, 5xx and
  429 are retried up to 5 times with exponential backoff and jitter (1-2,
  2-4, 4-8, 8-16, 16-32 s, about 30-60 s in total). `Retry-After` from the image host
  is respected, capped at 5 min. 404 and other 4xx, a redirect loop, a
  non-image content type or an oversized file fail immediately. Expected
  image retries are logged at INFO; unexpected errors (a bug, a full disk) are
  still retried but logged with a traceback, and giving up is logged by
  Dramatiq at ERROR when the message goes to the dead-letter queue.
* **After the last retry** the failure is written to the DB (image or feed
  `failed`, with the reason) and the message is moved to the dead-letter queue
  (`images.XQ`, `feeds.XQ`) for inspection. This includes a task killed by its
  time limit while it downloads: Dramatiq raises that as a `BaseException`,
  which a plain `except Exception` would miss. Replaying a dead message alone
  does nothing, since the row is already `failed`; reprocessing would need a
  small replay command (not built) that resets the row to pending and re-sends.
* **One bad image does not fail the feed.** The feed status is computed from DB
  state: `done` once all items are stored and no image is pending. It is not a
  counter updated by messages, so duplicate or reordered messages cannot corrupt
  it. The check runs in its own transaction after the image row is committed,
  so whichever worker finishes the last image sees the others too.
* **Broken XML.** The example feed itself is missing the closing `</rss>` tag.
  Items that were complete before the error are kept, the feed ends as `done`
  with the parse error in `error`. A document with no usable items ends as
  `failed`. Parse errors are not retried because a retry cannot fix them.
  Being lenient is a deliberate choice because of the sample, and it has a
  risk: a truncated upload looks complete. A non-null `error` on a `done` feed
  means "may be incomplete". In production this should be a per-customer
  policy, or the feed should fail, because importing a cut-off catalog as
  complete could remove items downstream.
* **Limits everywhere.** Upload size, image size (checked while streaming, not
  just from `Content-Length`), connect and read timeouts plus a deadline for
  reading the body (a host that drips one byte every few seconds never trips
  a read timeout), at most 5 redirects, and task time limits as a backstop.
  The feed task's limit stays below RabbitMQ's 30 min `consumer_timeout`, and
  the feed-worker prefetches only one message per thread, because that
  timeout also counts while a message waits in the worker's local buffer.

What happens when:

* *The worker dies mid-feed:* the message was not acked, RabbitMQ redelivers it,
  the feed is parsed again and the upserts overwrite the same rows.
* *An image host is down or slow:* timeouts and retries on the `images` queue
  only. Parsing of other feeds continues. After the retries the image is
  `failed` and the feed finishes.
* *The worker dies after storing an image but before checking the feed:* the
  redelivered message sees the image is done and runs the check.
* *The broker or the DB is down during upload:* the client gets `503` with
  `Retry-After` and retries. Without a key the feed row and file are removed;
  with a key they stay and the retry publishes the message again.
* *The client loses the `202` and retries:* with the same `Idempotency-Key`
  it gets the same feed back; without a key, a second feed.
* *The same file is uploaded twice on purpose:* two feeds. A feed is a
  snapshot of the catalog at upload time, so this is intended.

## Mapping to a production setup like yours

With **Kafka** instead of RabbitMQ the design stays the same, only the
transport changes:

* One topic per message type (`feed-uploaded` keyed by `feed_id`,
  `image-requested` keyed by `image_id`). Image messages need no ordering,
  because status is derived from DB state. Keying them by feed would put a
  big feed's images in one partition, consumed serially.
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

What exists today: per-feed progress counters in `GET /feeds/{id}`, queue
depth and the dead-letter queues in the RabbitMQ UI, `/health` and `/ready`,
and log lines that carry `feed=<id>` (and `image=<id>`) in both workers.

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

Roughly in the order I would fix them:

* Upload writes the DB row and then publishes. If publishing fails the client
  gets `503`, but the two steps are not atomic: a crash between them leaves a
  feed that is never processed (unless the client retries with its key). A
  transactional outbox would fix it.
* If an image message is lost entirely, or the feed task exhausts its retries
  while publishing image messages, the feed stays `processing`. A periodic
  sweeper re-publishing old pending images (and alerting on feeds stuck in
  `processing`) would close that.
* No per-host concurrency limit, rate limit or circuit breaker for image hosts:
  8 threads per worker times N workers can hammer one customer's CDN.
* Image messages are published only after the whole feed is parsed, and a
  redelivered feed message re-publishes every still-pending image. Harmless
  (duplicates are no-ops) but wasted work on big feeds.
* Every upload is a new snapshot, so re-uploading the same catalog downloads
  all images again. Cross-feed dedupe by URL with a conditional `GET`
  (`ETag`/`If-Modified-Since`) or by content hash would avoid it.
* The `Idempotency-Key` is global (there are no customers yet), never expires
  and is not checked against the body, so a reused key with a different file
  returns the first feed.
* If an item ID appears twice in a feed in different batches, the images of the
  overwritten definition are still downloaded and counted, just not linked to
  any item.
* Tests use Dramatiq's stub broker, so the RabbitMQ-specific parts (publisher
  confirms, delayed retries, `.XQ` dead-lettering) were checked by hand with
  `docker compose`, not by the test suite.
* No authentication, rate limiting or per-customer quotas.
* The image downloader fetches any URL from the feed. Production needs SSRF
  protection (block private and link-local addresses, also after redirects).
* Files live on a shared volume; this does not work across machines.
* Schema is created with `create_all` on API start; production would use Alembic
  migrations.
* No metrics endpoint or tracing yet (see the monitoring list above).
* Item fields are fixed columns, as the assignment allows. Unknown feed elements
  are ignored.
* No deletion or retention of old feeds and their images (nor of `.part-`
  temp files a hard-killed worker can leave behind).
