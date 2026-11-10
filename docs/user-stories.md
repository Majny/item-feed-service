# User stories

Who uses the service and what they need, with the acceptance criteria the
implementation meets and the test that checks each one (`tests/`).

## Client (the customer's integration)

**1. Upload a feed without waiting for it to be processed.**
As a client I want to upload a feed file and get an ID right away, so that a
large feed does not block my request.
- `POST /feeds` with the raw XML body returns `202` with `{"id"}` and a
  `Location` header; processing happens in the background.
- Wrong content type is `415`, an empty body `400`, a body that is not XML
  `422`, a body over the limit `413`.
- Tests: `test_feed_is_processed_in_background`, `test_upload_validation`.

**2. Retry an upload safely.**
As a client whose connection dropped after sending a feed, I want to send it
again without creating a duplicate feed.
- The same `Idempotency-Key` returns the first feed's ID with
  `Idempotent-Replayed: true`; another key creates another feed.
- If the queue was down on the first try (`503` + `Retry-After`), the retry
  with the same key publishes it and the feed is processed.
- An empty key is rejected (`422`).
- Tests: `test_retried_upload_with_idempotency_key_creates_one_feed`,
  `test_keyed_retry_after_a_queue_outage_publishes_again`,
  `test_empty_idempotency_key_is_rejected`.

**3. Know when the feed is ready and what went wrong.**
As a client I want to poll the status and see progress and errors.
- `GET /feeds/{id}` returns `processing`, `done` or `failed` with item and
  image counts (pending, downloaded, failed) and an error message.
- A feed whose XML breaks after some items is `done` with an error, keeping
  the items before the break.
- Tests: `test_feed_is_processed_in_background`,
  `test_example_feed_items_are_parsed_even_though_the_document_is_truncated`.

**4. Read the catalog.**
As a client I want to list item IDs and read each item with its images.
- `GET /feeds/{id}/items` returns sorted IDs, paginated with `limit`/`after`
  and a `Link: rel="next"` header.
- `GET /feeds/{id}/items/{item_id}` returns the item with prices as decimal
  strings and its images in feed order, each with its download status. IDs
  containing `/` work.
- Unknown feeds and items are `404`, malformed IDs `422`.
- Tests: `test_feed_is_processed_in_background` (price string, image order
  and status), `test_pagination`, `test_unknown_resources_return_404`,
  `test_item_id_with_a_slash_can_be_fetched`, `test_parse_price`.

**5. Get the images.**
As a client I want to list the downloaded images and fetch their bytes.
- `GET /feeds/{id}/images` lists downloaded image IDs;
  `GET /feeds/{id}/images/{image_id}` returns the bytes with the original
  content type, an `ETag` and long-lived caching (`304` on `If-None-Match`).
- Tests: `test_feed_is_processed_in_background`.

## Customer's image host (external system)

**6. Not be hammered when I am down.**
As an image host that is temporarily failing, I want the service to back off.
- `429`/`5xx`/timeouts are retried 5 times with exponential backoff and
  jitter, honouring `Retry-After`; `4xx`, non-images and redirect loops are
  not retried at all.
- Tests: `test_feed_is_processed_in_background` (a 503 is retried, a 404 and
  an HTML page are fetched only once), `test_redirect_loop_is_permanent`,
  `test_slow_drip_hits_the_download_deadline`. Retry-After and the backoff
  timing are not covered by tests.

## Operator (whoever runs the service)

**7. Trust the data after crashes and duplicates.**
As an operator I want a killed worker or a redelivered message to never
duplicate data or leave a feed stuck in `processing`.
- Duplicate feed messages do not duplicate items or images; a redelivered
  image message still completes the feed; a task killed by its time limit on
  the last attempt is recorded as a failed image.
- An item repeated in the feed keeps only its last definition's images.
- Tests: `test_duplicate_messages_do_not_duplicate_data`,
  `test_redelivery_after_a_crash_still_finishes_the_feed`,
  `test_time_limit_on_the_last_attempt_is_recorded`,
  `test_item_repeated_in_a_later_batch_keeps_only_its_last_images`.

**8. Survive outages of the database or the queue.**
As an operator I want outages to be visible and retryable, not to corrupt
state.
- A queue outage during upload returns `503` + `Retry-After` and leaves no
  orphan row or file; a database outage returns `503` too.
- `/health` (liveness) and `/ready` (database reachable) for the orchestrator.
- Tests: `test_queue_outage_returns_503_and_leaves_nothing_behind` (the
  database outage and `/ready` were checked by hand against the compose stack).

**9. Scale the slow part on its own.**
As an operator I want to add image download capacity without touching feed
parsing: `docker compose up --scale image-worker=N` (separate queues and
workers). Not covered by a test.
