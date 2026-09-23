# Nzovu store API

Runnable sync/async SDK example. A cart enters `store-cart`; a worker calculates
its total and posts `<cart-id>-checkout`; an exclusive worker acknowledges checkout.
Queues are created before consumers start. One client serves routes/workers and
is closed at application shutdown, with every worker task joined.

## Run

Build the server at the commit in `../../proto/SOURCE.json`. Set `NZOVU_IMAGE`
to that locally built image; Compose uses PostgreSQL and binds development ports
to loopback. No published SDK or server release is assumed.

```bash
NZOVU_IMAGE=your-local-nzovu-image docker compose -f deploy/docker-compose.yaml up -d
poetry sync --with dev
export NZOVU_HOST=localhost NZOVU_PORT=9000
export NZOVU_TLS_ENABLED=false NZOVU_API_KEY=local-example-key
poetry run uvicorn api.main_async:app --host 127.0.0.1 --port 8000
# Sync SDK variant: api.main:app
```

`NZOVU_TLS_ENABLED` defaults to `true`. Configure private CA/mTLS paths explicitly
with `NZOVU_CA_FILE`, `NZOVU_CERT_FILE`, `NZOVU_KEY_FILE`. Missing certificates do
not silently disable TLS. The example always sets a five-second RPC deadline.

```bash
curl -X POST 'http://localhost:8000/store/cart?cart_id=cart-1' \
  -H 'Content-Type: application/json' \
  -d '{"items":[{"name":"apple","quantity":2,"price":3.50}]}'
curl 'http://localhost:8000/store/queues?page_size=10'
curl 'http://localhost:8000/store/queue/store-cart/stats'
```

Enqueue is awaited before returning `queued`; server failures reach the HTTP
caller. Pagination uses `page_size`/`page_token`, with `nextPageToken` in JSON.
Queue counts come from `get_queue_state().state_counts`. Protobuf JSON encodes
int64 counts as strings; the pending-count endpoint explicitly returns integers.

Workers ACK with the original immutable claim. They use deterministic downstream
IDs to handle duplicate forward attempts; abandoned claims stop heartbeat and
expire for retry. This sample has no payments or durable business-side deduplication:
production consumers must make external side effects idempotent. A successful
checkout logs its total. Processing failures are logged and eventually reach DLQ
once server retries are exhausted.

Both application variants use async HTTP routes; synchronous SDK operations run
in a thread and settle before cancellation completes. The async variant uses
native async SDK calls throughout, including routes.

```bash
poetry run python -m pytest tests/ -v
```

The example has its own committed Poetry lock. Publishing is disabled in the SDK.
