# Quick reference

| Operation | Command/endpoint |
| --- | --- |
| Async SDK app | `poetry run uvicorn api.main_async:app --port 8000` |
| Sync SDK app | `poetry run uvicorn api.main:app --port 8000` |
| Submit cart | `POST /store/cart?cart_id=<unique-id>` |
| Queue pages | `GET /store/queues?prefix=store&page_size=10&page_token=<token>` |
| Counts | `GET /store/queue/store-cart/stats` |
| Pending count | `GET /store/queue/store-cart/messages/pending` |
| Health | `GET /store/health` |
| Tests | `poetry run python -m pytest tests/ -v` |

See [setup and lifecycle](README.md) and [TLS configuration](CERTIFICATE.md).
