# Store API - Quick Reference

## Commands

### Development
```bash
# Install dependencies
poetry install

# Run server
poetry run uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload

# Run tests
poetry run pytest tests/ -v

# Run tests with coverage
poetry run pytest tests/ --cov=api --cov-report=term-missing
```

### API Calls

#### Example request data:

Example 1:

cart-id: `9a306ece-d479-4056-a2b7-fa33859c15by`
```json
{
    "items": [
        {"name": "blue berry", "quantity": 1, "price": 90.0},
        {"name": "Strawberry", "quantity": 2, "price": 55.0}
    ]
}
```

Example 2:

cart-id: `8a306ece-4056-d479-a2b7-fa33859c15bh`
```json
{
    "items": [
        {"name": "Mango", "quantity": 1, "price": 80.0},
        {"name": "Banana", "quantity": 2, "price": 45.0}
    ]
}
```

#### Curl commands

```bash
# Submit cart
curl -X POST http://localhost:8000/store/cart \
  -H "Content-Type: application/json" \
  -d '{"items":[{"name":"Potato","quantity":1,"price":10.0}]}'

# Get queue stats
curl http://localhost:8000/store/queue/store-cart/stats

# List queues
curl http://localhost:8000/store/queues

# List queues with prefix
curl "http://localhost:8000/store/queues?prefix=store&limit=10"

# Get pending messages
curl http://localhost:8000/store/queue/store-cart/messages/pending

# Delete queue
curl -X DELETE http://localhost:8000/store/queue/test-queue

# Health check
curl http://localhost:8000/store/health
```

## SDK Features Used

### Queue Operations
- `create_queue()` - Create SIMPLE/EXCLUSIVE queues
- `get_queue()` - Get queue statistics
- `list_queues()` - List queues with prefix filter
- `delete_queue()` - Delete a queue

### Message Operations  
- `post_message()` - Publish message to queue
- `get_next_message()` - Consume next message with lease
- `acknowledge_message()` - Acknowledge message processing

### Configuration Options
- `QueueOptions` - Configure queue behavior
- `PostMessageParams` - Message publishing parameters
- `PostMessageOptions` - Message options (priority, state, etc.)
- `AcknowledgeMessageParams` - Acknowledgement parameters
- `TlsConfig` - TLS/SSL configuration

## Key Code Patterns

### Queue Creation
```python
from nzovu.utils import QueueOptions, QueueType

queue_options = QueueOptions(
    type=QueueType.SIMPLE,
    exclusivity_key="",
    dequeue_attempts=2,
    lease_duration="1m",
)
client.create_queue(name="my-queue", options=queue_options)
```

### Post Message
```python
from nzovu.utils import PostMessageParams

params = PostMessageParams(
    message_id="msg-123",
    data={"key": "value"},
    queue_name="my-queue"
)
response = client.post_message(msg_params=params)
```

### Consume Message
```python
response = client.get_next_message(
    queue_name="my-queue",
    lease_duration="10s",
    enable_heartbeat=True
).to_proto()

if response and response.message:
    # Process message
    data = json_format.MessageToDict(
        response.message.metadata.payload.data,
        preserving_proto_field_name=True
    )
```

### Acknowledge Message
```python
from nzovu.utils import AcknowledgeMessageParams, MessageState

params = AcknowledgeMessageParams(
    message_id=response.message.message_id,
    queue_name="my-queue",
    state=MessageState.COMPLETED.value,
)
client.acknowledge_message(params=params)
```

## File Structure

```
api/
├── main.py                        # FastAPI app with startup/shutdown
├── models/
│   ├── request_models.py          # CartItems, Item
│   └── response_models.py         # CartResponse, CheckoutResponse
├── routes/
│   └── store.py                   # REST endpoints
└── workers/
    ├── store_cart_worker.py       # Cart submission
    ├── process_store_cart_worker.py   # Cart & checkout processors
    └── queue_manager_worker.py    # Queue creation

config/
└── settings.py                    # Environment configuration

tests/
├── conftest.py                    # Fixtures
├── test_store.py                  # API tests (9 tests)
└── test_workers.py                # Worker tests (6 tests)
```
