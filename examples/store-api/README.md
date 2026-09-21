# Store API Example - Heartbeat Demo

This example demonstrates the **enhanced heartbeat features** of the Nzovu Python SDK with **both sync and async implementations**. It showcases automatic heartbeat management for long-running message processing, proper cleanup, and observability.

## � Two Implementations

This example provides **two complete implementations**:

1. **Sync Client** (`main.py`) - Uses `NzovuClient` with threading-based heartbeats
2. **Async Client** (`main_async.py`) - Uses `AsyncNzovuClient` with asyncio-based heartbeats

Both implementations demonstrate the same heartbeat features but with different concurrency models.

## �🆕 What's New: Enhanced Heartbeat Management

This example demonstrates:

1. **Automatic Heartbeat** - Keeps message leases alive during long processing (60s in this demo)
2. **Automatic Cleanup** - Heartbeats stop immediately when messages are acknowledged
3. **Empty Message Handling** - No heartbeats started for empty queue responses
4. **Observability** - Monitor active heartbeats, statistics, and errors
5. **Concurrency** - Thread pool (sync) or asyncio tasks (async) for multiple messages
6. **Safety Limits** - Max duration and count prevent infinite loops
7. **Error Callbacks** - Get notified when heartbeats fail
8. **Queue Discovery** - List all queues with `list_queues()` method for monitoring and debugging

### Sync vs Async

| Feature | Sync Client | Async Client |
|---------|------------|--------------|
| **Concurrency Model** | Threading (ThreadPoolExecutor) | Asyncio (Tasks) |
| **Heartbeat Implementation** | Background threads | Asyncio tasks |
| **Best For** | Traditional Python apps, blocking I/O | Modern async apps, high concurrency |
| **File** | `main.py` | `main_async.py` |
| **Port** | 8001 | 8002 |

### Key Take-Aways

- ✅ Heartbeats auto-stop on acknowledge
- ✅ Only start for valid messages
- ✅ Thread pool (sync) or asyncio tasks (async) for concurrency
- ✅ Full observability API
- ✅ Max duration (120s) and count (500) limits

## Prerequisites

1. **Nzovu Server** - Must be running and accessible
   - Default: `localhost:9000`
   - Configure via env vars: `NZOVU_HOST` and `NZOVU_PORT`
   - See `deploy/docker-compose.yaml` for running with Docker

2. **Python 3.10+**
3. **Poetry** - For dependency management

## Quick Start

### Installation

```bash
cd examples/store-api
poetry install
```

The `pyproject.toml` uses the local Nzovu SDK:
```toml
nzovu = {path = "../..", develop = true}
```

### Start Nzovu Server (Optional)

If you have Docker Compose:

```bash
docker-compose -f deploy/docker-compose.yaml up -d
```

### Run the Application

**Option 1: Sync Client (Threading-based)**
```bash
poetry run uvicorn api.main:app --host 0.0.0.0 --port 8001 --reload
```

**Option 2: Async Client (Asyncio-based)**
```bash
poetry run uvicorn api.main_async:app --host 0.0.0.0 --port 8002 --reload
```

**Note**: The application will show connection errors in logs if the Nzovu server is not running. This is expected - the workers will retry connections automatically.

### Submit a Cart

**To Sync Client:**
```bash
curl -X POST http://localhost:8001/store/cart \
  -H "Content-Type: application/json" \
  -d '{
    "items": [
        {"name": "Potato", "quantity": 1, "price": 10.0},
        {"name": "Banana", "quantity": 2, "price": 15.0}
    ]
}'
```

**To Async Client:**
```bash
curl -X POST http://localhost:8002/store/cart \
  -H "Content-Type: application/json" \
  -d '{
    "items": [
        {"name": "Potato", "quantity": 1, "price": 10.0},
        {"name": "Banana", "quantity": 2, "price": 15.0}
    ]
}'
```

### Watch the Heartbeat in Action

When you submit a cart, watch the logs to see the heartbeat system working:

**Sync Client Logs:**
```
� Processing message abc123... (Active heartbeats before: 0)
   📦 Cart Items: [{'name': 'Potato', 'quantity': 1, 'price': 10.0}, ...]
⏳ Starting 60s processing for message abc123...
💓 [5s] Heartbeat active for abc123... (sent: 5, failed: 0)
💓 [10s] Heartbeat active for abc123... (sent: 10, failed: 0)
💓 [15s] Heartbeat active for abc123... (sent: 15, failed: 0)
...
✅ Finished processing message abc123...
📊 Final heartbeat stats for abc123...: sent=60, failed=0
🎯 Message abc123... acknowledged (Active heartbeats after: 0)
✅ Heartbeat successfully stopped for abc123...
```

**Async Client Logs:**
```
📥 [ASYNC] Processing message abc123... (Active heartbeats before: 0)
   📦 [ASYNC] Cart Items: [{'name': 'Potato', 'quantity': 1, 'price': 10.0}, ...]
⏳ [ASYNC] Starting 60s processing for message abc123...
💓 [ASYNC] [5s] Heartbeat active for abc123... (sent: 5, failed: 0)
💓 [ASYNC] [10s] Heartbeat active for abc123... (sent: 10, failed: 0)
💓 [ASYNC] [15s] Heartbeat active for abc123... (sent: 15, failed: 0)
...
✅ [ASYNC] Finished processing message abc123...
� [ASYNC] Final heartbeat stats for abc123...: sent=60, failed=0
🎯 [ASYNC] Message abc123... acknowledged (Active heartbeats after: 0)
✅ [ASYNC] Heartbeat successfully stopped for abc123...
```

The heartbeat monitor (runs every 10s) will also show:
```
💓 Active heartbeats: 1
   � Message abc123...: 45 sent, 0 failed
```

### Run Tests

```bash
poetry run pytest tests/ -v
```

## Features Demonstrated

### Core Features
- ✅ **Queue Creation** - SIMPLE and EXCLUSIVE queue types
- ✅ **Message Publishing** - Posting cart data to queues
- ✅ **Message Consumption** - Workers processing messages with lease duration
- ✅ **Message Acknowledgement** - Completing messages after processing
- ✅ **Background Workers** - Async workers for message processing
- ✅ **Queue Management API** - REST endpoints for monitoring queues
- ✅ **TLS Support** - Automatic TLS with certificates or fallback to insecure

### 🆕 Enhanced Heartbeat Features

#### 1. Automatic Heartbeat Management
```python
# In process_store_cart_worker.py
response = client.get_next_message(
    queue_name=QUEUE_NAME_STORE_CART,
    lease_duration="5s",
    enable_heartbeat=True  # ✅ Automatically keeps message alive
)

# Long processing (60 seconds)
time.sleep(60)  # ✅ Heartbeat renews lease every ~1s

# Acknowledge message
client.acknowledge_message(params)  # ✅ Heartbeat auto-stops here!
```

#### 2. Configurable Heartbeat Parameters
```python
# In api/main.py
client = NzovuClient(
    host=NZOVU_HOST,
    port=NZOVU_PORT,
    use_tls=False,
    heartbeat_max_duration=120,      # Max 2 minutes per message
    heartbeat_max_count=500,         # Max 500 heartbeats
    heartbeat_thread_pool_size=10,   # 10 concurrent workers
    heartbeat_error_callback=heartbeat_error_handler,
)
```

#### 3. Observability & Monitoring
```python
# Get active heartbeat count
count = client.get_active_heartbeat_count()

# Get detailed heartbeat info
active = client.get_active_heartbeats()
# Returns: {msg_id: {queue_name, started_at, duration, frequency}}

# Get heartbeat statistics
stats = client.get_heartbeat_stats()
# Returns: {msg_id: {heartbeats_sent, heartbeats_failed, ...}}

# Manually stop a heartbeat (if needed)
client.stop_heartbeat(message_id)
```

#### 4. Error Handling
```python
def heartbeat_error_handler(error_info: dict):
    """Called when heartbeat errors occur"""
    logger.error(
        f"Heartbeat Error - Message: {error_info['message_id']}, "
        f"Error: {error_info['error_details']}, "
        f"Retry Count: {error_info['retry_count']}"
    )
```

#### 5. Background Monitoring
The example includes a background task that monitors heartbeats:
```python
async def monitor_heartbeats(client):
    """Logs heartbeat status every 10 seconds"""
    active_count = client.get_active_heartbeat_count()
    if active_count > 0:
        active_hb = client.get_active_heartbeats()
        for msg_id, info in active_hb.items():
            logger.info(
                f"Message {msg_id} running for {info['duration']:.1f}s"
            )
```

## Heartbeat Demo Walkthrough

### Step 1: Start the Application
```bash
poetry run uvicorn api.main:app --host 0.0.0.0 --port 8001 --reload
```

You'll see initialization logs:
```
✅ Nzovu client initialized with enhanced heartbeat management
   - Max heartbeat duration: 120s
   - Max heartbeat count: 500
   - Thread pool size: 10
🚀 Store cart worker started
🚀 Checkout cart worker started
```

### Step 2: Submit a Cart
```bash
curl -X POST http://localhost:8001/store/cart \
  -H "Content-Type: application/json" \
  -d '{
    "items": [
        {"name": "Laptop", "quantity": 1, "price": 999.99},
        {"name": "Mouse", "quantity": 2, "price": 25.00}
    ]
}'
```

### Step 3: Observe the Logs

**Message Received:**
```
📬 Received message 550e8400... from store-cart queue
💓 Heartbeat started (active heartbeats: 1)
💰 Calculated total: $1049.99 for 2 items
```

**During Processing (60s):**
```
⏳ Simulating 60s processing time (heartbeat keeps message alive)...
   💓 Heartbeat is automatically renewing the message lease every ~1s
```

**Heartbeat Monitor (every 10s):**
```
💓 Active heartbeats: 1
   📨 Message 550e8400... on queue 'store-cart' (running for 15.3s, freq: 1s)
   📊 Stats: 15 sent, 0 failed
```

**After Acknowledgment:**
```
✅ Processing completed after 60s
💓 Active heartbeats before ACK: 1
📝 Acknowledging message 550e8400...
💓 Active heartbeats after ACK: 0
🛑 Heartbeat automatically stopped (was: 1, now: 0)
✅ DONE processing message 550e8400...
```

### Step 4: Compare with Non-Heartbeat Worker

The checkout cart worker processes quickly without heartbeat:
```
📬 Received message 660e9500... from checkout-cart queue
💓 Heartbeat: DISABLED (fast processing, no need)
⚡ Quick processing (no heartbeat needed)
✅ Message 660e9500... acknowledged
```

## Testing Edge Cases

### Test 1: Empty Queue (No Heartbeat Started)
```bash
# Observe logs when queue is empty
📭 No message available in queue (heartbeat not started)
```

### Test 2: Multiple Concurrent Messages
Submit multiple carts quickly to see thread pool in action:
```bash
for i in {1..5}; do
  curl -X POST http://localhost:8001/store/cart \
    -H "Content-Type: application/json" \
    -d "{\"items\": [{\"name\": \"Item$i\", \"quantity\": 1, \"price\": 10.0}]}" &
done
```

Observe concurrent heartbeats:
```
💓 Active heartbeats: 5
   📨 Message aaa111... on queue 'store-cart' (running for 45.2s, freq: 1s)
   📨 Message bbb222... on queue 'store-cart' (running for 43.8s, freq: 1s)
   📨 Message ccc333... on queue 'store-cart' (running for 42.1s, freq: 1s)
   ...
```

### Test 3: Graceful Shutdown
Stop the application (Ctrl+C) and watch graceful shutdown:
```
🛑 Shutting down Nzovu client...
⚠️ Closing with 2 active heartbeat(s)
Closing NzovuClient, stopping all heartbeats...
Signaling 2 active heartbeat(s) to stop
Shutting down heartbeat thread pool...
✅ Nzovu client closed successfully
```
- ✅ **Comprehensive Tests** - 15 tests covering API and workers

## Architecture

```
POST /store/cart
    ↓
store-cart Queue (SIMPLE)
    ↓
Store Cart Worker (calculate total)
    ↓
checkout-cart Queue (EXCLUSIVE)
    ↓
Checkout Cart Worker (finalize)
    ↓
COMPLETED
```

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/store/cart` | Submit cart items |
| GET | `/store/queue/{name}/stats` | Get queue statistics |
| GET | `/store/queues` | List all queues |
| GET | `/store/queue/{name}/messages/pending` | Get pending message count |
| DELETE | `/store/queue/{name}` | Delete a queue |
| GET | `/store/health` | Health check |

## Configuration

Environment variables (see `config/settings.py`):

- `NZOVU_HOST` - Nzovu server host (default: `localhost`)
- `NZOVU_PORT` - Nzovu server port (default: `9000`)
- `FASTAPI_HOST` - FastAPI server host (default: `localhost`)
- `FASTAPI_PORT` - FastAPI server port (default: `8000`)
- `QUEUE_NAME_STORE_CART` - Store cart queue name (default: `store-cart`)
- `QUEUE_NAME_CHECKOUT_CART` - Checkout cart queue name (default: `checkout-cart`)

## Documentation

See [USAGE.md](./USAGE.md) for detailed usage guide, API documentation, and code examples.

## Project Structure

```
store-api/
├── api/
│   ├── main.py                 # FastAPI application
│   ├── models/
│   │   ├── request_models.py   # Pydantic request models
│   │   └── response_models.py  # Pydantic response models
│   ├── routes/
│   │   └── store.py            # Store API endpoints
│   └── workers/
│       ├── store_cart_worker.py           # Cart submission worker
│       ├── process_store_cart_worker.py   # Cart & checkout processors
│       └── queue_manager_worker.py        # Queue creation
├── config/
│   └── settings.py             # Configuration settings
├── tests/
│   ├── conftest.py             # Test fixtures
│   ├── test_store.py           # API endpoint tests
│   └── test_workers.py         # Worker tests
├── pyproject.toml              # Poetry dependencies
└── README.md                   # This file
```