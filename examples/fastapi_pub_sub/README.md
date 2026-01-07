# FastAPI Pub/Sub Example with PGMQ

This example demonstrates a real-world scenario of using PGMQ with FastAPI for an order management system. It shows how to:

- Use PGMQ with FastAPI and sync SQLAlchemy sessions (psycopg2)
- Publish messages using `PGMQOperation` (op) in a web API
- Consume messages asynchronously using `PGMQueue` with asyncpg


- **Scenario**:
  1. Client creates an order via the API.
  2. The API saves the order to the database and publishes a message to PGMQ within the same transaction.
  3. The consumer polls the queue, processes messages, deletes the message on success, or leaves it for retry on failure.
    - We **simulate** that if `msg_id` modulo 6 == 0, it will fail twice before succeeding, and if `msg_id` modulo 2 == 0, it will fail once before succeeding.

## Architecture

- **API Server (api.py)**: FastAPI application that creates orders and publishes them to PGMQ
  - Uses sync database driver (psycopg2)
  - Uses `PGMQOperation` (imported as `op`) for publishing messages
  - Provides REST endpoints for creating and retrieving orders
    - `POST /orders` - Create a new order
    - `GET /orders/{order_id}` - Get order by ID
    - `GET /health` - Health check endpoint

- **Consumer (consumer.py)**: Async worker that processes orders from the queue
  - Uses async database driver (asyncpg)
  - Uses `PGMQueue` class for reading messages
  - Processes messages concurrently with asyncio

- **Create Orders Script (create_orders_coordinator.py)**: Script to create multiple orders in parallel via the API for testing

## Prerequisites

- PostgreSQL with PGMQ extension installed
- Python 3.10 or higher

Quick setup:
```bash
docker run -d --name postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 quay.io/tembo/pg16-pgmq:latest
```

Install dependencies from the project root with uv:

```bash
cd /path/to/pgmq-sqlalchemy
uv sync --group dev
```

## Running the Example with Pytest Script

You can run the entire example (API server, consumer, and order creation) using the provided pytest script:
```bash
cd /path/to/pgmq-sqlalchemy
uv run pytest ./examples_tests/integration/test_fastapi_integration.py -ss
```

This command starts the API server and consumer, and creates orders automatically. You will see logs from all three processes in real time from the terminal:
```
========================================================================================================================= test session starts ==========================================================================================================================
platform darwin -- Python 3.12.12, pytest-7.4.4, pluggy-1.6.0
rootdir: /Users/jason/pgmq-sqlalchemy
configfile: pyproject.toml
plugins: asyncio-0.23.8, anyio-4.12.0, xdist-3.8.0, lazy-fixture-0.6.3, cov-7.0.0
asyncio: mode=Mode.AUTO
collected 1 item                                                                                                                                                                                                                                                       

examples_tests/integration/test_fastapi_integration.py 
examples_tests/integration/test_fastapi_integration.py 
Starting API process...
API process started with PID: 95531
Starting Consumer process...
Consumer process started with PID: 95532
Starting Create Orders process...
Create Orders process started with PID: 95533


╭───────────────────────────────── API process: 12653 ─────────────────────────────────╮╭────────────────────────────── Consumer process: 12654 ───────────────────────────────╮╭──────────────────────────── Create Orders process: 12655 ────────────────────────────╮
│ Starting API process...                                                              ││ Starting Consumer process...                                                         ││ Starting Create Orders process...                                                    │
│ INFO:     127.0.0.1:54358 - "GET /health HTTP/1.1" 200 OK                            ││ [2026-01-07 19:48:03][INFO] - Starting consumer for queue: order_queue               ││ [2026-01-07 19:48:03][INFO] - API Server is not ready...                             │
│ INFO:     Started server process [12653]                                             ││ [2026-01-07 19:48:03][INFO] - Batch size: 30, Visibility timeout: 10s                ││ [2026-01-07 19:48:03][INFO] - API Server is not ready...                             │
│ INFO:     127.0.0.1:54359 - "POST /orders HTTP/1.1" 201 Created                      ││ [2026-01-07 19:48:04][INFO] - Received 30 messages                                   ││ [2026-01-07 19:48:03][INFO] - API Server is not ready...                             │
│ INFO:     Waiting for application startup.                                           ││ [2026-01-07 19:48:05][INFO] - Order 111 processed fail at first try                  ││ [2026-01-07 19:48:03][INFO] - API Server is not ready...                             │
│ INFO:     127.0.0.1:54360 - "POST /orders HTTP/1.1" 201 Created                      ││ [2026-01-07 19:48:05][INFO] - Order 102 processed fail at first try                  ││ [2026-01-07 19:48:03][INFO] - HTTP Request: GET http://localhost:8000/health         │
│ INFO:     Application startup complete.                                              ││ [2026-01-07 19:48:05][INFO] - Order 116 processed fail at first try                  ││ "HTTP/1.1 200 OK"                                                                    │
│ INFO:     127.0.0.1:54368 - "POST /orders HTTP/1.1" 201 Created                      ││ [2026-01-07 19:48:05][INFO] - Order 122 processed fail at first try                  ││ [2026-01-07 19:48:03][INFO] - API Server is ready!                                   │
│ INFO:     Uvicorn running on http://0.0.0.0:8000 (Press CTRL+C to quit)              ││ [2026-01-07 19:48:05][INFO] - Order 130 processed fail at first try                  ││ [2026-01-07 19:48:03][INFO] - HTTP Request: POST http://localhost:8000/orders        │
│ INFO:     127.0.0.1:54363 - "POST /orders HTTP/1.1" 201 Created                      ││ [2026-01-07 19:48:05][INFO] - Order 104 processed fail at first try                  ││ "HTTP/1.1 201 Created"                                                               │
│ INFO:     127.0.0.1:54362 - "POST /orders HTTP/1.1" 201 Created                      ││ [2026-01-07 19:48:05][INFO] - Order 119 processed fail at first try                  ││ [2026-01-07 19:48:03][INFO] - HTTP Request: POST http://localhost:8000/orders        │
│ INFO:     127.0.0.1:54369 - "POST /orders HTTP/1.1" 201 Created                      ││ [2026-01-07 19:48:05][INFO] - Order 108 processed fail at first try                  ││ "HTTP/1.1 201 Created"                                                               │
│ INFO:     127.0.0.1:54364 - "POST /orders HTTP/1.1" 201 Created                      ││ [2026-01-07 19:48:05][INFO] - Order 118 processed fail at first try                  ││ [2026-01-07 19:48:03][INFO] - HTTP Request: POST http://localhost:8000/orders        │
│ INFO:     127.0.0.1:54366 - "POST /orders HTTP/1.1" 201 Created                      ││ [2026-01-07 19:48:05][INFO] - Order 127 processed fail at first try                  ││ "HTTP/1.1 201 Created"                                                               │
│ INFO:     127.0.0.1:54372 - "POST /orders HTTP/1.1" 201 Created                      ││ [2026-01-07 19:48:05][INFO] - Order 109 processed fail at first try                  ││ [2026-01-07 19:48:03][INFO] - HTTP Request: POST http://localhost:8000/orders        │
│ INFO:     127.0.0.1:54367 - "POST /orders HTTP/1.1" 201 Created                      ││ [2026-01-07 19:48:05][INFO] - Order 128 processed fail at first try                  ││ "HTTP/1.1 201 Created"                                                               │
│ INFO:     127.0.0.1:54370 - "POST /orders HTTP/1.1" 201 Created                      ││ [2026-01-07 19:48:05][INFO] - Order 112 processed fail at first try                  ││ [2026-01-07 19:48:03][INFO] - HTTP Request: POST http://localhost:8000/orders        │
│ INFO:     127.0.0.1:54373 - "POST /orders HTTP/1.1" 201 Created                      ││ [2026-01-07 19:48:05][INFO] - Order 124 processed fail at first try                  ││ "HTTP/1.1 201 Created"                                                               │
│ INFO:     127.0.0.1:54365 - "POST /orders HTTP/1.1" 201 Created                      ││ [2026-01-07 19:48:05][INFO] - Order 113 processed fail at first try                  ││ [2026-01-07 19:48:03][INFO] - HTTP Request: POST http://localhost:8000/orders        │
│ INFO:     127.0.0.1:54371 - "POST /orders HTTP/1.1" 201 Created                      ││ [2026-01-07 19:48:05][WARNING] - Message 110 processing failed, will retry later     ││ "HTTP/1.1 201 Created"                                                               │
│ INFO:     127.0.0.1:54361 - "POST /orders HTTP/1.1" 201 Created                      ││ [2026-01-07 19:48:05][WARNING] - Message 102 processing failed, will retry later     ││ [2026-01-07 19:48:03][INFO] - HTTP Request: POST http://localhost:8000/orders        │
│ INFO:     127.0.0.1:54376 - "POST /orders HTTP/1.1" 201 Created                      ││ [2026-01-07 19:48:05][WARNING] - Message 116 processing failed, will retry later     ││ "HTTP/1.1 201 Created"                                                               │
│ INFO:     127.0.0.1:54380 - "POST /orders HTTP/1.1" 201 Created                      ││ [2026-01-07 19:48:05][WARNING] - Message 122 processing failed, will retry later     ││ [2026-01-07 19:48:03][INFO] - HTTP Request: POST http://localhost:8000/orders        │
│ INFO:     127.0.0.1:54377 - "POST /orders HTTP/1.1" 201 Created                      ││ [2026-01-07 19:48:05][WARNING] - Message 130 processing failed, will retry later     ││ "HTTP/1.1 201 Created"                                                               │
│ INFO:     127.0.0.1:54379 - "POST /orders HTTP/1.1" 201 Created                      ││ [2026-01-07 19:48:05][WARNING] - Message 104 processing failed, will retry later     ││ [2026-01-07 19:48:03][INFO] - HTTP Request: POST http://localhost:8000/orders        │
│ INFO:     127.0.0.1:54393 - "POST /orders HTTP/1.1" 201 Created                      ││ [2026-01-07 19:48:05][WARNING] - Message 120 processing failed, will retry later     ││ "HTTP/1.1 201 Created"                                                               │
│ INFO:     127.0.0.1:54394 - "POST /orders HTTP/1.1" 201 Created                      ││                                                                                      ││ [2026-01-07 19:48:03][INFO] - HTTP Request: POST http://localhost:8000/orders        │
╰──────────────────────────────────────────────────────────────────────────────────────╯╰──────────────────────────────────────────────────────────────────────────────────────╯╰──────────────────────────────────────────────────────────────────────────────────────╯
```

## Configuration

You can modify the following environment variables before running the example:

- `DATABASE_URL`: PostgreSQL connection string
- `QUEUE_NAME`: Name of the PGMQ queue (default: "order_queue")
- `BATCH_SIZE`: Number of messages to process in each batch (consumer.py)
- `VT`: Visibility timeout in seconds (consumer.py)
- `API_PORT`: Port for the FastAPI server (default: 8000)

## How It Works

1. When an order is created via the API:
  - The order is saved to the database and published to PGMQ **within the same transaction** by using `op.send()`.
  - The message contains order details.

2. The consumer:
   - Continuously polls the queue for new messages
   - Processes messages concurrently using asyncio
   - Deletes successfully processed messages
   - Leaves failed messages in the queue for retry

## Testing

This example is covered by an integration test located at `examples_tests/integration/test_fastapi_integration.py`. It is run end to end with pytest for every pull request to ensure correctness and reliability. The GitHub Actions workflow configuration for running the tests is located in `.github/workflows/examples.yml`.

You can refer to that workflow file for more details on how to set up and run the tests.