# FastAPI Pub/Sub Example with PGMQ

This example demonstrates a real-world scenario of using PGMQ with FastAPI for an order management system. It shows how to:

- Use PGMQ with FastAPI and sync SQLAlchemy sessions (psycopg2)
- Publish messages using `PGMQOperation` (op) in a web API
- Consume messages asynchronously using `PGMQueue` with asyncpg

## Architecture

- **API Server (api.py)**: FastAPI application that creates orders and publishes them to PGMQ
  - Uses sync database driver (psycopg2)
  - Uses `PGMQOperation` (imported as `op`) for publishing messages
  - Provides REST endpoints for creating and retrieving orders

- **Consumer (consumer.py)**: Async worker that processes orders from the queue
  - Uses async database driver (asyncpg)
  - Uses `PGMQueue` class for reading messages
  - Processes messages concurrently with asyncio

## Prerequisites

- PostgreSQL with PGMQ extension installed
- Python 3.9 or higher

Quick setup:
```bash
docker run -d --name postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 quay.io/tembo/pg16-pgmq:latest
```

## Installation

Install required dependencies using uv:

```bash
uv pip install fastapi uvicorn psycopg2-binary asyncpg pgmq-sqlalchemy
```

Or install from the project root with uv:

```bash
cd /path/to/pgmq-sqlalchemy
uv pip install -e ".[psycopg2-binary,asyncpg]"
```

## Running the Example

### 1. Start the API Server

```bash
python api.py
```

The API will be available at http://localhost:8000

### 2. Start the Consumer

In a separate terminal:

```bash
python consumer.py
```

### 3. Create Orders

Create an order via the API:

```bash
curl -X POST "http://localhost:8000/orders" \
  -H "Content-Type: application/json" \
  -d '{
    "customer_name": "John Doe",
    "product_name": "Widget",
    "quantity": 5,
    "price": 29.99
  }'
```

You should see:
- The API returns the created order with a message ID
- The consumer logs show the order being processed

### 4. View Order

Get an order by ID:

```bash
curl "http://localhost:8000/orders/1"
```

## API Endpoints

- `POST /orders` - Create a new order
- `GET /orders/{order_id}` - Get order by ID
- `GET /health` - Health check endpoint

## How It Works

1. When an order is created via the API:
   - The order is saved to the database
   - A message is published to PGMQ using `op.send()`
   - The message contains order details

2. The consumer:
   - Continuously polls the queue for new messages
   - Processes messages concurrently using asyncio
   - Deletes successfully processed messages
   - Leaves failed messages in the queue for retry

## Configuration

You can modify the following constants in the files:

- `DATABASE_URL`: PostgreSQL connection string
- `QUEUE_NAME`: Name of the PGMQ queue (default: "order_queue")
- `batch_size`: Number of messages to process in each batch (consumer.py)
- `vt`: Visibility timeout in seconds (consumer.py)
