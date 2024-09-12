from typing import List, Tuple


def create_queue(queue_name: str, unlogged: bool = False) -> Tuple[str, List[str]]:
    """Create a new queue."""
    if unlogged:
        return "select pgmq.create_unlogged(%(queue_name)s);", {
            "queue_name": queue_name
        }
    else:
        return "select pgmq.create(%(queue_name)s);", {"queue_name": queue_name}


def create_partitioned_queue(
    queue_name: str, partition_interval: int = 10000, retention_interval: int = 100000
) -> Tuple[str, List[str]]:
    """Create a new partitioned queue."""
    return (
        "select pgmq.create_partitioned(%(queue_name)s, %(partition_interval)s, %(retention_interval)s);",
        {
            "queue_name": queue_name,
            "partition_interval": partition_interval,
            "retention_interval": retention_interval,
        },
    )


def validate_queue_name(queue_name: str) -> Tuple[str, List[str]]:
    """Validate the length of a queue name."""
    return "select pgmq.validate_queue_name(%(queue)s);", {"queue": queue_name}


def drop_queue(queue: str, partitioned: bool = False) -> Tuple[str, List[str]]:
    """Drop a queue."""
    return "select pgmq.drop_queue(%(queue)s, %(partitioned)s);", {
        "queue": queue,
        "partitioned": partitioned,
    }


def list_queues() -> Tuple[str, List[str]]:
    """List all queues."""
    return "select queue_name from pgmq.list_queues();", {}


def send(queue_name: str, message: str, delay: int = 0) -> Tuple[str, List[str]]:
    """Send a message to a queue."""
    return "select * from pgmq.send(%(queue_name)s, %(message)s, %(delay)s);", {
        "queue_name": queue_name,
        "message": message,
        "delay": delay,
    }


def send_batch(queue_name: str, messages: str, delay: int = 0) -> Tuple[str, List[str]]:
    """Send a batch of messages to a queue."""
    return "select * from pgmq.send_batch(%(queue_name)s, %(messages)s, %(delay)s);", {
        "queue_name": queue_name,
        "messages": messages,
        "delay": delay,
    }


def read(queue_name: str, vt: int) -> Tuple[str, List[str]]:
    """Read a message from a queue."""
    return "select * from pgmq.read(%(queue_name)s, %(vt)s, 1);", {
        "queue_name": queue_name,
        "vt": vt,
    }


def read_batch(queue_name: str, vt: int, batch_size: int) -> Tuple[str, List[str]]:
    """Read a batch of messages from a queue."""
    return "select * from pgmq.read(%(queue_name)s, %(vt)s, %(batch_size)s);", {
        "queue_name": queue_name,
        "vt": vt,
        "batch_size": batch_size,
    }


def read_with_poll(
    queue_name: str, vt: int, qty: int, max_poll_seconds: int, poll_interval_ms: int
) -> Tuple[str, List[str]]:
    """Read messages from a queue with polling."""
    return (
        "select * from pgmq.read_with_poll(%(queue_name)s, %(vt)s, %(qty)s, %(max_poll_seconds)s, %(poll_interval_ms)s);",
        {
            "queue_name": queue_name,
            "vt": vt,
            "qty": qty,
            "max_poll_seconds": max_poll_seconds,
            "poll_interval_ms": poll_interval_ms,
        },
    )


def pop(queue_name: str) -> Tuple[str, List[str]]:
    """Pop a message from a queue."""
    return "select * from pgmq.pop(%(queue_name)s);", {"queue_name": queue_name}


def delete(queue_name: str, msg_id: int) -> Tuple[str, List[str]]:
    """Delete a message from a queue."""
    return "select * from pgmq.delete(%(queue_name)s, %(msg_id)s::BIGINT);", {
        "queue_name": queue_name,
        "msg_id": msg_id,
    }


def delete_batch(queue_name: str, msg_ids: List[int]) -> Tuple[str, List[str]]:
    """Delete a batch of messages from a queue."""
    return "select * from pgmq.delete(%(queue_name)s, %(msg_ids)s::BIGINT);", {
        "queue_name": queue_name,
        "msg_ids": msg_ids,
    }


def archive(queue_name: str, msg_id: int) -> Tuple[str, List[str]]:
    """Archive a message from a queue."""
    return "select pgmq.archive(%(queue_name)s, %(msg_id)s::BIGINT);", {
        "queue_name": queue_name,
        "msg_id": msg_id,
    }


def archive_batch(queue_name: str, msg_ids: List[int]) -> Tuple[str, List[str]]:
    """Archive multiple messages from a queue."""
    return "select * from pgmq.archive(%(queue_name)s, %(msg_ids)s::BIGINT);", {
        "queue_name": queue_name,
        "msg_ids": msg_ids,
    }


def purge(queue_name: str) -> Tuple[str, List[str]]:
    """Purge a queue."""
    return "select pgmq.purge_queue(%(queue_name)s);", {"queue_name": queue_name}


def metrics(queue_name: str) -> Tuple[str, List[str]]:
    """Get metrics for a queue."""
    return "select * from pgmq.metrics(%(queue_name)s);", {"queue_name": queue_name}


def metrics_all() -> Tuple[str, List[str]]:
    """Get metrics for all queues."""
    return "select * from pgmq.metrics_all();", {}
