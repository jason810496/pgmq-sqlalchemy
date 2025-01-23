from typing import Any, Dict, List, Tuple
from sqlalchemy import text
from sqlalchemy.sql.elements import TextClause


def create_queue(
    queue_name: str, unlogged: bool = False
) -> Tuple[TextClause, Dict[str, Any]]:
    """Create a new queue."""
    if unlogged:
        return text("select pgmq.create_unlogged(:queue_name);"), {
            "queue_name": queue_name
        }
    else:
        return text("select pgmq.create(:queue_name);"), {"queue_name": queue_name}


def create_partitioned_queue(
    queue_name: str, partition_interval: int = 10000, retention_interval: int = 100000
) -> Tuple[TextClause, Dict[str, Any]]:
    """Create a new partitioned queue."""
    return (
        text(
            "select pgmq.create_partitioned(:queue_name, :partition_interval, :retention_interval);"
        ),
        {
            "queue_name": queue_name,
            "partition_interval": partition_interval,
            "retention_interval": retention_interval,
        },
    )


def validate_queue_name(queue_name: str) -> Tuple[TextClause, Dict[str, Any]]:
    """Validate the length of a queue name."""
    return text("select pgmq.validate_queue_name(:queue_name);"), {
        "queue_name": queue_name
    }


def drop_queue(
    queue: str, partitioned: bool = False
) -> Tuple[TextClause, Dict[str, Any]]:
    """Drop a queue."""
    return text("select pgmq.drop_queue(:queue, :partitioned);"), {
        "queue": queue,
        "partitioned": partitioned,
    }


def list_queues() -> Tuple[TextClause, Dict[str, Any]]:
    """List all queues."""
    return text("select queue_name from pgmq.list_queues();"), {}


def send(
    queue_name: str, message: str, delay: int = 0
) -> Tuple[TextClause, Dict[str, Any]]:
    """Send a message to a queue."""
    return text("select * from pgmq.send(:queue_name, :message, :delay);"), {
        "queue_name": queue_name,
        "message": message,
        "delay": delay,
    }


def send_batch(
    queue_name: str, messages: str, delay: int = 0
) -> Tuple[TextClause, Dict[str, Any]]:
    """Send a batch of messages to a queue."""
    return text("select * from pgmq.send_batch(:queue_name, :messages, :delay);"), {
        "queue_name": queue_name,
        "messages": messages,
        "delay": delay,
    }


def read(queue_name: str, vt: int) -> Tuple[TextClause, Dict[str, Any]]:
    """Read a message from a queue."""
    return text("select * from pgmq.read(:queue_name, :vt, 1);"), {
        "queue_name": queue_name,
        "vt": vt,
    }


def read_batch(
    queue_name: str, vt: int, batch_size: int
) -> Tuple[TextClause, Dict[str, Any]]:
    """Read a batch of messages from a queue."""
    return text("select * from pgmq.read(:queue_name, :vt, :batch_size);"), {
        "queue_name": queue_name,
        "vt": vt,
        "batch_size": batch_size,
    }


def read_with_poll(
    queue_name: str, vt: int, qty: int, max_poll_seconds: int, poll_interval_ms: int
) -> Tuple[TextClause, Dict[str, Any]]:
    """Read messages from a queue with polling."""
    return (
        text(
            "select * from pgmq.read_with_poll(:queue_name, :vt, :qty, :max_poll_seconds, :poll_interval_ms);"
        ),
        {
            "queue_name": queue_name,
            "vt": vt,
            "qty": qty,
            "max_poll_seconds": max_poll_seconds,
            "poll_interval_ms": poll_interval_ms,
        },
    )


def pop(queue_name: str) -> Tuple[TextClause, Dict[str, Any]]:
    """Pop a message from a queue."""
    return text("select * from pgmq.pop(:queue_name);"), {"queue_name": queue_name}


def delete(queue_name: str, msg_id: int) -> Tuple[TextClause, Dict[str, Any]]:
    """Delete a message from a queue."""
    return text("select * from pgmq.delete(:queue_name, :msg_id::BIGINT);"), {
        "queue_name": queue_name,
        "msg_id": msg_id,
    }


def delete_batch(
    queue_name: str, msg_ids: List[int]
) -> Tuple[TextClause, Dict[str, Any]]:
    """Delete a batch of messages from a queue."""
    return text("select * from pgmq.delete(:queue_name, :msg_ids);"), {
        "queue_name": queue_name,
        "msg_ids": msg_ids,
    }


def archive(queue_name: str, msg_id: int) -> Tuple[TextClause, Dict[str, Any]]:
    """Archive a message from a queue."""
    return text("select pgmq.archive(:queue_name, :msg_id::BIGINT);"), {
        "queue_name": queue_name,
        "msg_id": msg_id,
    }


def archive_batch(
    queue_name: str, msg_ids: List[int]
) -> Tuple[TextClause, Dict[str, Any]]:
    """Archive multiple messages from a queue."""
    return text("select * from pgmq.archive(:queue_name, :msg_ids);"), {
        "queue_name": queue_name,
        "msg_ids": msg_ids,
    }


def purge(queue_name: str) -> Tuple[TextClause, Dict[str, Any]]:
    """Purge a queue."""
    return text("select pgmq.purge_queue(:queue_name);"), {"queue_name": queue_name}


def metrics(queue_name: str) -> Tuple[TextClause, Dict[str, Any]]:
    """Get metrics for a queue."""
    return text("select * from pgmq.metrics(:queue_name);"), {"queue_name": queue_name}


def metrics_all() -> Tuple[TextClause, Dict[str, Any]]:
    """Get metrics for all queues."""
    return text("select * from pgmq.metrics_all();"), {}
