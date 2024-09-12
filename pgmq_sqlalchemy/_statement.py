from typing import List, Tuple


def create_queue(queue_name: str, unlogged: bool = False) -> Tuple[str, List[str]]:
    """Create a new queue."""
    if unlogged:
        return "select pgmq.create_unlogged($1);", [queue_name]
    else:
        return "select pgmq.create($1);", [queue_name]


def create_partitioned_queue(
    queue_name: str, partition_interval: int = 10000, retention_interval: int = 100000
) -> Tuple[str, List[str]]:
    """Create a new partitioned queue."""
    return (
        "select pgmq.create_partitioned($1, $2, $3);",
        [queue_name, partition_interval, retention_interval],
    )


def validate_queue_name(queue_name: str) -> Tuple[str, List[str]]:
    """Validate the length of a queue name."""
    return "select pgmq.validate_queue_name($1);", [queue_name]


def drop_queue(queue: str, partitioned: bool = False) -> Tuple[str, List[str]]:
    """Drop a queue."""
    return "select pgmq.drop_queue($1, $2);", [queue, partitioned]


def list_queues() -> Tuple[str, List[str]]:
    """List all queues."""
    return "select queue_name from pgmq.list_queues();", None


def send(queue_name: str, message: str, delay: int = 0) -> Tuple[str, List[str]]:
    """Send a message to a queue."""
    return "select * from pgmq.send($1, $2, $3);", [queue_name, message, delay]


def send_batch(queue_name: str, messages: str, delay: int = 0) -> Tuple[str, List[str]]:
    """Send a batch of messages to a queue."""
    return "select * from pgmq.send_batch($1, $2, $3);", [queue_name, messages, delay]


def read(queue_name: str, vt: int) -> Tuple[str, List[str]]:
    """Read a message from a queue."""
    return "select * from pgmq.read($1, $2, 1);", [queue_name, vt]


def read_batch(queue_name: str, vt: int, batch_size: int) -> Tuple[str, List[str]]:
    """Read a batch of messages from a queue."""
    return "select * from pgmq.read($1, $2, $3);", [queue_name, vt, batch_size]


def read_with_poll(
    queue_name: str, vt: int, qty: int, max_poll_seconds: int, poll_interval_ms: int
) -> Tuple[str, List[str]]:
    """Read messages from a queue with polling."""
    return (
        "select * from pgmq.read_with_poll($1, $2, $3, $4, $5);",
        [queue_name, vt, qty, max_poll_seconds, poll_interval_ms],
    )


def pop(queue_name: str) -> Tuple[str, List[str]]:
    """Pop a message from a queue."""
    return "select * from pgmq.pop($1);", [queue_name]


def delete(queue_name: str, msg_id: int) -> Tuple[str, List[str]]:
    """Delete a message from a queue."""
    return "select * from pgmq.delete($1, $2);", [queue_name, msg_id]


def delete_batch(queue_name: str, msg_ids: List[int]) -> Tuple[str, List[str]]:
    """Delete a batch of messages from a queue."""
    return "select * from pgmq.delete($1, $2::int[]);", [queue_name, msg_ids]


def archive(queue_name: str, msg_id: int) -> Tuple[str, List[str]]:
    """Archive a message from a queue."""
    return "select pgmq.archive($1, $2);", [queue_name, msg_id]


def archive_batch(queue_name: str, msg_ids: List[int]) -> Tuple[str, List[str]]:
    """Archive multiple messages from a queue."""
    return "select pgmq.archive($1, $2::int[]);", [queue_name, msg_ids]


def purge(queue_name: str) -> Tuple[str, List[str]]:
    """Purge a queue."""
    return "select pgmq.purge_queue($1);", [queue_name]


def metrics(queue_name: str) -> Tuple[str, List[str]]:
    """Get metrics for a queue."""
    return "select * from pgmq.metrics($1);", [queue_name]


def metrics_all() -> Tuple[str, List[str]]:
    """Get metrics for all queues."""
    return "select * from pgmq.metrics_all();", None
