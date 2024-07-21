from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class Message:
    """
    .. _schema_message_class: `schema.Message`_
    .. |schema_message_class| replace:: :py:class:`.~pgmq_sqlalchemy.schema.Message`
    """

    msg_id: int
    read_ct: int
    enqueued_at: datetime
    vt: datetime
    message: dict


@dataclass
class QueueMetrics:
    """
    .. _schema_queue_metrics_class: `schema.QueueMetrics`_
    .. |schema_queue_metrics_class| replace:: :py:class:`.~pgmq_sqlalchemy.schema.QueueMetrics`
    """

    queue_name: str
    queue_length: int
    newest_msg_age_sec: Optional[int]
    oldest_msg_age_sec: Optional[int]
    total_messages: int
