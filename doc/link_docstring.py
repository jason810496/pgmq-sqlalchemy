import sys
from typing import Union, Dict, List, Callable
from pgmq_sqlalchemy import (
    PGMQueue,
    func,
    async_func,
)
import pgmq_sqlalchemy._docstring as doc

sys.path.append("..")

"""
{
    "group_name":{
        "callables":[
            module_name.function_name,
            module_name.class_name.method_name,
        ],
        "docstring_before": DOCSTRING_BEFORE,
        "docstring_after": DOCSTRING_AFTER,
    }
}
"""
DEFINITION_TYPE = Dict[str, Dict[str, Union[str, List[Callable]]]]

docstring_definition: DEFINITION_TYPE = {
    "create_queue": {
        "callables": [
            PGMQueue.create_queue,
            func.create_queue,
            async_func.create_queue,
        ],
        "docstring_before": doc.CREATE_QUEUE_DOCSTRING_BEFORE,
        "docstring_after": doc.CREATE_QUEUE_DOCSTRING_AFTER,
    },
    "create_partitioned_queue": {
        "callables": [
            PGMQueue.create_partitioned_queue,
            func.create_partitioned_queue,
            async_func.create_partitioned_queue,
        ],
        "docstring_before": doc.CREATE_PARTITIONED_QUEUE_DOCSTRING_BEFORE,
        "docstring_after": doc.CREATE_PARTITIONED_QUEUE_DOCSTRING_AFTER,
    },
    "drop_queue": {
        "callables": [
            PGMQueue.drop_queue,
            func.drop_queue,
            async_func.drop_queue,
        ],
        "docstring_before": doc.DROP_QUEUE_DOCSTRING_BEFORE,
        "docstring_after": doc.DROP_QUEUE_DOCSTRING_AFTER,
    },
    "list_queues": {
        "callables": [
            PGMQueue.list_queues,
            func.list_queues,
            async_func.list_queues,
        ],
        "docstring_before": doc.LIST_QUEUES_DOCSTRING_BEFORE,
        "docstring_after": doc.LIST_QUEUES_DOCSTRING_AFTER,
    },
    "send": {
        "callables": [
            PGMQueue.send,
            func.send,
            async_func.send,
        ],
        "docstring_before": doc.SEND_DOCSTRING_BEFORE,
        "docstring_after": doc.SEND_DOCSTRING_AFTER,
    },
    "send_batch": {
        "callables": [
            PGMQueue.send_batch,
            func.send_batch,
            async_func.send_batch,
        ],
        "docstring_before": doc.SEND_BATCH_DOCSTRING_BEFORE,
        "docstring_after": doc.SEND_BATCH_DOCSTRING_AFTER,
    },
    "read": {
        "callables": [
            PGMQueue.read,
            func.read,
            async_func.read,
        ],
        "docstring_before": doc.READ_DOCSTRING_BEFORE,
        "docstring_after": doc.READ_DOCSTRING_AFTER,
    },
    "read_batch": {
        "callables": [
            PGMQueue.read_batch,
            func.read_batch,
            async_func.read_batch,
        ],
        "docstring_before": doc.READ_BATCH_DOCSTRING_BEFORE,
        "docstring_after": doc.READ_BATCH_DOCSTRING_AFTER,
    },
    "read_with_poll": {
        "callables": [
            PGMQueue.read_with_poll,
            func.read_with_poll,
            async_func.read_with_poll,
        ],
        "docstring_before": doc.READ_WITH_POLL_DOCSTRING_BEFORE,
        "docstring_after": doc.READ_WITH_POLL_DOCSTRING_AFTER,
    },
    "pop": {
        "callables": [
            PGMQueue.pop,
            func.pop,
            async_func.pop,
        ],
        "docstring_before": doc.POP_DOCSTRING_BEFORE,
        "docstring_after": doc.POP_DOCSTRING_AFTER,
    },
    "delete": {
        "callables": [
            PGMQueue.delete,
            func.delete,
            async_func.delete,
        ],
        "docstring_before": doc.DELETE_DOCSTRING_BEFORE,
        "docstring_after": doc.DELETE_DOCSTRING_AFTER,
    },
    "delete_batch": {
        "callables": [
            PGMQueue.delete_batch,
            func.delete_batch,
            async_func.delete_batch,
        ],
        "docstring_before": doc.DELETE_BATCH_DOCSTRING_BEFORE,
        "docstring_after": doc.DELETE_BATCH_DOCSTRING_AFTER,
    },
    "archive": {
        "callables": [
            PGMQueue.archive,
            func.archive,
            async_func.archive,
        ],
        "docstring_before": doc.ARCHIVE_DOCSTRING_BEFORE,
        "docstring_after": doc.ARCHIVE_DOCSTRING_AFTER,
    },
    "archive_batch": {
        "callables": [
            PGMQueue.archive_batch,
            func.archive_batch,
            async_func.archive_batch,
        ],
        "docstring_before": doc.ARCHIVE_BATCH_DOCSTRING_BEFORE,
        "docstring_after": doc.ARCHIVE_BATCH_DOCSTRING_AFTER,
    },
    "purge": {
        "callables": [
            PGMQueue.purge,
            func.purge,
            async_func.purge,
        ],
        "docstring_before": doc.PURGE_DOCSTRING_BEFORE,
        "docstring_after": doc.PURGE_DOCSTRING_AFTER,
    },
    "metrics": {
        "callables": [
            PGMQueue.metrics,
            func.metrics_all,
            async_func.metrics_all,
        ],
        "docstring_before": doc.METRICS_DOCSTRING_BEFORE,
        "docstring_after": doc.METRICS_DOCSTRING_AFTER,
    },
    "metrics_all": {
        "callables": [
            PGMQueue.metrics_all,
            func.metrics_all,
            async_func.metrics_all,
        ],
        "docstring_before": doc.METRICS_ALL_DOCSTRING_BEFORE,
        "docstring_after": doc.METRICS_ALL_DOCSTRING_AFTER,
    },
}
