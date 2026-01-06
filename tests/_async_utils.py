"""Utility functions for handling both sync and async PGMQueue instances in tests."""

from typing import Any, Optional
from pgmq_sqlalchemy import PGMQueue


def call_method(pgmq: PGMQueue, method_name: str, *args, **kwargs) -> Any:
    """
    Call a method on PGMQueue, automatically handling sync vs async.
    
    For async PGMQueue instances, calls the async version of the method
    and runs it with loop.run_until_complete.
    
    Args:
        pgmq: The PGMQueue instance
        method_name: Name of the method to call (without _async suffix)
        *args: Positional arguments to pass to the method
        **kwargs: Keyword arguments to pass to the method
    
    Returns:
        The result from the method call
    """
    if pgmq.is_async:
        # Call the async version
        async_method_name = method_name + '_async'
        async_method = getattr(pgmq, async_method_name)
        return pgmq.loop.run_until_complete(async_method(*args, **kwargs))
    else:
        # Call the sync version
        method = getattr(pgmq, method_name)
        return method(*args, **kwargs)
