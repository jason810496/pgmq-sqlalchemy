from typing import Dict, Tuple, Any, List, Callable

from .._types import PARAM_STYLE_TYPE


def paramstyle_decorator(func: Callable) -> Callable:
    """
    add `paramstyle: PARAM_STYLE_TYPE` parameter to inner function
    """

    def inner(paramstyle: PARAM_STYLE_TYPE, *args, **kwargs):
        return func(*args, **kwargs)

    return inner


def paramstyle_named_to_format(
    sql_params: Tuple[str, Dict[str, Any]],
) -> Tuple[str, List[Any]]:
    sql, params = sql_params
    new_params = []
    for key, value in params.items():
        sql = sql.replace(f":{key}", "%s")
        new_params.append(value)
    return sql, new_params


def paramstyle_named_to_numeric(
    sql_params: Tuple[str, Dict[str, Any]],
) -> Tuple[str, List[Any]]:
    sql, params = sql_params
    new_params = []
    for key, value in params.items():
        sql = sql.replace(f":{key}", f"${key+1}")
        new_params.append(value)
    return sql, new_params
