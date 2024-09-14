from typing import Dict, Tuple, Any, List


def paramstyle_named_to_numeric(
    sql_params: Tuple[str, Dict[str, Any]],
) -> Tuple[str, List[Any]]:
    sql, params = sql_params
    new_params = []
    for key, value in params.items():
        sql = sql.replace(f":{key}", f"${key+1}")
        new_params.append(value)
    return sql, new_params
