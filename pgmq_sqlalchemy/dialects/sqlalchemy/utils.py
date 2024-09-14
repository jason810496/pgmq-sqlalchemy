from pgmq_sqlalchemy.dialects.sqlalchemy.types import (
    TextClause,
    text,
)


def add_sa_text_clause(sql: str) -> TextClause:
    return text(sql)
