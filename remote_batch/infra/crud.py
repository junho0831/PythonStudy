from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine, Result

IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def quote_identifier(name: str) -> str:
    if not IDENTIFIER_PATTERN.fullmatch(name):
        raise ValueError(f"허용되지 않는 식별자입니다: {name}")
    return f'"{name}"'


def quote_table_name(name: str) -> str:
    return ".".join(quote_identifier(part) for part in name.split("."))


class CrudClient:
    def __init__(self, bind: Engine | Connection):
        self.bind = bind

    def fetch_one(self, query: str, params: Mapping[str, Any] | None = None):
        return self.bind.execute(text(query), dict(params or {})).fetchone()

    def fetch_all(self, query: str, params: Mapping[str, Any] | None = None):
        return self.bind.execute(text(query), dict(params or {})).fetchall()

    def execute(self, query: str, params: Mapping[str, Any] | None = None) -> Result:
        return self.bind.execute(text(query), dict(params or {}))

    def insert(
        self,
        table: str,
        values: Mapping[str, Any],
        *,
        returning: str | None = None,
        casts: Mapping[str, str] | None = None,
    ):
        columns = ", ".join(quote_identifier(column) for column in values)
        placeholders = ", ".join(self._value_expression(column, casts) for column in values)
        sql = f"insert into {quote_table_name(table)} ({columns}) values ({placeholders})"
        if returning:
            sql += f" returning {quote_identifier(returning)}"
        result = self.execute(sql, values)
        return result.scalar_one() if returning else result.rowcount

    def update(
        self,
        table: str,
        values: Mapping[str, Any],
        *,
        where_clause: str,
        where_params: Mapping[str, Any] | None = None,
        expression_values: Mapping[str, str] | None = None,
    ) -> int:
        assignments = [f"{quote_identifier(column)} = :{column}" for column in values]
        if expression_values:
            assignments.extend(
                f"{quote_identifier(column)} = {expression}"
                for column, expression in expression_values.items()
            )
        params = {**dict(values), **dict(where_params or {})}
        result = self.execute(
            f"update {quote_table_name(table)} set {', '.join(assignments)} where {where_clause}",
            params,
        )
        return result.rowcount

    def delete(self, table: str, *, where_clause: str, params: Mapping[str, Any] | None = None) -> int:
        result = self.execute(f"delete from {quote_table_name(table)} where {where_clause}", params)
        return result.rowcount

    def upsert(
        self,
        table: str,
        values: Mapping[str, Any],
        *,
        conflict_columns: Sequence[str],
        update_columns: Sequence[str],
        casts: Mapping[str, str] | None = None,
        update_expression_values: Mapping[str, str] | None = None,
    ) -> int:
        columns = ", ".join(quote_identifier(column) for column in values)
        placeholders = ", ".join(self._value_expression(column, casts) for column in values)
        conflict = ", ".join(quote_identifier(column) for column in conflict_columns)
        updates = [
            f"{quote_identifier(column)} = excluded.{quote_identifier(column)}"
            for column in update_columns
        ]
        if update_expression_values:
            updates.extend(
                f"{quote_identifier(column)} = {expression}"
                for column, expression in update_expression_values.items()
            )
        sql = (
            f"insert into {quote_table_name(table)} ({columns}) values ({placeholders}) "
            f"on conflict ({conflict}) do update set {', '.join(updates)}"
        )
        return self.execute(sql, values).rowcount

    @staticmethod
    def _value_expression(column: str, casts: Mapping[str, str] | None) -> str:
        if casts and column in casts:
            return f"cast(:{column} as {casts[column]})"
        return f":{column}"
