from __future__ import annotations

import os
import re
from contextlib import contextmanager

import pandas as pd


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class PostgresDB:
    def __init__(self, dsn: str | None = None):
        self.dsn = dsn or os.getenv("ER_DOSE_DB_DSN") or os.getenv("DATABASE_URL") or ""

    def _connect(self):
        import psycopg2

        return psycopg2.connect(self.dsn)

    @contextmanager
    def transaction(self):
        conn = self._connect()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def fetch_df(self, query: str, params=None, connection=None):
        own_connection = connection is None
        conn = connection or self._connect()
        try:
            return pd.read_sql_query(query, conn, params=params)
        finally:
            if own_connection:
                conn.close()

    def bulk_insert_df(
        self,
        table_name: str,
        df,
        connection=None,
        page_size: int = 1000,
        on_conflict_column: str | None = None,
    ) -> int:
        if df.empty:
            return 0

        normalized_df = df.where(pd.notna(df), None)
        columns = list(normalized_df.columns)
        rows = list(normalized_df.itertuples(index=False, name=None))
        table_sql = self._quote_identifier_path(table_name)
        column_sql = ", ".join(self._quote_identifier(column) for column in columns)
        conflict_sql = ""
        if on_conflict_column:
            conflict_sql = f" on conflict ({self._quote_identifier(on_conflict_column)}) do nothing returning 1"

        query = f"""
            insert into {table_sql} ({column_sql})
            values %s
            {conflict_sql}
        """

        own_connection = connection is None
        conn = connection or self._connect()
        insert_count = len(rows)
        try:
            from psycopg2.extras import execute_values

            with conn.cursor() as cur:
                if on_conflict_column is not None:
                    insert_count = len(execute_values(cur, query, rows, page_size=page_size, fetch=True))
                else:
                    execute_values(cur, query, rows, page_size=page_size)
            if own_connection:
                conn.commit()
        except Exception:
            if own_connection:
                conn.rollback()
            raise
        finally:
            if own_connection:
                conn.close()

        return insert_count

    def bulk_insert_do_nothing_df(
        self,
        table_name: str,
        df,
        conflict_columns: list[str],
        connection=None,
        page_size: int = 1000,
    ) -> int:
        if df.empty:
            return 0

        normalized_df = df.where(pd.notna(df), None)
        columns = list(normalized_df.columns)
        rows = list(normalized_df.itertuples(index=False, name=None))
        table_sql = self._quote_identifier_path(table_name)
        column_sql = ", ".join(self._quote_identifier(column) for column in columns)
        conflict_sql = ", ".join(self._quote_identifier(column) for column in conflict_columns)
        query = f"""
            insert into {table_sql} ({column_sql})
            values %s
            on conflict ({conflict_sql}) do nothing
        """

        own_connection = connection is None
        conn = connection or self._connect()
        try:
            from psycopg2.extras import execute_values

            with conn.cursor() as cur:
                execute_values(cur, query, rows, page_size=page_size)
            if own_connection:
                conn.commit()
        except Exception:
            if own_connection:
                conn.rollback()
            raise
        finally:
            if own_connection:
                conn.close()

        return len(rows)

    def iter_query_chunks(self, query: str, params=None, chunk_size: int = 20000):
        if chunk_size <= 0:
            raise ValueError("chunk_size must be greater than 0")

        conn = self._connect()
        try:
            with conn.cursor(name="er_dose_stream") as cur:
                cur.itersize = chunk_size
                cur.execute(query, params)
                columns = [column.name for column in cur.description]
                while True:
                    rows = cur.fetchmany(chunk_size)
                    if not rows:
                        break
                    yield [dict(zip(columns, row, strict=False)) for row in rows]
        finally:
            conn.close()

    def execute(self, query: str, params=None, connection=None) -> int:
        own_connection = connection is None
        conn = connection or self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(query, params)
                rowcount = cur.rowcount
            if own_connection:
                conn.commit()
            return rowcount
        except Exception:
            if own_connection:
                conn.rollback()
            raise
        finally:
            if own_connection:
                conn.close()

    def _quote_identifier_path(self, identifier_path: str) -> str:
        return ".".join(self._quote_identifier(part) for part in identifier_path.split("."))

    def _quote_identifier(self, identifier: str) -> str:
        if not _IDENTIFIER_RE.match(identifier):
            raise ValueError(f"invalid SQL identifier: {identifier}")
        return f'"{identifier}"'
