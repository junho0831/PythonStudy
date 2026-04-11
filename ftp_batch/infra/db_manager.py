from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path

import pandas as pd


class DBManager:
    def __init__(self, db_path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def _connect(self):
        return sqlite3.connect(self.db_path)

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

    def table_exists(self, table_name, connection=None):
        own_connection = connection is None
        conn = connection or self._connect()
        try:
            query = """
                select name
                from sqlite_master
                where type = 'table'
                  and name = ?
            """
            return conn.execute(query, (table_name,)).fetchone() is not None
        finally:
            if own_connection:
                conn.close()

    def fetch_df(self, query, params=None, connection=None):
        own_connection = connection is None
        conn = connection or self._connect()
        try:
            return pd.read_sql_query(query, conn, params=params)
        finally:
            if own_connection:
                conn.close()

    def bulk_insert_df(self, table_name, df, connection=None):
        if df.empty:
            return 0
        normalized_df = df.where(pd.notna(df), None)
        columns = list(normalized_df.columns)
        placeholders = ", ".join(["?"] * len(columns))
        column_names = ", ".join(columns)
        query = f"""
            insert into {table_name} ({column_names})
            values ({placeholders})
        """
        rows = list(normalized_df.itertuples(index=False, name=None))
        own_connection = connection is None
        conn = connection or self._connect()
        try:
            if not self.table_exists(table_name, connection=conn):
                raise ValueError(f"table does not exist: {table_name}")
            conn.executemany(query, rows)
            if own_connection:
                conn.commit()
        finally:
            if own_connection:
                conn.close()
        return len(rows)

    def execute(self, query, params=None, connection=None):
        own_connection = connection is None
        conn = connection or self._connect()
        try:
            cursor = conn.execute(query, params or ())
            if own_connection:
                conn.commit()
            return cursor.rowcount
        finally:
            if own_connection:
                conn.close()
