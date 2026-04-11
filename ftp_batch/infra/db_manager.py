from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd


class DBManager:
    def __init__(self, db_path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def _connect(self):
        return sqlite3.connect(self.db_path)

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

    def fetch_df(self, query, params=None):
        with self._connect() as conn:
            return pd.read_sql_query(query, conn, params=params)

    def bulk_insert_df(self, table_name, df):
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
        with self._connect() as conn:
            if not self.table_exists(table_name, connection=conn):
                raise ValueError(f"table does not exist: {table_name}")
            conn.executemany(query, rows)
            conn.commit()
        return len(rows)

    def execute(self, query, params=None):
        with self._connect() as conn:
            conn.execute(query, params or ())
            conn.commit()
