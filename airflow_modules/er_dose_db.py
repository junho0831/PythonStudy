"""Database access owned by the DAG project; no parser configuration imports."""
from __future__ import annotations

from contextlib import closing

import pandas as pd
from sqlalchemy import text
from sqlalchemy.dialects.postgresql.psycopg2 import dialect


POSTGRES_CONN_ID = "er_dose_db"


class PostgresDB:
    def _connect_raw(self):
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        return PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()

    @staticmethod
    def _bind(query, params):
        compiled = text(query).compile(dialect=dialect())
        return str(compiled), compiled.construct_params(params or {})

    def select(self, query, params=None):
        query, params = self._bind(query, params)
        with closing(self._connect_raw()) as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return pd.DataFrame(cur.fetchall(), columns=[column[0] for column in cur.description])

    def execute(self, query, params=None):
        query, params = self._bind(query, params)
        with closing(self._connect_raw()) as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(query, params)
                    count = cur.rowcount
                conn.commit()
                return count
            except Exception:
                conn.rollback()
                raise

    def bulk_insert_df(self, table_name, df):
        if df.empty:
            return 0

        from psycopg2 import sql
        from psycopg2.extras import execute_values

        query = sql.SQL("insert into {} ({}) values %s").format(
            sql.Identifier(*table_name.split(".")),
            sql.SQL(", ").join(sql.Identifier(column) for column in df.columns),
        )
        normalized_df = df.astype(object).where(pd.notna(df), None)
        rows = list(normalized_df.itertuples(index=False, name=None))
        with closing(self._connect_raw()) as conn:
            try:
                with conn.cursor() as cur:
                    execute_values(cur, query, rows)
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return len(rows)
