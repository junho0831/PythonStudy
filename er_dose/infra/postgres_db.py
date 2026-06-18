from __future__ import annotations

import io
import os
import re
from contextlib import contextmanager
from io import StringIO

import pandas as pd


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class PostgresDB:
    def __init__(self, dsn: str | None = None):
        self.dsn = dsn or os.getenv("ER_DOSE_DB_DSN") or os.getenv("DATABASE_URL") or ""
        self.__engine = self  # Support user-provided copy_insert_to_partition_table
        self._sqlalchemy_engine = None

    def raw_connection(self):
        return self._connect_raw()

    def _connect_raw(self):
        import psycopg2

        return psycopg2.connect(self.dsn)

    def _connect_sqlalchemy(self):
        if self._sqlalchemy_engine is None:
            from sqlalchemy import create_engine

            self._sqlalchemy_engine = create_engine(self.dsn)
        return self._sqlalchemy_engine

    def copy_insert_to_partition_table(
        self,
        schema: str,
        table_name: str,
        target_date: str,
        df: pd.DataFrame,
        is_truncate: bool = False,
    ) -> None:
        if df is None or df.empty:
            print("insert 대상 데이터가 없습니다.")
            return

        partition_table = f'{schema}.{table_name}_1_prt_p{target_date.replace("-", "")}'
        query = f"COPY {partition_table} FROM STDIN WITH CSV HEADER"

        buffer = io.StringIO()
        insert_df = df.drop_duplicates()
        insert_df.to_csv(buffer, index=False)
        buffer.seek(0)

        conn = self.__engine.raw_connection()
        cursor = conn.cursor()

        try:
            if is_truncate:
                print(f"TRUNCATE TABLE {partition_table}")
                cursor.execute(f"TRUNCATE TABLE {partition_table}")

            cursor.copy_expert(query, buffer)

            print(f"{len(insert_df)} rows were saved.")

            cursor.execute(f"ANALYZE {partition_table}")
            conn.commit()

            print(f"data inserted into table {partition_table} successfully.")

        except Exception as e:
            conn.rollback()
            print(f"[ERROR] copy insert failed: {e}")
            raise

        finally:
            cursor.close()
            conn.close()

    @contextmanager
    def transaction(self):
        conn = self._connect_raw()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def select(self, query: str, params=None, connection=None):
        if connection is not None:
            return pd.read_sql_query(query, connection, params=params)

        chunks = list(self.select_in_chunks(query, params=params, chunk_size=100000))
        if not chunks:
            return pd.DataFrame()
        return pd.concat(chunks, ignore_index=True)

    def select_in_chunks(self, query: str, params=None, chunk_size: int = 10000, connection=None):
        if chunk_size <= 0:
            raise ValueError("chunk_size must be greater than 0")

        if connection is not None:
            yield from pd.read_sql_query(query, connection, params=params, chunksize=chunk_size)
            return

        from sqlalchemy import text

        with self._connect_sqlalchemy().connect() as conn:
            result = conn.execute(text(query), params or {})
            columns = list(result.keys())

            while True:
                rows = result.fetchmany(chunk_size)
                if not rows:
                    break
                yield pd.DataFrame(rows, columns=columns)

    def copy_insert_df(self, table_name: str, df, connection=None) -> int:
        if df.empty:
            return 0

        normalized_df = df.where(pd.notna(df), None)
        columns = list(normalized_df.columns)
        table_sql = self._quote_identifier_path(table_name)
        column_sql = ", ".join(self._quote_identifier(column) for column in columns)
        query = f"copy {table_sql} ({column_sql}) from stdin with csv null ''"

        stream = StringIO()
        for row in normalized_df.itertuples(index=False, name=None):
            serialized = []
            for value in row:
                if value is None or pd.isna(value):
                    serialized.append("")
                    continue
                text = str(value).replace('"', '""')
                serialized.append(f'"{text}"')
            stream.write(",".join(serialized))
            stream.write("\n")
        stream.seek(0)

        own_connection = connection is None
        conn = connection or self._connect_raw()
        try:
            with conn.cursor() as cur:
                cur.copy_expert(query, stream)
            if own_connection:
                conn.commit()
        except Exception:
            if own_connection:
                conn.rollback()
            raise
        finally:
            if own_connection:
                conn.close()

        return len(normalized_df)

    def execute(self, query: str, params=None, connection=None) -> int:
        own_connection = connection is None
        conn = connection or self._connect_raw()
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
