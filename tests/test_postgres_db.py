from __future__ import annotations

import unittest

import pandas as pd

from er_dose.infra.postgres_db import PostgresDB


class FakeCursor:
    def __init__(self):
        self.executed = []
        self.copy_query = None
        self.copy_payload = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def copy_expert(self, query, file):
        self.copy_query = query
        self.copy_payload = file.getvalue()


class FakeConnection:
    def __init__(self):
        self.cursor_obj = FakeCursor()
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


class PostgresDBTest(unittest.TestCase):
    pass


if __name__ == "__main__":
    unittest.main()
