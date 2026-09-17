from datetime import datetime

import pytest

from er_dose.raw.raw_repository import ERDoseRepository


@pytest.mark.parametrize("repository_type,method,table", [
    (ERDoseRepository, "die_yield_daily_summary", "de_trend_die_yield_daily"),
    (ERDoseRepository, "root_cause_daily_summary", "de_trend_root_cause_daily"),
])
@pytest.mark.parametrize("fail_insert", [False, True])
def test_summary_delete_and_insert_are_separate_calls(repository_type, method, table, fail_insert):
    class DB:
        def __init__(self):
            self.connection = object()
            self.events = []
            self.calls = []

        def transaction(self):
            raise AssertionError("summary must not create a shared transaction")

        def execute(self, query, params):
            self.calls.append((query, params))
            action = query.strip().split()[0].lower()
            self.events.append(action)
            if action == "insert" and fail_insert:
                raise RuntimeError("insert failed")
            return None

    db = DB()
    repo = repository_type(db)
    start, end = datetime(2026, 5, 1), datetime(2026, 5, 2)
    assert getattr(repo, "delete_" + method)(start, end) is None
    if fail_insert:
        with pytest.raises(RuntimeError, match="insert failed"):
            getattr(repo, "insert_" + method)(start, end)
    else:
        assert getattr(repo, "insert_" + method)(start, end) is None
    expected = ["delete", "insert"]
    assert db.events == expected
    delete_query, params = db.calls[0]
    assert table in delete_query
    assert "where occur_date >= cast(:start_time as date)" in delete_query
    assert params == {"start_time": datetime(2026, 5, 1), "end_time": datetime(2026, 5, 2)}
    assert "on conflict" not in db.calls[1][0].lower()
