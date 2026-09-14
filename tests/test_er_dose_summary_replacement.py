from datetime import date
from unittest.mock import Mock
import pytest
from er_dose.raw.raw_repository import ERDoseRepository
from er_dose.euv.euv_repository import ERDoseEUVRepository

@pytest.mark.parametrize("repository_type,kind", [(ERDoseRepository,"die_yield"),(ERDoseRepository,"root_cause"),(ERDoseEUVRepository,"root_cause")])
@pytest.mark.parametrize("failure", [None,"delete","insert"])
def test_summary_uses_separate_statements(repository_type, kind, failure):
    db = Mock()
    events = []
    def execute(query, params):
        action = query.strip().split()[0].lower()
        events.append(action)
        if action == failure:
            raise RuntimeError(action)
    db.execute.side_effect = execute
    repo = repository_type(db)
    method = getattr(repo, 'replace_' + kind + '_daily_summary')
    if failure:
        with pytest.raises(RuntimeError, match=failure):
            method(date(2026,5,1))
    else:
        assert method(date(2026,5,1)) is None
    assert events == (['delete'] if failure == 'delete' else ['delete','insert'])
    db.transaction.assert_not_called()
    for call in db.execute.call_args_list:
        assert 'connection' not in call.kwargs
        assert 'on conflict' not in call.args[0].lower()
