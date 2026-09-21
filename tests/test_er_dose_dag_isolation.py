"""Verify the deployment bundle contains no dependency on the parser project."""
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

pytest.importorskip("airflow")
pytest.importorskip("airflow.providers.ssh")
pytest.importorskip("airflow.providers.postgres")


def test_dag_loads_without_parser_project(tmp_path):
    root = Path(__file__).resolve().parents[1]
    package = tmp_path / "airflow_modules"
    package.mkdir()
    for name in ("__init__.py", "er_dose_jobs.py", "er_dose_repository.py", "er_dose_db.py", "er_dose_dates.py", "er_dose_process.py"):
        shutil.copy2(root / "airflow_modules" / name, package / name)
    shutil.copy2(root / "dags" / "er_dose_daily_dag.py", tmp_path / "er_dose_daily_dag.py")
    script = """
import importlib.util
import sys
sys.path.insert(0, sys.argv[1])
assert importlib.util.find_spec('er_dose') is None
from airflow_modules.er_dose_db import PostgresDB
from airflow.providers.postgres.hooks.postgres import PostgresHook
from unittest.mock import patch
with patch.object(PostgresHook, 'get_conn', return_value='configured-connection') as connect:
    assert PostgresDB()._connect_raw() == 'configured-connection'
    connect.assert_called_once()
from airflow.models import DagBag
bag = DagBag(dag_folder=sys.argv[1] + '/er_dose_daily_dag.py', include_examples=False)
assert not bag.import_errors, bag.import_errors
assert len(bag.dags['er_dose_daily'].tasks) == 4
assert set(bag.dags) == {'er_dose_daily'}
assert not any(name == 'er_dose' or name.startswith('er_dose.') for name in sys.modules)
"""
    result = subprocess.run(
        [sys.executable, "-I", "-c", script, str(tmp_path)],
        cwd=tmp_path, capture_output=True, text=True, timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr
