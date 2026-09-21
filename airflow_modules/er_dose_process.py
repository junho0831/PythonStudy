"""Each date has EUV and RAW tasks; RAW finishes all summaries."""
from hashlib import sha256

from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.providers.ssh.hooks.ssh import SSHHook

from airflow_modules.er_dose_dates import prepare_single_date
from airflow_modules.er_dose_jobs import PLAN_XCOM_KEY, plan_dates, report_day


SSH_CONN_ID = "er_dose_parser"
REPORTS = ("die_yield", "root_cause", "raw_count", "euv_count")


def progress_key(ti):
    identity = f"{ti.dag_id}:{ti.run_id}"
    return "er_dose_progress_" + sha256(identity.encode()).hexdigest()


def run_parser(command):
    hook = SSHHook(ssh_conn_id=SSH_CONN_ID, cmd_timeout=None)
    with hook.get_conn() as client:
        status, _, stderr = hook.exec_ssh_client_command(
            ssh_client=client, command=command, get_pty=True, environment=None, timeout=None,
        )
    if status != 0:
        raise AirflowException(f"Parser exited with status {status}: {stderr.decode(errors='replace')}")


def process_date(parser, **context):
    if parser not in ("euv", "raw"):
        raise ValueError("unknown parser")
    ti = context["ti"]
    target = prepare_single_date(**context)
    key = progress_key(ti)
    # Airflow clears this task's XCom on retry. A per-run Variable retains only
    # the frozen plan/commands and completed stages until RAW and reports succeed.
    progress = Variable.get(key, default_var=None, deserialize_json=True)
    if progress is None:
        if parser != "euv":
            raise ValueError("saved EUV plan is missing")
        plan_dates(ti, date_task_id=ti.task_id)
        progress = {
            "plan": ti.xcom_pull(task_ids=ti.task_id, key=PLAN_XCOM_KEY),
            "euv_command": ti.xcom_pull(task_ids=ti.task_id, key="ER_DOSE_EUV"),
            "raw_command": ti.xcom_pull(task_ids=ti.task_id, key="ER_DOSE_RAW"),
            "completed": [],
        }
        Variable.set(key, progress, serialize_json=True)
    plan = progress["plan"]
    if plan[0]["target_date"] != target:
        raise ValueError("saved progress does not match this run's target_date")
    ti.xcom_push(key=PLAN_XCOM_KEY, value=plan)
    ti.xcom_push(key="ER_DOSE_" + parser.upper(), value=progress[parser + "_command"])
    if plan[0][parser] and parser not in progress["completed"]:
        ti.log.info("Processing %s: %s", target, parser)
        run_parser(progress[parser + "_command"])
        progress["completed"].append(parser)
        Variable.set(key, progress, serialize_json=True)
    if parser == "euv":
        return
    if plan[0]["euv"] or plan[0]["raw"]:
        for job in REPORTS:
            if job not in progress["completed"]:
                ti.log.info("Processing %s: %s", plan[0]["target_date"], job)
                report_day(job, 0, ti, plan_task_id=ti.task_id)
                progress["completed"].append(job)
                Variable.set(key, progress, serialize_json=True)
    Variable.delete(key)
