"""A run's logical date is its data date; freeze commands in start's XCom."""
from datetime import date


DATES_TASK_ID = 'start'


def prepare_run(**context):
    from airflow.models import Variable

    logical_date = context['logical_date']
    if logical_date.utcoffset().total_seconds() != 0 or any((
        logical_date.hour, logical_date.minute, logical_date.second, logical_date.microsecond,
    )):
        raise ValueError('logical_date must be the data date at 00:00:00 UTC; use er_dose_runs')
    target = logical_date.date().isoformat()
    conf = getattr(context.get('dag_run'), 'conf', None) or {}
    if conf.get('target_date', target) != target or 'end_date' in conf:
        raise ValueError('one run must use exactly its logical date')
    commands = conf.get('commands')
    if commands is None:
        commands = {
            'ER_DOSE_EUV': Variable.get('ER_DOSE_EUV_COMMAND'),
            'ER_DOSE_RAW': Variable.get('ER_DOSE_RAW_COMMAND'),
        }
    ti = context['ti']
    ti.xcom_push(key='date', value=target)
    ti.xcom_push(key='ER_DOSE_COMMANDS', value=commands)


def prepare_single_date(**context):
    ti = context['ti']
    target = ti.xcom_pull(task_ids=DATES_TASK_ID, key='date')
    commands = ti.xcom_pull(task_ids=DATES_TASK_ID, key='ER_DOSE_COMMANDS')
    if target is None or commands is None:
        raise ValueError('saved date or commands are missing')
    if not isinstance(target, str) or date.fromisoformat(target).isoformat() != target:
        raise ValueError('target_date must be YYYY-MM-DD')
    ti.xcom_push(key='date', value=target)
    ti.xcom_push(key='ER_DOSE_EUV', value=commands['ER_DOSE_EUV'].replace('{target_date}', target))
    ti.xcom_push(key='ER_DOSE_RAW', value=commands['ER_DOSE_RAW'].replace('{target_date}', target))
    return target
