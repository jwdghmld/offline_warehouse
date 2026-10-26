#!/usr/bin/env python3
"""Generate one business day, then load its five MySQL facts into Hive ODS."""

import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


SHANGHAI = pendulum.timezone("Asia/Shanghai")
TARGET_DS = "{{ (dag_run.conf or {}).get('target_ds') or data_interval_end.in_timezone('Asia/Shanghai').subtract(days=1).strftime('%Y%m%d') }}"
SPARK_SUBMIT = os.environ.get("SPARK_SUBMIT", "此处自定义")
OFFLINE_PROJECT_HOME = os.environ.get("OFFLINE_PROJECT_HOME", "此处自定义").rstrip("/")
GENERATOR_PYTHON = os.environ.get("GENERATOR_PYTHON", "python3")
ALERT_EMAIL = os.environ.get("ALERT_EMAIL", "此处自定义")
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": [ALERT_EMAIL],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="offline_ods_daily",
    description="Generate daily facts, publish Kafka events, then load Hive ODS",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2026, 1, 1, tz=SHANGHAI),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["offline", "ods"],
) as dag:
    generate_daily_facts = BashOperator(
        task_id="generate_daily_facts",
        bash_command=f'''set -euo pipefail
{GENERATOR_PYTHON} {OFFLINE_PROJECT_HOME}/source-data-generator/2_generate_daily_facts.py --ds {TARGET_DS} --publish''',
    )
    ods_mysql_to_hive = BashOperator(
        task_id="ods_mysql_to_hive",
        bash_command=f'''set -euo pipefail
{SPARK_SUBMIT} --master yarn --deploy-mode client --driver-memory 1g --num-executors 2 --executor-cores 3 {OFFLINE_PROJECT_HOME}/spark/ods/ods_mysql_to_hive_job.py --ds {TARGET_DS}''',
    )

    trigger_offline_dwd_daily = TriggerDagRunOperator(
        task_id="trigger_offline_dwd_daily",
        trigger_dag_id="offline_dwd_daily",
        conf={"target_ds": TARGET_DS},
        wait_for_completion=False,
    )

    generate_daily_facts >> ods_mysql_to_hive >> trigger_offline_dwd_daily
