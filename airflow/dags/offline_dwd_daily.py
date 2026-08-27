#!/usr/bin/env python3
"""Build the three DWD snapshots for one business-date cutoff."""

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
    dag_id="offline_dwd_daily",
    description="Serially build the three DWD historical snapshots",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2026, 1, 1, tz=SHANGHAI),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["offline", "dwd"],
) as dag:
    dwd_trd_pay_dtl_snapshot = BashOperator(
        task_id="dwd_trd_pay_dtl_snapshot",
        bash_command=f'''set -euo pipefail
{SPARK_SUBMIT} --master yarn --deploy-mode client --driver-memory 1g --num-executors 2 --executor-cores 3 {OFFLINE_PROJECT_HOME}/spark/dwd/dwd_trd_pay_dtl_snapshot_job.py --ds {TARGET_DS}''',
    )
    dwd_user_behavior_snapshot = BashOperator(
        task_id="dwd_user_behavior_snapshot",
        bash_command=f'''set -euo pipefail
{SPARK_SUBMIT} --master yarn --deploy-mode client --driver-memory 1g --num-executors 2 --executor-cores 3 {OFFLINE_PROJECT_HOME}/spark/dwd/dwd_user_behavior_snapshot_job.py --ds {TARGET_DS}''',
    )
    dwd_shop_rating_snapshot = BashOperator(
        task_id="dwd_shop_rating_snapshot",
        bash_command=f'''set -euo pipefail
{SPARK_SUBMIT} --master yarn --deploy-mode client --driver-memory 1g --num-executors 2 --executor-cores 3 {OFFLINE_PROJECT_HOME}/spark/dwd/dwd_shop_rating_snapshot_job.py --ds {TARGET_DS}''',
    )
   
    trigger_offline_dws_ads_daily = TriggerDagRunOperator(
        task_id="trigger_offline_dws_ads_daily",
        trigger_dag_id="offline_dws_ads_daily",
        conf={"target_ds": TARGET_DS},
        wait_for_completion=False,
    )

    dwd_trd_pay_dtl_snapshot >> dwd_user_behavior_snapshot >> dwd_shop_rating_snapshot >> trigger_offline_dws_ads_daily
