#!/usr/bin/env python3
"""Build DWS and ADS snapshots in one strictly serial DAG."""

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
    dag_id="offline_dws_ads_daily",
    description="Serially build three DWS and three ADS snapshots",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2026, 1, 1, tz=SHANGHAI),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["offline", "dws", "ads"],
) as dag:
    dws_site_stats = BashOperator(
        task_id="dws_site_stats",
        bash_command=f'''set -euo pipefail
{SPARK_SUBMIT} --master yarn --deploy-mode client --driver-memory 1g --num-executors 2 --executor-cores 3 {OFFLINE_PROJECT_HOME}/spark/dws/dws_site_stats_job.py --ds {TARGET_DS}''',
    )
    dws_sku_trade = BashOperator(
        task_id="dws_sku_trade",
        bash_command=f'''set -euo pipefail
{SPARK_SUBMIT} --master yarn --deploy-mode client --driver-memory 1g --num-executors 2 --executor-cores 3 {OFFLINE_PROJECT_HOME}/spark/dws/dws_sku_trade_job.py --ds {TARGET_DS}''',
    )
    dws_shop_rating = BashOperator(
        task_id="dws_shop_rating",
        bash_command=f'''set -euo pipefail
{SPARK_SUBMIT} --master yarn --deploy-mode client --driver-memory 1g --num-executors 2 --executor-cores 3 {OFFLINE_PROJECT_HOME}/spark/dws/dws_shop_rating_job.py --ds {TARGET_DS}''',
    )
    ads_biz_overview = BashOperator(
        task_id="ads_biz_overview",
        bash_command=f'''set -euo pipefail
{SPARK_SUBMIT} --master yarn --deploy-mode client --driver-memory 1g --num-executors 2 --executor-cores 3 {OFFLINE_PROJECT_HOME}/spark/ads/ads_biz_overview_job.py --ds {TARGET_DS}''',
    )
    ads_sku_topn = BashOperator(
        task_id="ads_sku_topn",
        bash_command=f'''set -euo pipefail
{SPARK_SUBMIT} --master yarn --deploy-mode client --driver-memory 1g --num-executors 2 --executor-cores 3 {OFFLINE_PROJECT_HOME}/spark/ads/ads_sku_topn_job.py --ds {TARGET_DS}''',
    )
    ads_shop_rating = BashOperator(
        task_id="ads_shop_rating",
        bash_command=f'''set -euo pipefail
{SPARK_SUBMIT} --master yarn --deploy-mode client --driver-memory 1g --num-executors 2 --executor-cores 3 {OFFLINE_PROJECT_HOME}/spark/ads/ads_shop_rating_job.py --ds {TARGET_DS}''',
    )
    trigger_offline_publish_daily = TriggerDagRunOperator(
        task_id="trigger_offline_publish_daily",
        trigger_dag_id="offline_publish_daily",
        conf={"target_ds": TARGET_DS},
        wait_for_completion=False,
    )

    dws_site_stats >> dws_sku_trade >> dws_shop_rating >> ads_biz_overview >> ads_sku_topn >> ads_shop_rating >> trigger_offline_publish_daily
