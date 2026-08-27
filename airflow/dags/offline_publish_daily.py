#!/usr/bin/env python3
"""Publish the three ADS snapshots to the offline MySQL result database."""

import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator


SHANGHAI = pendulum.timezone("Asia/Shanghai")
TARGET_DS = "{{ (dag_run.conf or {}).get('target_ds') or data_interval_end.in_timezone('Asia/Shanghai').subtract(days=1).strftime('%Y%m%d') }}"
SPARK_SUBMIT = os.environ.get("SPARK_SUBMIT", "此处自定义")
OFFLINE_PROJECT_HOME = os.environ.get("OFFLINE_PROJECT_HOME", "此处自定义").rstrip("/")
MAIL_TO = os.environ.get("ALERT_EMAIL", "此处自定义")
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": [MAIL_TO],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="offline_publish_daily",
    description="Serially publish three ADS tables and notify on success",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2026, 1, 1, tz=SHANGHAI),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["offline", "publish"],
) as dag:
    publish_biz_overview = BashOperator(
        task_id="publish_biz_overview",
        bash_command=f'''set -euo pipefail
{SPARK_SUBMIT} --master yarn --deploy-mode client --driver-memory 1g --num-executors 2 --executor-cores 3 {OFFLINE_PROJECT_HOME}/spark/ads/publish_ads_biz_overview_job.py --ds {TARGET_DS}''',
    )
    publish_sku_topn = BashOperator(
        task_id="publish_sku_topn",
        bash_command=f'''set -euo pipefail
{SPARK_SUBMIT} --master yarn --deploy-mode client --driver-memory 1g --num-executors 2 --executor-cores 3 {OFFLINE_PROJECT_HOME}/spark/ads/publish_ads_sku_topn_job.py --ds {TARGET_DS}''',
    )
    publish_shop_rating = BashOperator(
        task_id="publish_shop_rating",
        bash_command=f'''set -euo pipefail
{SPARK_SUBMIT} --master yarn --deploy-mode client --driver-memory 1g --num-executors 2 --executor-cores 3 {OFFLINE_PROJECT_HOME}/spark/ads/publish_ads_shop_rating_job.py --ds {TARGET_DS}''',
    )
    success_email = EmailOperator(
        task_id="success_email",
        to=MAIL_TO,
        subject=f"[Airflow] 离线数仓发布成功 {TARGET_DS}",
        html_content="<p>离线经营总览、SKU 排行和店铺评分已完成发布。</p>",
    )

    publish_biz_overview >> publish_sku_topn >> publish_shop_rating >> success_email
