# coding: utf-8
#!/usr/bin/python3

import os

os.environ["PYSPARK_PYTHON"] = os.environ["SPARK_PYTHON"]
os.environ["PYSPARK_DRIVER_PYTHON"] = os.environ["SPARK_PYTHON"]

import argparse
from datetime import datetime

from pyspark import StorageLevel
from pyspark.sql import SparkSession


SHUFFLE_PARTITIONS = os.environ.get("SPARK_SQL_SHUFFLE_PARTITIONS", "12")
OUTPUT_PARTITIONS = int(os.environ.get("SPARK_OUTPUT_PARTITIONS", "3"))


class DataQualityError(RuntimeError):
    pass


def parse_args():
    '''
    作用：读取并校验统计截止日期。
    输入：命令行中的 --ds，格式为 yyyyMMdd。
    输出：包含 ds 的 argparse.Namespace。
    '''
    parser = argparse.ArgumentParser(description="构建全站经营统计快照")
    parser.add_argument("--ds", required=True, help="统计截止日期，格式 yyyyMMdd")
    args = parser.parse_args()
    try:
        parsed = datetime.strptime(args.ds, "%Y%m%d")
    except ValueError as exc:
        raise ValueError("--ds 必须是有效的 yyyyMMdd 日期") from exc
    if parsed.strftime("%Y%m%d") != args.ds:
        raise ValueError("--ds 必须是 8 位日期")
    return args


def create_spark_session():
    '''
    作用：创建启用 Hive 支持的 Spark 会话。
    输入：无。
    输出：配置完成的 SparkSession。
    '''
    return (
        SparkSession.builder.appName("dws_site_stats")
        .config("spark.dynamicAllocation.enabled", "false")
        .config("spark.default.parallelism", "12")
        .config("spark.sql.session.timeZone", "Asia/Shanghai")
        .config("spark.sql.sources.partitionOverwriteMode", "static")
        .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
        .config("spark.sql.orc.compression.codec", "snappy")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64m")
        .config("spark.sql.autoBroadcastJoinThreshold", "32m")
        .config("spark.sql.broadcastTimeout", "300")
        .config("spark.driver.maxResultSize", "512m")
        .enableHiveSupport()
        .getOrCreate()
    )


def assert_partition_exists(spark, table, ds):
    '''
    作用：检查上游快照或当日 ODS 分区是否存在。
    输入：SparkSession、表名 table、统计日期 ds。
    输出：无；分区不存在时抛出 DataQualityError。
    '''
    if not spark.sql(f"show partitions {table} partition (ds='{ds}')").take(1):
        raise DataQualityError(f"缺少分区：{table} ds={ds}")


def assert_result(dataframe):
    '''
    作用：检查全站统计是否完整包含四种统计范围。
    输入：全站统计 DataFrame。
    输出：无；检查失败时抛出 DataQualityError。
    '''
    periods = {row[0] for row in dataframe.select("stat_period").collect()}
    if periods != {"1d", "7d", "30d", "all"} or dataframe.count() != 4:
        raise DataQualityError(f"全站统计范围不完整：{periods}")
    if dataframe.where(
        "pv_count < 0 or uv_count < 0 or favorite_count < 0 or cart_count < 0 "
        "or order_count < 0 or paid_order_count < 0 or paid_user_count < 0 "
        "or paid_sku_num < 0 or gmv < 0"
    ).take(1):
        raise DataQualityError("全站统计存在负数指标")


def write_partition(spark, dataframe, ds):
    '''
    作用：覆盖全站统计 DWS 的目标分区。
    输入：SparkSession、全站统计 DataFrame、统计日期 ds。
    输出：无；写入 cdm.dws_site_stats_df 的 ds 分区。
    '''
    cached = dataframe.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        cached.coalesce(OUTPUT_PARTITIONS).createOrReplaceTempView(
            "dws_site_stats_result"
        )
        spark.sql(f"""
            -- 覆盖目标统计分区
            insert overwrite table cdm.dws_site_stats_df partition (ds='{ds}')
            select
              stat_period, pv_count, uv_count, favorite_count, cart_count,
              order_count, paid_order_count, paid_user_count, paid_sku_num, gmv
            from dws_site_stats_result
        """)
        print(f"写入 cdm.dws_site_stats_df ds={ds} rows={cached.count()}")
    finally:
        spark.catalog.dropTempView("dws_site_stats_result")
        cached.unpersist()


def build_stats(spark, ds):
    '''
    作用：从同一 DWD 快照计算 1d、7d、30d 和 all 全站指标。
    输入：SparkSession、统计截止日期 ds。
    输出：无；写入全站统计 DWS 的目标分区。
    '''
    for table in (
        "cdm.dwd_user_behavior_df",
        "cdm.dwd_trd_pay_dtl_df",
        "ods.ods_order_info_di",
    ):
        assert_partition_exists(spark, table, ds)

    result = spark.sql(f"""
        -- 四种统计范围共用同一个截止日期
        with periods as (
          select stack(
            4,
            '1d', date_sub(to_date('{ds}', 'yyyyMMdd'), 0),
            '7d', date_sub(to_date('{ds}', 'yyyyMMdd'), 6),
            '30d', date_sub(to_date('{ds}', 'yyyyMMdd'), 29),
            'all', cast('1900-01-01' as date)
          ) as (stat_period, start_date)
        ), traffic as (
          select
            periods.stat_period,
            cast(count(behavior.event_id) as bigint) as pv_count,
            cast(count(distinct case when event_type='view' then user_id end) as bigint)
              as uv_count,
            cast(sum(case when event_type='favorite' then 1 else 0 end) as bigint)
              as favorite_count,
            cast(sum(case when event_type='cart' then 1 else 0 end) as bigint)
              as cart_count
          from periods
          left join cdm.dwd_user_behavior_df behavior
            on behavior.ds='{ds}'
           and behavior.event_date between periods.start_date and to_date('{ds}', 'yyyyMMdd')
          group by periods.stat_period
        ), order_ranked as (
          select
            order_id, create_time,
            row_number() over (
              partition by order_id order by update_time desc, ds desc
            ) as row_num
          from ods.ods_order_info_di
          where ds <= '{ds}'
        ), created_order as (
          select order_id, to_date(create_time) as order_date
          from order_ranked
          where row_num=1
        ), order_metrics as (
          select
            periods.stat_period,
            cast(count(distinct created.order_id) as bigint) as order_count
          from periods
          left join created_order created
            on created.order_date between periods.start_date and to_date('{ds}', 'yyyyMMdd')
          group by periods.stat_period
        ), paid_order as (
          select
            order_id,
            max(user_id) as user_id,
            max(payment_amount) as payment_amount,
            max(payment_date) as payment_date
          from cdm.dwd_trd_pay_dtl_df
          where ds='{ds}'
          group by order_id
        ), paid_order_metrics as (
          select
            periods.stat_period,
            cast(count(distinct paid.order_id) as bigint) as paid_order_count,
            cast(count(distinct paid.user_id) as bigint) as paid_user_count,
            cast(coalesce(sum(paid.payment_amount), 0) as decimal(20,2)) as gmv
          from periods
          left join paid_order paid
            on paid.payment_date between periods.start_date and to_date('{ds}', 'yyyyMMdd')
          group by periods.stat_period
        ), paid_detail_metrics as (
          select
            periods.stat_period,
            cast(coalesce(sum(detail.sku_num), 0) as bigint) as paid_sku_num
          from periods
          left join cdm.dwd_trd_pay_dtl_df detail
            on detail.ds='{ds}'
           and detail.payment_date between periods.start_date and to_date('{ds}', 'yyyyMMdd')
          group by periods.stat_period
        )
        select
          periods.stat_period,
          cast(coalesce(traffic.pv_count, 0) as bigint) as pv_count,
          cast(coalesce(traffic.uv_count, 0) as bigint) as uv_count,
          cast(coalesce(traffic.favorite_count, 0) as bigint) as favorite_count,
          cast(coalesce(traffic.cart_count, 0) as bigint) as cart_count,
          cast(coalesce(orders.order_count, 0) as bigint) as order_count,
          cast(coalesce(paid.paid_order_count, 0) as bigint) as paid_order_count,
          cast(coalesce(paid.paid_user_count, 0) as bigint) as paid_user_count,
          cast(coalesce(detail.paid_sku_num, 0) as bigint) as paid_sku_num,
          cast(coalesce(paid.gmv, 0) as decimal(20,2)) as gmv
        from periods
        left join traffic on periods.stat_period=traffic.stat_period
        left join order_metrics orders on periods.stat_period=orders.stat_period
        left join paid_order_metrics paid on periods.stat_period=paid.stat_period
        left join paid_detail_metrics detail on periods.stat_period=detail.stat_period
    """)
    assert_result(result)
    write_partition(spark, result, ds)


def main():
    args = parse_args()
    spark = create_spark_session()
    try:
        build_stats(spark, args.ds)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

