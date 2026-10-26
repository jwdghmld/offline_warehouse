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
    parser = argparse.ArgumentParser(description="构建 SKU 交易统计快照")
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
        SparkSession.builder.appName("dws_sku_trade")
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
    作用：检查行为和交易 DWD 快照是否存在。
    输入：SparkSession、表名 table、统计日期 ds。
    输出：无；分区不存在时抛出 DataQualityError。
    '''
    if not spark.sql(f"show partitions {table} partition (ds='{ds}')").take(1):
        raise DataQualityError(f"缺少分区：{table} ds={ds}")


def assert_result(dataframe):
    '''
    作用：检查 SKU 统计主键和非负指标。
    输入：SKU 交易统计 DataFrame。
    输出：无；检查失败时抛出 DataQualityError。
    '''
    if dataframe.groupBy("stat_period", "sku_id").count().where("count > 1").take(1):
        raise DataQualityError("SKU 统计存在重复主键")
    if dataframe.where(
        "pv_count < 0 or visitor_count < 0 or cart_user_count < 0 "
        "or paid_order_count < 0 or paid_user_count < 0 "
        "or paid_sku_num < 0 or gmv < 0"
    ).take(1):
        raise DataQualityError("SKU 统计存在负数指标")


def write_partition(spark, dataframe, ds):
    '''
    作用：覆盖 SKU 交易 DWS 的目标分区。
    输入：SparkSession、SKU 统计 DataFrame、统计日期 ds。
    输出：无；写入 cdm.dws_sku_trade_df 的 ds 分区。
    '''
    cached = dataframe.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        cached.coalesce(OUTPUT_PARTITIONS).createOrReplaceTempView(
            "dws_sku_trade_result"
        )
        spark.sql(f"""
            -- 覆盖目标统计分区
            insert overwrite table cdm.dws_sku_trade_df partition (ds='{ds}')
            select
              stat_period, sku_id, sku_name, category_id, category_name,
              shop_id, shop_name, pv_count, visitor_count, cart_user_count,
              paid_order_count, paid_user_count, paid_sku_num, gmv
            from dws_sku_trade_result
        """)
        print(f"写入 cdm.dws_sku_trade_df ds={ds} rows={cached.count()}")
    finally:
        spark.catalog.dropTempView("dws_sku_trade_result")
        cached.unpersist()


def build_stats(spark, ds):
    '''
    作用：按 SKU 计算四种时间范围的流量和交易指标。
    输入：SparkSession、统计截止日期 ds。
    输出：无；写入 SKU 交易 DWS 的目标分区。
    '''
    for table in ("cdm.dwd_user_behavior_df", "cdm.dwd_trd_pay_dtl_df"):
        assert_partition_exists(spark, table, ds)
    result = spark.sql(f"""
        -- 流量和交易分别聚合后按统计范围与 SKU 合并
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
            behavior.sku_id,
            max(behavior.sku_name) as sku_name,
            max(behavior.category_id) as category_id,
            max(behavior.category_name) as category_name,
            max(behavior.shop_id) as shop_id,
            max(behavior.shop_name) as shop_name,
            cast(sum(case when behavior.event_type='view' then 1 else 0 end) as bigint)
              as pv_count,
            cast(count(distinct case when behavior.event_type='view' then behavior.user_id end)
              as bigint) as visitor_count,
            cast(count(distinct case when behavior.event_type='cart' then behavior.user_id end)
              as bigint) as cart_user_count
          from periods
          inner join cdm.dwd_user_behavior_df behavior
            on behavior.ds='{ds}'
           and behavior.event_date between periods.start_date and to_date('{ds}', 'yyyyMMdd')
          group by periods.stat_period, behavior.sku_id
        ), trade as (
          select
            periods.stat_period,
            detail.sku_id,
            max(detail.sku_name) as sku_name,
            max(detail.category_id) as category_id,
            max(detail.category_name) as category_name,
            max(detail.shop_id) as shop_id,
            max(detail.shop_name) as shop_name,
            cast(count(distinct detail.order_id) as bigint) as paid_order_count,
            cast(count(distinct detail.user_id) as bigint) as paid_user_count,
            cast(sum(detail.sku_num) as bigint) as paid_sku_num,
            cast(sum(detail.final_amount) as decimal(20,2)) as gmv
          from periods
          inner join cdm.dwd_trd_pay_dtl_df detail
            on detail.ds='{ds}'
           and detail.payment_date between periods.start_date and to_date('{ds}', 'yyyyMMdd')
          group by periods.stat_period, detail.sku_id
        )
        select
          coalesce(traffic.stat_period, trade.stat_period) as stat_period,
          coalesce(traffic.sku_id, trade.sku_id) as sku_id,
          coalesce(traffic.sku_name, trade.sku_name) as sku_name,
          coalesce(traffic.category_id, trade.category_id) as category_id,
          coalesce(traffic.category_name, trade.category_name) as category_name,
          coalesce(traffic.shop_id, trade.shop_id) as shop_id,
          coalesce(traffic.shop_name, trade.shop_name) as shop_name,
          cast(coalesce(traffic.pv_count, 0) as bigint) as pv_count,
          cast(coalesce(traffic.visitor_count, 0) as bigint) as visitor_count,
          cast(coalesce(traffic.cart_user_count, 0) as bigint) as cart_user_count,
          cast(coalesce(trade.paid_order_count, 0) as bigint) as paid_order_count,
          cast(coalesce(trade.paid_user_count, 0) as bigint) as paid_user_count,
          cast(coalesce(trade.paid_sku_num, 0) as bigint) as paid_sku_num,
          cast(coalesce(trade.gmv, 0) as decimal(20,2)) as gmv
        from traffic
        full outer join trade
          on traffic.stat_period=trade.stat_period and traffic.sku_id=trade.sku_id
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

