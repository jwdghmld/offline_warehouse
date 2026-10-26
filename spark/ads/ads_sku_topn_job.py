# coding: utf-8
#!/usr/bin/python3

import os

os.environ["PYSPARK_PYTHON"] = os.environ["SPARK_PYTHON"]
os.environ["PYSPARK_DRIVER_PYTHON"] = os.environ["SPARK_PYTHON"]

import argparse
from datetime import datetime

from pyspark import StorageLevel
from pyspark.sql import SparkSession


TOP_N = 10
SHUFFLE_PARTITIONS = os.environ.get("SPARK_SQL_SHUFFLE_PARTITIONS", "12")
OUTPUT_PARTITIONS = int(os.environ.get("SPARK_OUTPUT_PARTITIONS", "3"))


class DataQualityError(RuntimeError):
    pass


def parse_args():
    '''
    作用：读取并校验展示截止日期。
    输入：命令行中的 --ds，格式为 yyyyMMdd。
    输出：包含 ds 的 argparse.Namespace。
    '''
    parser = argparse.ArgumentParser(description="构建 SKU TopN 展示快照")
    parser.add_argument("--ds", required=True, help="展示截止日期，格式 yyyyMMdd")
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
        SparkSession.builder.appName("ads_sku_topn")
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
    作用：检查 SKU 交易 DWS 分区是否存在。
    输入：SparkSession、表名 table、展示日期 ds。
    输出：无；分区不存在时抛出 DataQualityError。
    '''
    if not spark.sql(f"show partitions {table} partition (ds='{ds}')").take(1):
        raise DataQualityError(f"缺少分区：{table} ds={ds}")


def assert_result(dataframe):
    '''
    作用：检查排行榜主键、类型和名次范围。
    输入：SKU TopN DataFrame。
    输出：无；检查失败时抛出 DataQualityError。
    '''
    if dataframe.groupBy(
        "stat_period", "rank_type", "rank_no"
    ).count().where("count > 1").take(1):
        raise DataQualityError("SKU TopN 存在重复名次")
    if dataframe.where(
        f"rank_type not in ('gmv', 'paid_sku_num', 'paid_order_count') "
        f"or rank_no < 1 or rank_no > {TOP_N}"
    ).take(1):
        raise DataQualityError("SKU TopN 存在非法排行类型或名次")


def write_partition(spark, dataframe, ds):
    '''
    作用：覆盖 SKU TopN ADS 的目标分区。
    输入：SparkSession、排行榜 DataFrame、展示日期 ds。
    输出：无；写入 ads.ads_sku_topn_df 的 ds 分区。
    '''
    cached = dataframe.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        cached.coalesce(OUTPUT_PARTITIONS).createOrReplaceTempView(
            "ads_sku_topn_result"
        )
        spark.sql(f"""
            -- 覆盖目标展示分区
            insert overwrite table ads.ads_sku_topn_df partition (ds='{ds}')
            select
              stat_period, rank_type, rank_no, sku_id, sku_name,
              category_id, category_name, shop_id, shop_name,
              pv_count, visitor_count, cart_user_count,
              paid_order_count, paid_user_count, paid_sku_num, gmv
            from ads_sku_topn_result
        """)
        print(f"写入 ads.ads_sku_topn_df ds={ds} rows={cached.count()}")
    finally:
        spark.catalog.dropTempView("ads_sku_topn_result")
        cached.unpersist()


def build_topn(spark, ds):
    '''
    作用：按 GMV、支付件数和支付订单数生成每周期前十名。
    输入：SparkSession、展示截止日期 ds。
    输出：无；写入 SKU TopN ADS 的目标分区。
    '''
    assert_partition_exists(spark, "cdm.dws_sku_trade_df", ds)
    result = spark.sql(f"""
        -- 同值时按 sku_id 升序，保证重跑名次稳定
        with ranked as (
          select
            stat_period,
            'gmv' as rank_type,
            row_number() over (
              partition by stat_period order by gmv desc, sku_id asc
            ) as rank_no,
            sku_id, sku_name, category_id, category_name, shop_id, shop_name,
            pv_count, visitor_count, cart_user_count,
            paid_order_count, paid_user_count, paid_sku_num, gmv
          from cdm.dws_sku_trade_df
          where ds='{ds}'
          union all
          select
            stat_period,
            'paid_sku_num' as rank_type,
            row_number() over (
              partition by stat_period order by paid_sku_num desc, sku_id asc
            ) as rank_no,
            sku_id, sku_name, category_id, category_name, shop_id, shop_name,
            pv_count, visitor_count, cart_user_count,
            paid_order_count, paid_user_count, paid_sku_num, gmv
          from cdm.dws_sku_trade_df
          where ds='{ds}'
          union all
          select
            stat_period,
            'paid_order_count' as rank_type,
            row_number() over (
              partition by stat_period order by paid_order_count desc, sku_id asc
            ) as rank_no,
            sku_id, sku_name, category_id, category_name, shop_id, shop_name,
            pv_count, visitor_count, cart_user_count,
            paid_order_count, paid_user_count, paid_sku_num, gmv
          from cdm.dws_sku_trade_df
          where ds='{ds}'
        )
        select
          stat_period, rank_type, cast(rank_no as int) as rank_no,
          sku_id, sku_name, category_id, category_name, shop_id, shop_name,
          pv_count, visitor_count, cart_user_count,
          paid_order_count, paid_user_count, paid_sku_num, gmv
        from ranked
        where rank_no <= {TOP_N}
    """)
    assert_result(result)
    write_partition(spark, result, ds)


def main():
    args = parse_args()
    spark = create_spark_session()
    try:
        build_topn(spark, args.ds)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

