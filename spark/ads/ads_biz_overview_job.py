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
    作用：读取并校验展示截止日期。
    输入：命令行中的 --ds，格式为 yyyyMMdd。
    输出：包含 ds 的 argparse.Namespace。
    '''
    parser = argparse.ArgumentParser(description="构建经营总览展示快照")
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
        SparkSession.builder.appName("ads_biz_overview")
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
    作用：检查全站统计 DWS 分区是否存在。
    输入：SparkSession、表名 table、展示日期 ds。
    输出：无；分区不存在时抛出 DataQualityError。
    '''
    if not spark.sql(f"show partitions {table} partition (ds='{ds}')").take(1):
        raise DataQualityError(f"缺少分区：{table} ds={ds}")


def assert_result(dataframe):
    '''
    作用：检查经营总览是否包含四种统计范围且比例非负。
    输入：经营总览 DataFrame。
    输出：无；检查失败时抛出 DataQualityError。
    '''
    periods = {row[0] for row in dataframe.select("stat_period").collect()}
    if periods != {"1d", "7d", "30d", "all"} or dataframe.count() != 4:
        raise DataQualityError(f"经营总览统计范围不完整：{periods}")
    if dataframe.where("avg_order_amount < 0 or view_to_pay_rate < 0").take(1):
        raise DataQualityError("经营总览存在负数金额或比例")


def write_partition(spark, dataframe, ds):
    '''
    作用：覆盖经营总览 ADS 的目标分区。
    输入：SparkSession、经营总览 DataFrame、展示日期 ds。
    输出：无；写入 ads.ads_biz_overview_df 的 ds 分区。
    '''
    cached = dataframe.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        cached.coalesce(OUTPUT_PARTITIONS).createOrReplaceTempView(
            "ads_biz_overview_result"
        )
        spark.sql(f"""
            -- 覆盖目标展示分区
            insert overwrite table ads.ads_biz_overview_df partition (ds='{ds}')
            select
              stat_period, pv_count, uv_count, favorite_count, cart_count,
              order_count, paid_order_count, paid_user_count, paid_sku_num,
              gmv, avg_order_amount, view_to_pay_rate
            from ads_biz_overview_result
        """)
        print(f"写入 ads.ads_biz_overview_df ds={ds} rows={cached.count()}")
    finally:
        spark.catalog.dropTempView("ads_biz_overview_result")
        cached.unpersist()


def build_overview(spark, ds):
    '''
    作用：从全站 DWS 计算客单价和浏览支付转化率。
    输入：SparkSession、展示截止日期 ds。
    输出：无；写入经营总览 ADS 的目标分区。
    '''
    assert_partition_exists(spark, "cdm.dws_site_stats_df", ds)
    result = spark.sql(f"""
        -- 分母为零时金额和比例返回零
        select
          stat_period,
          pv_count,
          uv_count,
          favorite_count,
          cart_count,
          order_count,
          paid_order_count,
          paid_user_count,
          paid_sku_num,
          gmv,
          cast(
            case when paid_order_count=0 then 0 else gmv / paid_order_count end
            as decimal(20,2)
          ) as avg_order_amount,
          cast(
            case when uv_count=0 then 0 else paid_user_count * 1.0 / uv_count end
            as decimal(10,6)
          ) as view_to_pay_rate
        from cdm.dws_site_stats_df
        where ds='{ds}'
    """)
    assert_result(result)
    write_partition(spark, result, ds)


def main():
    args = parse_args()
    spark = create_spark_session()
    try:
        build_overview(spark, args.ds)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

