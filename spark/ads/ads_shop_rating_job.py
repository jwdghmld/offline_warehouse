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
    parser = argparse.ArgumentParser(description="构建店铺评分展示快照")
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
        SparkSession.builder.appName("ads_shop_rating")
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
    作用：检查店铺评分 DWS 分区是否存在。
    输入：SparkSession、表名 table、展示日期 ds。
    输出：无；分区不存在时抛出 DataQualityError。
    '''
    if not spark.sql(f"show partitions {table} partition (ds='{ds}')").take(1):
        raise DataQualityError(f"缺少分区：{table} ds={ds}")


def assert_result(dataframe):
    '''
    作用：检查店铺评分结果主键、比例和风险标记。
    输入：店铺评分 ADS DataFrame。
    输出：无；检查失败时抛出 DataQualityError。
    '''
    if dataframe.groupBy("stat_period", "shop_id").count().where("count > 1").take(1):
        raise DataQualityError("店铺评分 ADS 存在重复主键")
    if dataframe.where(
        "avg_score < 0 or good_rate < 0 or good_rate > 1 "
        "or bad_rate < 0 or bad_rate > 1 "
        "or wilson_good_rate < 0 or wilson_good_rate > 1"
    ).take(1):
        raise DataQualityError("店铺评分 ADS 存在非法评分或比例")


def write_partition(spark, dataframe, ds):
    '''
    作用：覆盖店铺评分 ADS 的目标分区。
    输入：SparkSession、店铺评分结果 DataFrame、展示日期 ds。
    输出：无；写入 ads.ads_shop_rating_df 的 ds 分区。
    '''
    cached = dataframe.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        cached.coalesce(OUTPUT_PARTITIONS).createOrReplaceTempView(
            "ads_shop_rating_result"
        )
        spark.sql(f"""
            -- 覆盖目标展示分区
            insert overwrite table ads.ads_shop_rating_df partition (ds='{ds}')
            select
              stat_period, shop_id, shop_name, rating_count, avg_score,
              good_count, mid_count, bad_count, good_rate, bad_rate,
              wilson_good_rate, risk_flag
            from ads_shop_rating_result
        """)
        print(f"写入 ads.ads_shop_rating_df ds={ds} rows={cached.count()}")
    finally:
        spark.catalog.dropTempView("ads_shop_rating_result")
        cached.unpersist()


def build_rating(spark, ds):
    '''
    作用：计算店铺平均分、好差评率、Wilson 下限和风险标记。
    输入：SparkSession、展示截止日期 ds。
    输出：无；写入店铺评分 ADS 的目标分区。
    '''
    assert_partition_exists(spark, "cdm.dws_shop_rating_df", ds)
    result = spark.sql(f"""
        -- Wilson 下限使用 95% 置信水平 z=1.96
        with rates as (
          select
            stat_period, shop_id, shop_name, rating_count, score_sum,
            good_count, mid_count, bad_count,
            good_count * 1.0 / rating_count as good_rate_raw,
            bad_count * 1.0 / rating_count as bad_rate_raw
          from cdm.dws_shop_rating_df
          where ds='{ds}' and rating_count > 0
        )
        select
          stat_period,
          shop_id,
          shop_name,
          rating_count,
          cast(score_sum * 1.0 / rating_count as decimal(10,6)) as avg_score,
          good_count,
          mid_count,
          bad_count,
          cast(good_rate_raw as decimal(10,6)) as good_rate,
          cast(bad_rate_raw as decimal(10,6)) as bad_rate,
          cast(
            (
              good_rate_raw + 3.8416 / (2 * rating_count)
              - 1.96 * sqrt(
                  (good_rate_raw * (1 - good_rate_raw)
                    + 3.8416 / (4 * rating_count)) / rating_count
                )
            ) / (1 + 3.8416 / rating_count)
            as decimal(10,6)
          ) as wilson_good_rate,
          cast(rating_count >= 20 and bad_rate_raw >= 0.30 as boolean) as risk_flag
        from rates
    """)
    assert_result(result)
    write_partition(spark, result, ds)


def main():
    args = parse_args()
    spark = create_spark_session()
    try:
        build_rating(spark, args.ds)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

