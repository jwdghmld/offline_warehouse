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
    parser = argparse.ArgumentParser(description="构建店铺评分统计快照")
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
        SparkSession.builder.appName("dws_shop_rating")
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
    作用：检查店铺评分 DWD 快照是否存在。
    输入：SparkSession、表名 table、统计日期 ds。
    输出：无；分区不存在时抛出 DataQualityError。
    '''
    if not spark.sql(f"show partitions {table} partition (ds='{ds}')").take(1):
        raise DataQualityError(f"缺少分区：{table} ds={ds}")


def assert_result(dataframe):
    '''
    作用：检查店铺评分统计主键和计数关系。
    输入：店铺评分统计 DataFrame。
    输出：无；检查失败时抛出 DataQualityError。
    '''
    if dataframe.groupBy("stat_period", "shop_id").count().where("count > 1").take(1):
        raise DataQualityError("店铺评分统计存在重复主键")
    if dataframe.where(
        "rating_count < 0 or score_sum < 0 or good_count < 0 "
        "or mid_count < 0 or bad_count < 0 "
        "or rating_count <> good_count + mid_count + bad_count"
    ).take(1):
        raise DataQualityError("店铺评分统计计数不一致")


def write_partition(spark, dataframe, ds):
    '''
    作用：覆盖店铺评分 DWS 的目标分区。
    输入：SparkSession、店铺评分统计 DataFrame、统计日期 ds。
    输出：无；写入 cdm.dws_shop_rating_df 的 ds 分区。
    '''
    cached = dataframe.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        cached.coalesce(OUTPUT_PARTITIONS).createOrReplaceTempView(
            "dws_shop_rating_result"
        )
        spark.sql(f"""
            -- 覆盖目标统计分区
            insert overwrite table cdm.dws_shop_rating_df partition (ds='{ds}')
            select
              stat_period, shop_id, shop_name, rating_count, score_sum,
              good_count, mid_count, bad_count
            from dws_shop_rating_result
        """)
        print(f"写入 cdm.dws_shop_rating_df ds={ds} rows={cached.count()}")
    finally:
        spark.catalog.dropTempView("dws_shop_rating_result")
        cached.unpersist()


def build_stats(spark, ds):
    '''
    作用：按店铺计算四种时间范围的评分计数和分值合计。
    输入：SparkSession、统计截止日期 ds。
    输出：无；写入店铺评分 DWS 的目标分区。
    '''
    assert_partition_exists(spark, "cdm.dwd_shop_rating_df", ds)
    result = spark.sql(f"""
        -- 每条评分同时进入符合日期范围的统计周期
        with periods as (
          select stack(
            4,
            '1d', date_sub(to_date('{ds}', 'yyyyMMdd'), 0),
            '7d', date_sub(to_date('{ds}', 'yyyyMMdd'), 6),
            '30d', date_sub(to_date('{ds}', 'yyyyMMdd'), 29),
            'all', cast('1900-01-01' as date)
          ) as (stat_period, start_date)
        )
        select
          periods.stat_period,
          rating.shop_id,
          max(rating.shop_name) as shop_name,
          cast(count(*) as bigint) as rating_count,
          cast(sum(rating.shop_score) as bigint) as score_sum,
          cast(sum(case when rating.score_level='good' then 1 else 0 end) as bigint)
            as good_count,
          cast(sum(case when rating.score_level='mid' then 1 else 0 end) as bigint)
            as mid_count,
          cast(sum(case when rating.score_level='bad' then 1 else 0 end) as bigint)
            as bad_count
        from periods
        inner join cdm.dwd_shop_rating_df rating
          on rating.ds='{ds}'
         and rating.rating_date between periods.start_date and to_date('{ds}', 'yyyyMMdd')
        group by periods.stat_period, rating.shop_id
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

