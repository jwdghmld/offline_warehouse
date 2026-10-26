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
    作用：读取并校验快照截止日期。
    输入：命令行中的 --ds，格式为 yyyyMMdd。
    输出：包含 ds 的 argparse.Namespace。
    '''
    parser = argparse.ArgumentParser(description="构建店铺评分完整快照")
    parser.add_argument("--ds", required=True, help="快照截止日期，格式 yyyyMMdd")
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
        SparkSession.builder.appName("dwd_shop_rating_snapshot")
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
    作用：检查上游表是否存在目标日期分区。
    输入：SparkSession、表名 table、业务日期 ds。
    输出：无；分区不存在时抛出 DataQualityError。
    '''
    if not spark.sql(f"show partitions {table} partition (ds='{ds}')").take(1):
        raise DataQualityError(f"缺少分区：{table} ds={ds}")


def assert_result(dataframe):
    '''
    作用：检查评分快照的必填字段和业务主键。
    输入：店铺评分快照 DataFrame。
    输出：无；检查失败时抛出 DataQualityError。
    '''
    if dataframe.where(
        "rating_id is null or order_id is null or shop_id is null "
        "or shop_name is null or shop_score is null or score_level is null "
        "or rating_time is null or rating_date is null"
    ).take(1):
        raise DataQualityError("店铺评分快照存在必填字段为空的记录")
    if dataframe.groupBy("order_id").count().where("count > 1").take(1):
        raise DataQualityError("店铺评分快照存在重复 order_id")


def write_partition(spark, dataframe, ds):
    '''
    作用：覆盖店铺评分 DWD 的目标快照分区。
    输入：SparkSession、评分快照 DataFrame、快照日期 ds。
    输出：无；写入 cdm.dwd_shop_rating_df 的 ds 分区。
    '''
    cached = dataframe.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        row_count = cached.count()
        cached.coalesce(OUTPUT_PARTITIONS).createOrReplaceTempView(
            "dwd_shop_rating_result"
        )
        spark.sql(f"""
            -- 覆盖目标快照分区
            insert overwrite table cdm.dwd_shop_rating_df partition (ds='{ds}')
            select
              rating_id, order_id, shop_id, shop_name,
              shop_score, score_level, rating_time, rating_date
            from dwd_shop_rating_result
        """)
        print(f"写入 cdm.dwd_shop_rating_df ds={ds} rows={row_count}")
    finally:
        spark.catalog.dropTempView("dwd_shop_rating_result")
        cached.unpersist()


def build_snapshot(spark, ds):
    '''
    作用：按订单去重评分，并只保留有效成功支付订单的店铺评分。
    输入：SparkSession、快照截止日期 ds。
    输出：无；写入店铺评分 DWD 的目标分区。
    '''
    assert_partition_exists(spark, "ods.ods_rating_info_di", ds)
    assert_partition_exists(spark, "cdm.dwd_trd_pay_dtl_df", ds)
    result = spark.sql(f"""
        -- 评分按订单保留最后一条，再校验支付订单和店铺
        with rating_ranked as (
          select
            rating_id, order_id, shop_id, shop_score, rating_time,
            row_number() over (
              partition by order_id order by rating_time desc, rating_id desc, ds desc
            ) as row_num
          from ods.ods_rating_info_di
          where ds <= '{ds}'
            and rating_id is not null
            and order_id is not null
            and shop_id is not null
            and shop_score between 1 and 5
            and rating_time is not null
            and date_format(rating_time, 'yyyyMMdd') <= '{ds}'
        ), paid_order as (
          select order_id, max(shop_id) as shop_id, max(shop_name) as shop_name
          from cdm.dwd_trd_pay_dtl_df
          where ds='{ds}'
          group by order_id
        )
        select
          rating.rating_id,
          rating.order_id,
          rating.shop_id,
          paid.shop_name,
          rating.shop_score,
          case
            when rating.shop_score >= 4 then 'good'
            when rating.shop_score = 3 then 'mid'
            else 'bad'
          end as score_level,
          rating.rating_time,
          to_date(rating.rating_time) as rating_date
        from rating_ranked rating
        inner join paid_order paid
          on rating.order_id=paid.order_id and rating.shop_id=paid.shop_id
        where rating.row_num=1
    """)
    assert_result(result)
    write_partition(spark, result, ds)


def main():
    args = parse_args()
    spark = create_spark_session()
    try:
        build_snapshot(spark, args.ds)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

