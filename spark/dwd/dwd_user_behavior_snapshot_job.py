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
    parser = argparse.ArgumentParser(description="构建用户行为完整快照")
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
        SparkSession.builder.appName("dwd_user_behavior_snapshot")
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
    作用：检查行为 ODS 是否存在目标日期分区。
    输入：SparkSession、表名 table、业务日期 ds。
    输出：无；分区不存在时抛出 DataQualityError。
    '''
    if not spark.sql(f"show partitions {table} partition (ds='{ds}')").take(1):
        raise DataQualityError(f"缺少分区：{table} ds={ds}")


def assert_result(dataframe):
    '''
    作用：检查行为快照的必填字段和事件主键。
    输入：行为快照 DataFrame。
    输出：无；检查失败时抛出 DataQualityError。
    '''
    if dataframe.where(
        "event_id is null or session_id is null or user_id is null "
        "or sku_id is null or sku_name is null or category_id is null "
        "or category_name is null or shop_id is null or shop_name is null "
        "or event_time is null or event_date is null"
    ).take(1):
        raise DataQualityError("行为快照存在必填字段为空的记录")
    if dataframe.groupBy("event_id").count().where("count > 1").take(1):
        raise DataQualityError("行为快照存在重复 event_id")


def write_partition(spark, dataframe, ds):
    '''
    作用：覆盖行为 DWD 的目标快照分区。
    输入：SparkSession、行为快照 DataFrame、快照日期 ds。
    输出：无；写入 cdm.dwd_user_behavior_df 的 ds 分区。
    '''
    cached = dataframe.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        row_count = cached.count()
        cached.coalesce(OUTPUT_PARTITIONS).createOrReplaceTempView(
            "dwd_user_behavior_result"
        )
        spark.sql(f"""
            -- 覆盖目标快照分区
            insert overwrite table cdm.dwd_user_behavior_df partition (ds='{ds}')
            select
              event_id, session_id, user_id, event_type,
              sku_id, sku_name, category_id, category_name, shop_id, shop_name,
              event_time, event_date
            from dwd_user_behavior_result
        """)
        print(f"写入 cdm.dwd_user_behavior_df ds={ds} rows={row_count}")
    finally:
        spark.catalog.dropTempView("dwd_user_behavior_result")
        cached.unpersist()


def build_snapshot(spark, ds):
    '''
    作用：清洗截至目标日的行为并关联冻结 SKU 维度。
    输入：SparkSession、快照截止日期 ds。
    输出：无；写入用户行为 DWD 的目标分区。
    '''
    assert_partition_exists(spark, "ods.ods_user_behavior_di", ds)
    result = spark.sql(f"""
        -- 同一 event_id 只保留最后一条有效记录
        with ranked as (
          select
            event_id, session_id, user_id, sku_id, event_type, event_time,
            row_number() over (
              partition by event_id order by event_time desc, ds desc
            ) as row_num
          from ods.ods_user_behavior_di
          where ds <= '{ds}'
            and event_id is not null
            and session_id is not null
            and user_id is not null
            and sku_id is not null
            and event_type in ('view', 'favorite', 'cart')
            and event_time is not null
            and date_format(event_time, 'yyyyMMdd') <= '{ds}'
        )
        select
          behavior.event_id,
          behavior.session_id,
          behavior.user_id,
          behavior.event_type,
          behavior.sku_id,
          sku.sku_name,
          sku.category_id,
          sku.category_name,
          sku.shop_id,
          sku.shop_name,
          behavior.event_time,
          to_date(behavior.event_time) as event_date
        from ranked behavior
        inner join cdm.dim_sku_df sku
          on behavior.sku_id=sku.sku_id
        where behavior.row_num=1
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

