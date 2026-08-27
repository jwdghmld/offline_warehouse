# coding: utf-8
#!/usr/bin/python3

import os

os.environ["PYSPARK_PYTHON"] = os.environ["SPARK_PYTHON"]
os.environ["PYSPARK_DRIVER_PYTHON"] = os.environ["SPARK_PYTHON"]

import argparse
from datetime import datetime

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


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
    parser = argparse.ArgumentParser(description="构建成功支付订单明细快照")
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
        SparkSession.builder.appName("dwd_trd_pay_dtl_snapshot")
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
    rows = spark.sql(f"show partitions {table} partition (ds='{ds}')").take(1)
    if not rows:
        raise DataQualityError(f"缺少分区：{table} ds={ds}")


def assert_result(dataframe):
    '''
    作用：检查交易快照的必填字段、主键和金额范围。
    输入：交易快照 DataFrame。
    输出：无；检查失败时抛出 DataQualityError。
    '''
    required = [
        "order_detail_id", "order_id", "payment_id", "user_id", "shop_id",
        "shop_name", "sku_id", "sku_name", "category_id", "category_name",
        "sku_num", "original_amount", "final_amount", "order_amount",
        "payment_amount", "create_time", "payment_time", "order_date",
        "payment_date",
    ]
    null_condition = None
    for column in required:
        current = F.col(column).isNull()
        null_condition = current if null_condition is None else null_condition | current
    if dataframe.where(null_condition).take(1):
        raise DataQualityError("交易快照存在必填字段为空的记录")
    if dataframe.groupBy("order_detail_id").count().where("count > 1").take(1):
        raise DataQualityError("交易快照存在重复 order_detail_id")
    if dataframe.where(
        "sku_num <= 0 or original_amount < 0 or final_amount < 0 "
        "or order_amount < 0 or payment_amount <= 0"
    ).take(1):
        raise DataQualityError("交易快照存在非法数量或金额")


def write_partition(spark, dataframe, ds):
    '''
    作用：覆盖交易 DWD 的目标快照分区。
    输入：SparkSession、交易快照 DataFrame、快照日期 ds。
    输出：无；写入 cdm.dwd_trd_pay_dtl_df 的 ds 分区。
    '''
    columns = [
        "order_detail_id", "order_id", "payment_id", "user_id", "shop_id",
        "shop_name", "sku_id", "sku_name", "category_id", "category_name",
        "sku_num", "original_amount", "final_amount", "order_amount",
        "payment_amount", "create_time", "payment_time", "order_date",
        "payment_date",
    ]
    if dataframe.columns != columns:
        raise ValueError(f"字段顺序不一致：{dataframe.columns}")
    cached = dataframe.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        row_count = cached.count()
        cached.coalesce(OUTPUT_PARTITIONS).createOrReplaceTempView(
            "dwd_trd_pay_dtl_result"
        )
        spark.sql(f"""
            -- 覆盖目标快照分区
            insert overwrite table cdm.dwd_trd_pay_dtl_df partition (ds='{ds}')
            select
              order_detail_id, order_id, payment_id, user_id,
              shop_id, shop_name, sku_id, sku_name, category_id, category_name,
              sku_num, original_amount, final_amount, order_amount,
              payment_amount, create_time, payment_time, order_date, payment_date
            from dwd_trd_pay_dtl_result
        """)
        print(f"写入 cdm.dwd_trd_pay_dtl_df ds={ds} rows={row_count}")
    finally:
        spark.catalog.dropTempView("dwd_trd_pay_dtl_result")
        cached.unpersist()


def build_snapshot(spark, ds):
    '''
    作用：清洗截至目标日的订单、明细和支付并生成完整交易快照。
    输入：SparkSession、快照截止日期 ds。
    输出：无；写入交易 DWD 分区并打印被剔除的订单数量。
    '''
    for table in (
        "ods.ods_order_info_di",
        "ods.ods_order_detail_di",
        "ods.ods_payment_info_di",
    ):
        assert_partition_exists(spark, table, ds)
    candidate_count = spark.sql(f"""
        -- 统计目标日前出现过成功支付的订单
        select count(distinct order_id) as order_count
        from ods.ods_payment_info_di
        where ds <= '{ds}' and payment_status='SUCCESS'
    """).first()["order_count"]

    result = spark.sql(f"""
        -- 各事实表先按业务主键保留最新记录
        with order_ranked as (
          select
            order_id, user_id, shop_id, order_status, order_amount,
            create_time, update_time,
            row_number() over (
              partition by order_id order by update_time desc, ds desc
            ) as row_num
          from ods.ods_order_info_di
          where ds <= '{ds}'
        ), latest_order as (
          select order_id, user_id, shop_id, order_status, order_amount, create_time
          from order_ranked
          where row_num=1
        ), detail_ranked as (
          select
            order_detail_id, order_id, sku_id, sku_num,
            original_amount, final_amount, create_time,
            row_number() over (
              partition by order_detail_id order by create_time desc, ds desc
            ) as row_num
          from ods.ods_order_detail_di
          where ds <= '{ds}'
        ), latest_detail as (
          select
            order_detail_id, order_id, sku_id, sku_num,
            original_amount, final_amount, create_time
          from detail_ranked
          where row_num=1
        ), payment_ranked as (
          select
            payment_id, order_id, user_id, payment_type, payment_status,
            payment_amount, payment_time, create_time,
            row_number() over (
              partition by payment_id order by payment_time desc, create_time desc, ds desc
            ) as row_num
          from ods.ods_payment_info_di
          where ds <= '{ds}'
        ), latest_payment as (
          select
            payment_id, order_id, user_id, payment_type, payment_status,
            payment_amount, payment_time
          from payment_ranked
          where row_num=1
        ), success_payment as (
          select
            payment_id, order_id, user_id, payment_amount, payment_time,
            count(*) over (partition by order_id) as success_count
          from latest_payment
          where payment_status='SUCCESS' and payment_amount > 0
        ), single_success as (
          select payment_id, order_id, user_id, payment_amount, payment_time
          from success_payment
          where success_count=1
        ), detail_amount as (
          select order_id, sum(final_amount) as detail_final_amount
          from latest_detail
          group by order_id
        ), valid_order as (
          select
            orders.order_id, orders.user_id, orders.shop_id, orders.order_amount,
            orders.create_time, paid.payment_id, paid.payment_amount, paid.payment_time
          from latest_order orders
          inner join single_success paid
            on orders.order_id=paid.order_id and orders.user_id=paid.user_id
          inner join detail_amount detail
            on orders.order_id=detail.order_id
          where orders.order_status='PAID'
            and paid.payment_time >= orders.create_time
            and detail.detail_final_amount=orders.order_amount
            and paid.payment_amount=orders.order_amount
        )
        select
          detail.order_detail_id,
          valid.order_id,
          valid.payment_id,
          valid.user_id,
          valid.shop_id,
          sku.shop_name,
          detail.sku_id,
          sku.sku_name,
          sku.category_id,
          sku.category_name,
          detail.sku_num,
          cast(detail.original_amount as decimal(20,2)) as original_amount,
          cast(detail.final_amount as decimal(20,2)) as final_amount,
          cast(valid.order_amount as decimal(20,2)) as order_amount,
          cast(valid.payment_amount as decimal(20,2)) as payment_amount,
          valid.create_time,
          valid.payment_time,
          to_date(valid.create_time) as order_date,
          to_date(valid.payment_time) as payment_date
        from latest_detail detail
        inner join valid_order valid on detail.order_id=valid.order_id
        inner join cdm.dim_sku_df sku
          on detail.sku_id=sku.sku_id
         and sku.shop_id=valid.shop_id
    """)
    assert_result(result)
    valid_count = result.select("order_id").distinct().count()
    print(
        f"交易订单清洗：candidate={candidate_count}, valid={valid_count}, "
        f"excluded={candidate_count - valid_count}"
    )
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

