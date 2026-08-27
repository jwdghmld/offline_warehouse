# coding: utf-8
#!/usr/bin/python3

import os

os.environ["PYSPARK_PYTHON"] = os.environ["SPARK_PYTHON"]
os.environ["PYSPARK_DRIVER_PYTHON"] = os.environ["SPARK_PYTHON"]

import argparse
from datetime import datetime, timedelta

from pyspark.sql import SparkSession


MYSQL_URL = (
    f"jdbc:mysql://{os.environ['MYSQL_HOST']}:"
    f"{os.environ['MYSQL_PORT']}/"
    f"{os.environ['ECOMMERCE_MYSQL_DATABASE']}"
    "?useUnicode=true"
    f"&characterEncoding={os.environ['MYSQL_CHARSET']}"
    f"&serverTimezone={os.environ['MYSQL_TIMEZONE']}"
)
MYSQL_PROPERTIES = {
    "user": os.environ["MYSQL_USER"],
    "password": os.environ["MYSQL_PASSWORD"],
    "driver": os.environ["MYSQL_DRIVER"],
}
SHUFFLE_PARTITIONS = os.environ.get("SPARK_SQL_SHUFFLE_PARTITIONS", "12")
OUTPUT_PARTITIONS = int(os.environ.get("SPARK_OUTPUT_PARTITIONS", "3"))

TABLES = (
    (
        "order_info",
        "ods.ods_order_info_di",
        "create_time",
        "order_id, user_id, shop_id, order_status, order_amount, "
        "create_time, update_time",
    ),
    (
        "order_detail",
        "ods.ods_order_detail_di",
        "create_time",
        "order_detail_id, order_id, sku_id, sku_num, original_amount, "
        "final_amount, create_time",
    ),
    (
        "payment_info",
        "ods.ods_payment_info_di",
        "payment_time",
        "payment_id, order_id, user_id, payment_type, payment_status, "
        "payment_amount, payment_time, create_time",
    ),
    (
        "user_behavior",
        "ods.ods_user_behavior_di",
        "event_time",
        "event_id, session_id, user_id, sku_id, event_type, event_time",
    ),
    (
        "rating_info",
        "ods.ods_rating_info_di",
        "rating_time",
        "rating_id, order_id, shop_id, shop_score, rating_time",
    ),
)


def main():
    parser = argparse.ArgumentParser(description="采集 MySQL 每日事实到 Hive ODS")
    parser.add_argument("--ds", required=True, help="业务日期，格式 yyyyMMdd")
    args = parser.parse_args()

    if len(args.ds) != 8:
        raise ValueError("--ds 必须是 8 位日期")
    start_time = datetime.strptime(args.ds, "%Y%m%d")
    end_time = start_time + timedelta(days=1)
    start_value = start_time.strftime("%Y-%m-%d %H:%M:%S")
    end_value = end_time.strftime("%Y-%m-%d %H:%M:%S")

    spark = (
        SparkSession.builder.appName("ods_mysql_to_hive")
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

    try:
        for source_table, target_table, time_column, columns in TABLES:
            query = (
                f"(select {columns} from {source_table} "
                f"where {time_column} >= '{start_value}' "
                f"and {time_column} < '{end_value}') {source_table}"
            )
            dataframe = spark.read.jdbc(
                MYSQL_URL,
                query,
                properties=MYSQL_PROPERTIES,
            )
            source_count = dataframe.count()
            view_name = f"mysql_{source_table}"
            dataframe.coalesce(OUTPUT_PARTITIONS).createOrReplaceTempView(view_name)

            # 只覆盖当前业务日分区
            spark.sql(f"""
                insert overwrite table {target_table} partition (ds='{args.ds}')
                select {columns}
                from {view_name}
            """)

            target_count = spark.sql(
                f"select count(*) as row_count from {target_table} "
                f"where ds='{args.ds}'"
            ).first()["row_count"]
            if source_count != target_count:
                raise ValueError(
                    f"{source_table} 行数不一致："
                    f"source={source_count}, target={target_count}"
                )
            print(
                f"ODS 写入完成：table={target_table}, "
                f"ds={args.ds}, rows={source_count}"
            )
            spark.catalog.dropTempView(view_name)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

