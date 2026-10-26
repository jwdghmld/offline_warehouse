# coding: utf-8
#!/usr/bin/python3

import os

os.environ["PYSPARK_PYTHON"] = os.environ["SPARK_PYTHON"]
os.environ["PYSPARK_DRIVER_PYTHON"] = os.environ["SPARK_PYTHON"]

import argparse
from datetime import datetime

from pyspark.sql import SparkSession


MYSQL_URL = (
    f"jdbc:mysql://{os.environ['MYSQL_HOST']}:{os.environ['MYSQL_PORT']}/"
    f"{os.environ['OFFLINE_MYSQL_DATABASE']}?useUnicode=true"
    f"&characterEncoding={os.environ['MYSQL_CHARSET']}"
    f"&serverTimezone={os.environ['MYSQL_TIMEZONE']}"
)
MYSQL_PROPERTIES = {
    "user": os.environ["MYSQL_USER"],
    "password": os.environ["MYSQL_PASSWORD"],
    "driver": os.environ["MYSQL_DRIVER"],
}
MYSQL_TABLE = "offline.ads_shop_rating_df"
SHUFFLE_PARTITIONS = os.environ.get("SPARK_SQL_SHUFFLE_PARTITIONS", "12")
OUTPUT_PARTITIONS = int(os.environ.get("SPARK_OUTPUT_PARTITIONS", "3"))


def main():
    parser = argparse.ArgumentParser(description="发布店铺评分到 MySQL")
    parser.add_argument("--ds", required=True, help="业务日期，格式 yyyyMMdd")
    args = parser.parse_args()
    if len(args.ds) != 8:
        raise ValueError("--ds 必须是 8 位日期")
    datetime.strptime(args.ds, "%Y%m%d")

    spark = (
        SparkSession.builder.appName("publish_ads_shop_rating")
        .config("spark.dynamicAllocation.enabled", "false")
        .config("spark.default.parallelism", "12")
        .config("spark.sql.session.timeZone", "Asia/Shanghai")
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
        # 读取待发布的店铺评分
        source = spark.sql(f"""
            select
              ds, stat_period, shop_id, rating_count, avg_score,
              good_count, mid_count, bad_count, good_rate, bad_rate,
              wilson_good_rate, cast(risk_flag as tinyint) as risk_flag
            from ads.ads_shop_rating_df
            where ds='{args.ds}'
        """).cache()
        source_count = source.count()
        key_count = source.select("ds", "stat_period", "shop_id").distinct().count()
        non_null_count = source.na.drop().count()
        if (
            source_count == 0
            or key_count != source_count
            or non_null_count != source_count
        ):
            raise ValueError(
                f"店铺评分校验失败：ds={args.ds}, rows={source_count}, "
                f"keys={key_count}, non_null_rows={non_null_count}"
            )
        if source.where(
            "stat_period not in ('1d', '7d', '30d', 'all') "
            "or shop_id is null or rating_count < 0 "
            "or rating_count <> good_count + mid_count + bad_count "
            "or avg_score < 0 or avg_score > 5 "
            "or good_rate < 0 or good_rate > 1 "
            "or bad_rate < 0 or bad_rate > 1 "
            "or wilson_good_rate < 0 or wilson_good_rate > 1 "
            "or risk_flag not in (0, 1)"
        ).take(1):
            raise ValueError("店铺评分存在非法字段")

        # 删除 MySQL 中相同日期的旧结果
        jvm = spark.sparkContext._gateway.jvm
        jvm.java.lang.Class.forName(os.environ["MYSQL_DRIVER"])
        connection = jvm.java.sql.DriverManager.getConnection(
            MYSQL_URL, os.environ["MYSQL_USER"], os.environ["MYSQL_PASSWORD"]
        )
        try:
            connection.setAutoCommit(False)
            statement = connection.prepareStatement(
                f"delete from {MYSQL_TABLE} where ds=?"
            )
            try:
                statement.setString(1, args.ds)
                statement.executeUpdate()
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            finally:
                statement.close()
        finally:
            connection.close()

        source.coalesce(OUTPUT_PARTITIONS).write.jdbc(
            MYSQL_URL, MYSQL_TABLE, mode="append", properties=MYSQL_PROPERTIES
        )

        # 回查发布后的行数和主键数
        check = spark.read.jdbc(
            MYSQL_URL,
            f"(select count(1) as row_count, "
            f"count(distinct stat_period, shop_id) as key_count "
            f"from {MYSQL_TABLE} where ds='{args.ds}') publish_check",
            properties=MYSQL_PROPERTIES,
        ).first()
        if check["row_count"] != source_count or check["key_count"] != source_count:
            raise ValueError("店铺评分发布结果不完整")

        print(f"店铺评分发布完成：ds={args.ds}, rows={source_count}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

