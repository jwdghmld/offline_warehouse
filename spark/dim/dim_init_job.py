# coding: utf-8
#!/usr/bin/python3

import os

os.environ["PYSPARK_PYTHON"] = os.environ["SPARK_PYTHON"]
os.environ["PYSPARK_DRIVER_PYTHON"] = os.environ["SPARK_PYTHON"]

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


def main():
    spark = (
        SparkSession.builder.appName("dim_init")
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
        # 读取店铺和商品基础数据
        shop = spark.read.jdbc(
            MYSQL_URL,
            "(select shop_id, shop_name, province_name, open_time, status "
            "from shop_info) shop_info",
            properties=MYSQL_PROPERTIES,
        ).cache()
        sku = spark.read.jdbc(
            MYSQL_URL,
            "(select sku_id, sku_name, category_id, category_name, "
            "shop_id, price, status from sku_info) sku_info",
            properties=MYSQL_PROPERTIES,
        ).cache()

        shop_count = shop.count()
        sku_count = sku.count()
        if shop_count == 0 or sku_count == 0:
            raise ValueError("店铺或商品数据为空")

        shop.createOrReplaceTempView("mysql_shop_info")
        sku.createOrReplaceTempView("mysql_sku_info")

        # 整表覆盖店铺维表
        spark.sql(f"""
            insert overwrite table cdm.dim_shop_df
            select /*+ coalesce({OUTPUT_PARTITIONS}) */
              shop_id, shop_name, province_name, open_time, status
            from mysql_shop_info
        """)

        # 商品关联店铺后写入商品维表
        spark.sql(f"""
            insert overwrite table cdm.dim_sku_df
            select /*+ coalesce({OUTPUT_PARTITIONS}) */
              sku.sku_id,
              sku.sku_name,
              sku.category_id,
              sku.category_name,
              sku.shop_id,
              shop.shop_name,
              shop.province_name,
              cast(sku.price as decimal(20,2)) as price,
              sku.status
            from mysql_sku_info sku
            inner join mysql_shop_info shop on sku.shop_id=shop.shop_id
        """)

        print(f"DIM 初始化完成：shops={shop_count}, skus={sku_count}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

