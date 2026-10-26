#!/usr/bin/env bash

# Spark JDBC 使用的 MySQL 连接参数
export MYSQL_HOST="此处自定义"
export MYSQL_PORT="此处自定义"
export MYSQL_USER="此处自定义"
export MYSQL_PASSWORD="此处自定义"
export MYSQL_DRIVER="com.mysql.cj.jdbc.Driver"
export MYSQL_TIMEZONE="Asia/Shanghai"
export MYSQL_CHARSET="utf8"

# 业务源库和离线结果库
export ECOMMERCE_MYSQL_DATABASE="ecommerce_business"
export OFFLINE_MYSQL_DATABASE="offline"

# Spark SQL计算分区和最终输出分区
export SPARK_SQL_SHUFFLE_PARTITIONS="12"
export SPARK_OUTPUT_PARTITIONS="3"

# 运行路径与通知配置
export SPARK_PYTHON="此处自定义"
export SPARK_SUBMIT="此处自定义"
export OFFLINE_PROJECT_HOME="此处自定义"
export GENERATOR_PYTHON="此处自定义"
export ALERT_EMAIL="此处自定义"

# 每日造数器发布 Kafka 使用
export KAFKA_BOOTSTRAP_SERVERS="此处自定义"
export KAFKA_CLIENT_ID="offline-data-generator"
