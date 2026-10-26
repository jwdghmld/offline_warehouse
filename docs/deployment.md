# 离线部署

## 环境变量

在提交节点加载 `spark/mysql-spark.sh`，并按实际环境填写其中的“此处自定义”项。`spark/spark-env.sh` 与它保持一致，并提供 `SPARK_PYTHON`。Airflow DAG 还需要：

```text
OFFLINE_PROJECT_HOME=仓库在 Linux 上的绝对路径
SPARK_SUBMIT=绝对路径/spark-submit
GENERATOR_PYTHON=造数器 Python 解释器
ALERT_EMAIL=接收任务通知的地址
```

造数器可用 `GENERATOR_CONFIG` 指向 `source-data-generator/config/default.json` 或 `test-small.json`；MySQL 和 Kafka 参数也可通过 `MYSQL_*`、`ECOMMERCE_MYSQL_DATABASE`、`KAFKA_*` 环境变量覆盖 JSON。

## 初始化顺序

1. 执行 `contracts/mysql/01_business.sql`。
2. 执行 `contracts/mysql/02_offline.sql`。
3. 执行 `hive/ddl/01_ods.sql` 至 `05_ads.sql`。
4. 运行 `source-data-generator/1_generate_dimensions.py` 初始化冻结维度。
5. 将 `airflow/dags/` 部署到 Airflow DAG 目录，并安装 `airflow/requirements-py311.txt`。

## 作业运行

维度初始化后，正常每日链路由 `offline_ods_daily` 启动。手工补数时运行 `source-data-generator/2_generate_daily_facts.py --ds yyyyMMdd --publish`，再按 ODS、DWD、DWS/ADS、Publish 顺序提交对应 Spark 作业。所有 Spark 作业接收 `--ds yyyyMMdd`，DIM 初始化不接收日期参数。

## 运行边界

项目任务需要 Linux、Spark 3.5.8、Hive、YARN、MySQL JDBC Driver 和 Airflow 2.10.5。Windows 不启动 Airflow、Spark、Kafka 或数据生成任务。生产配置只能通过部署环境注入，仓库中的连接值均为占位符。
