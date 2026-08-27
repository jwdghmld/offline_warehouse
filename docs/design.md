# 离线数仓设计

## 数据分层

`ecommerce_business` 保存 2 张冻结维表和 5 张每日事实表。Spark JDBC 将事实按业务时间采集到 Hive ODS；DIM 为无分区全量表，DWD 保存截至 `ds` 的有效事实快照，DWS 计算 `1d`、`7d`、`30d`、`all`，ADS 面向页面输出经营总览、SKU 销售排行和店铺评分。

```text
MySQL 业务源表 -> ODS -> DWD -> DWS -> ADS -> MySQL offline
                    ^       ^       ^       ^
                 日增量  截止日快照 统计窗口 展示快照
```

## 调度关系

```text
offline_ods_daily
  造数并发布 Kafka -> ODS JDBC 采集
      -> offline_dwd_daily
           3 张 DWD 串行构建
      -> offline_dws_ads_daily
           3 张 DWS -> 3 张 ADS 串行构建
      -> offline_publish_daily
           3 张 ADS 发布到 MySQL offline
```

ODS DAG 每天 02:00 运行，其余 DAG 由上游成功后触发。所有 DAG 支持通过 `dag_run.conf.target_ds` 指定业务日期；未指定时使用上海时区前一自然日。

## 指标口径

- 经营总览：PV、UV、收藏、加购、下单数、支付订单数、支付用户数、支付件数、GMV、客单价和浏览到支付转化率。
- SKU 排行：按 GMV、支付件数和支付订单数分别取各统计周期 Top 10。
- 店铺评分：评分数、平均分、好中差数量、好评/差评率、Wilson 好评率和风险标记。

## 一致性与质量

造数器先在单个 InnoDB 事务中覆盖五张事实表，提交成功后才发布 Kafka。MySQL 失败时整体回滚；Kafka 失败时使用相同 `ds` 和种子重发稳定事件。Spark 任务在写入前后校验源端行数、主键和关键金额，目标分区采用幂等覆盖。离线链路不读取 Kafka、Flink 状态或实时结果表。

## 合同

业务和离线结果表见 `contracts/mysql/`；Kafka 交易事件合同见 `contracts/kafka/topic-contract.md`。实时仓库复制同一 Kafka 合同，生产方和消费方以版本号协同变更。
