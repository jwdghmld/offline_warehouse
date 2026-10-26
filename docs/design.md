# 离线数仓详细设计

[返回项目首页](../README.md)

本文集中说明数据分层、指标口径与处理边界。结果展示和代码入口见项目首页。

## 技术栈

| 分类 | 技术与版本 | 用途 |
|---|---|---|
| 数据生成 | Python 3.9+ | 生成每日事实和 Kafka 交易事件 |
| 业务数据源 | MySQL 8.x | 保存店铺、SKU、订单、支付、行为和评分 |
| 离线存储 | Hive、HDFS、ORC | 保存分区历史数据和数仓快照 |
| 离线计算 | Spark 3.5.8、PySpark、Spark SQL | JDBC 采集、明细清洗、主题汇总和 ADS 计算 |
| 资源调度 | YARN | 运行 Spark Application |
| 任务调度 | Airflow 2.10.5 | 编排四个每日 DAG |
| 消息系统 | Kafka | 向实时风控发布订单、明细和支付事件 |
| 结果服务 | MySQL `offline` | 保存三类可直接查询的 ADS 结果 |
| 时间标准 | Asia/Shanghai | 统一业务时间、Spark SQL、JDBC 和分区日期 |

## 数据源设计

MySQL `ecommerce_business` 共有 2 张维表和 5 张每日事实表。

| 类型 | 表名 | 作用 |
|---|---|---|
| 维度 | `shop_info` |  保存店铺名称、省份、开店时间和状态 |
| 维度 | `sku_info` |  保存商品、类目、店铺归属、价格和状态 |
| 事实 | `order_info` |  保存订单用户、店铺、状态、金额和创建时间 |
| 事实 | `order_detail` | 保存 SKU、数量、原始金额和成交金额 |
| 事实 | `payment_info` | 支持多次失败支付和最多一次成功支付 |
| 事实 | `user_behavior` | 保存浏览、收藏和加购行为，仅用于离线分析 |
| 事实 | `rating_info` | 保存店铺评分及评分时间，仅用于离线分析 |

## 数仓分层架构

![离线数仓分层与表作用](images/warehouse-layers.png)

### ODS：原始数据层

| Hive 表 | 来源 | 作用 |
|---|---|---|
| `ods.ods_order_info_di` | `order_info` | 保存订单原始增量 |
| `ods.ods_order_detail_di` | `order_detail` | 保存订单商品明细原始增量 |
| `ods.ods_payment_info_di` | `payment_info` | 保存每次支付尝试原始增量 |
| `ods.ods_user_behavior_di` | `user_behavior` | 保存用户行为原始增量 |
| `ods.ods_rating_info_di` | `rating_info` | 保存店铺评分原始增量 |

ODS 使用 `ds` 表示业务发生日。Spark JDBC 根据各表业务时间字段使用左闭右开日期范围读取 MySQL，并通过静态 `insert overwrite` 只覆盖目标日期分区。

### DIM：公共维度层

| Hive 表 | 作用 |
|---|---|
| `cdm.dim_shop_df` | 店铺维度，店铺名称和省份 |
| `cdm.dim_sku_df` | SKU 维度，商品、类目和店铺归属 |

### DWD：明细数据层

| Hive 表 | 作用 |
|---|---|
| `cdm.dwd_trd_pay_dtl_df` | 统一订单、明细、成功支付和商品维度，形成有效交易明细 |
| `cdm.dwd_user_behavior_df` | 关联 SKU、类目和店铺信息的用户行为明细 |
| `cdm.dwd_shop_rating_df` | 关联有效交易与店铺维度的评分明细 |

DWD 的 `ds` 表示快照截止日。作业读取 `ods.ds <= target_ds` 的历史事实，对支付时序、重复成功支付、明细金额和支付金额进行清洗，并保存截至目标日的完整有效快照。

### DWS：公共汇总层

| Hive 表 | 汇总主题 | 主要指标 |
|---|---|---|
| `cdm.dws_site_stats_df` | 站点经营 | PV、UV、订单、支付用户、GMV、客单价、转化率 |
| `cdm.dws_sku_trade_df` | SKU 交易 | 支付订单、支付用户、支付件数、GMV |
| `cdm.dws_shop_rating_df` | 店铺评分 | 评分数、平均分、好中差评、好评率和差评率 |

DWS 只读取同一 `target_ds` 的 DWD 快照，统一计算 `1d`、`7d`、`30d` 和 `all` 四种统计周期，为 ADS 提供可复用的主题指标。

### ADS：应用数据层

| Hive / MySQL 表 | 页面主题 | 结果粒度 |
|---|---|---|
| `ads.ads_biz_overview_df` | 经营总览 | `ds + stat_period` |
| `ads.ads_sku_topn_df` | SKU 销售排行 | `ds + stat_period + rank_type + rank_no` |
| `ads.ads_shop_rating_df` | 店铺评分 | `ds + stat_period + shop_id` |

ADS 的 `ds` 表示展示截止日。三张 Hive ADS 表计算完成后，通过三个独立 Spark JDBC 发布作业写入 MySQL `offline` 同名结果表，发布前校验源数据的行数、业务键和关键字段，发布后回查目标行数与业务键数量。

## 完整数据链路

### 维度初始化

```text
1_generate_dimensions.py
  -> MySQL shop_info / sku_info
  -> dim_init_job.py
  -> cdm.dim_shop_df / cdm.dim_sku_df
```

店铺和 SKU 是后续每日事实使用的冻结实体池。

### 每日事实与 Kafka 发布

```text
2_generate_daily_facts.py --ds target_ds --publish
  -> MySQL 五张事实表单事务覆盖
  -> COMMIT 成功
  -> Kafka 订单 / 明细 / 支付事件
```

Kafka 发布不是 MySQL 与 Kafka 的分布式事务。MySQL 已提交但 Kafka 失败时，使用相同日期和确定性种子重跑，重新发送相同 `event_id` 集合，由实时侧状态去重和稳定告警主键共同收敛。

### ODS 到 ADS

```text
MySQL -> ODS 日增量
      -> DWD 截止日明细快照
      -> DWS 多周期公共汇总
      -> ADS 页面主题结果
      -> MySQL offline
```

## Airflow 调度

![Airflow 每日调度链路](images/airflow-pipeline.png)

| DAG | 调度方式 | 主要任务 |
|---|---|---|
| `offline_ods_daily` | 每天 02:00 | 生成事实、发布 Kafka、采集五张 ODS |
| `offline_dwd_daily` | ODS 成功后触发 | 串行构建三张 DWD |
| `offline_dws_ads_daily` | DWD 成功后触发 | 串行构建三张 DWS 和三张 ADS |
| `offline_publish_daily` | ADS 成功后触发 | 发布三张 MySQL 结果表并通知 |

四个 DAG 均设置 `max_active_runs=1` 和 `max_active_tasks=1`。上游通过 `TriggerDagRunOperator` 将同一个 `target_ds` 传给下游，避免跨阶段日期不一致。

当前串行配置用于适配本地集群资源；其他环境中的并行度应结合任务依赖与可用资源确定。

既有运行截图见[仓库首页](../README.md#调度设计)。

## 日期和快照语义

| 数据层 | `ds` 含义 |
|---|---|
| ODS | 业务发生日 |
| DIM | 无分区，冻结全量 |
| DWD | 历史有效事实快照截止日 |
| DWS | 公共指标统计截止日 |
| ADS | 页面展示截止日 |

统一时区为 `Asia/Shanghai`，日期格式为 `yyyyMMdd`，Hive 分区字段统一为 `ds STRING`。金额字段使用 `DECIMAL(20,2)`，比例字段使用 `DECIMAL(10,6)`。

## 核心指标口径

| 指标 | 口径 |
|---|---|
| PV | 统计周期内全部用户行为次数 |
| UV | 统计周期内发生行为的去重用户数 |
| 支付订单数 | 存在有效成功支付的去重订单数 |
| 支付用户数 | 存在有效成功支付的去重用户数 |
| 支付件数 | 有效交易明细中的 SKU 数量合计 |
| GMV | 有效支付交易明细成交金额合计 |
| 客单价 | GMV / 支付订单数 |
| 浏览到支付转化率 | 支付用户数 / 浏览用户数 |
| SKU TopN | 按 GMV、支付件数、支付订单数分别排名 |
| Wilson 好评率 | 在评分样本量差异下更稳健地衡量好评表现 |

## 数据质量与幂等设计

- 事务完整性：五张事实表在单个 MySQL 事务中整体提交或整体回滚。
- ODS 对账：逐表核对 MySQL 源端行数和 Hive 目标分区行数。
- 交易质量：剔除支付早于下单、重复成功支付、明细金额不一致和支付金额不一致的交易。
- 分区幂等：Hive 使用静态 `insert overwrite` 覆盖目标 `ds`。
- 发布重跑：先提交目标日期的 `DELETE`，再通过 Spark JDBC 追加结果。成功重跑可重建该日数据；删除与写入不在同一个事务中，失败期间可能出现结果为空或不完整，不能视为原子替换。
- 小文件控制：写 Hive 前按 `SPARK_OUTPUT_PARTITIONS` 合并最终输出分区。
- 资源稳定：关闭动态资源，启用 AQE、倾斜 Join 和广播优化。

## 资源规划

```text
Spark 3.5.8 on YARN
num-executors = 2
executor-cores = 3
spark.default.parallelism = 12
spark.sql.shuffle.partitions = 12（可配置）
最终输出分区 = 3（可配置）
```

该配置用于项目数据规模下的稳定串行执行。生产环境可通过环境变量调整 Shuffle 和输出分区。

## 数据合同

业务源表和离线结果表见 [MySQL DDL](../contracts/mysql/)。生产方与实时消费方共享 [Kafka 交易事件合同](../contracts/kafka/topic-contract.md)，当前版本为 `1.0.0`。离线计算不读取 Flink 状态或实时结果表。
