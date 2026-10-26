-- Hive ODS DDL. Source facts are partitioned by their business date.
-- No explicit HDFS path or load metadata columns are used.

CREATE DATABASE IF NOT EXISTS ods COMMENT '电商离线数仓ODS原始数据层';

CREATE EXTERNAL TABLE IF NOT EXISTS ods.ods_order_info_di (
  order_id BIGINT COMMENT '订单唯一标识',
  user_id BIGINT COMMENT '下单用户唯一标识',
  shop_id BIGINT COMMENT '订单所属店铺唯一标识',
  order_status STRING COMMENT '订单状态：CREATED、PAID、CANCELLED',
  order_amount DECIMAL(20,2) COMMENT '订单金额，单位人民币元',
  create_time TIMESTAMP COMMENT '订单创建时间',
  update_time TIMESTAMP COMMENT '订单最后更新时间'
)
COMMENT 'ODS订单日增量表，一行代表一个订单'
PARTITIONED BY (ds STRING COMMENT '按create_time归属的业务日期，格式yyyyMMdd')
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS ods.ods_order_detail_di (
  order_detail_id BIGINT COMMENT '订单明细唯一标识',
  order_id BIGINT COMMENT '所属订单唯一标识',
  sku_id BIGINT COMMENT '成交SKU唯一标识',
  sku_num INT COMMENT '购买商品数量',
  original_amount DECIMAL(20,2) COMMENT '优惠前明细金额，单位人民币元',
  final_amount DECIMAL(20,2) COMMENT '优惠分摊后的明细成交金额，单位人民币元',
  create_time TIMESTAMP COMMENT '订单明细创建时间'
)
COMMENT 'ODS订单商品明细日增量表，一行代表一个订单商品明细'
PARTITIONED BY (ds STRING COMMENT '按create_time归属的业务日期，格式yyyyMMdd')
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS ods.ods_payment_info_di (
  payment_id BIGINT COMMENT '支付尝试唯一标识',
  order_id BIGINT COMMENT '对应订单唯一标识',
  user_id BIGINT COMMENT '支付用户唯一标识',
  payment_type STRING COMMENT '支付方式：ALIPAY、WECHAT、CARD',
  payment_status STRING COMMENT '支付状态：SUCCESS、FAILED',
  payment_amount DECIMAL(20,2) COMMENT '支付金额，单位人民币元',
  payment_time TIMESTAMP COMMENT '支付业务时间',
  create_time TIMESTAMP COMMENT '支付记录创建时间'
)
COMMENT 'ODS支付尝试日增量表，一行代表一次支付尝试'
PARTITIONED BY (ds STRING COMMENT '按payment_time归属的业务日期，格式yyyyMMdd')
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS ods.ods_user_behavior_di (
  event_id BIGINT COMMENT '行为事件唯一标识',
  session_id STRING COMMENT '用户会话唯一标识',
  user_id BIGINT COMMENT '行为用户唯一标识',
  sku_id BIGINT COMMENT '行为关联SKU唯一标识',
  event_type STRING COMMENT '行为类型：view、favorite、cart',
  event_time TIMESTAMP COMMENT '用户行为发生时间'
)
COMMENT 'ODS用户行为日增量表，一行代表一次用户行为'
PARTITIONED BY (ds STRING COMMENT '按event_time归属的业务日期，格式yyyyMMdd')
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS ods.ods_rating_info_di (
  rating_id BIGINT COMMENT '评分唯一标识',
  order_id BIGINT COMMENT '被评价订单唯一标识',
  shop_id BIGINT COMMENT '被评价店铺唯一标识',
  shop_score TINYINT COMMENT '店铺评分，取值1至5',
  rating_time TIMESTAMP COMMENT '评分发生时间'
)
COMMENT 'ODS店铺评分日增量表，一行代表一个订单店铺评分'
PARTITIONED BY (ds STRING COMMENT '按rating_time归属的业务日期，格式yyyyMMdd')
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');
