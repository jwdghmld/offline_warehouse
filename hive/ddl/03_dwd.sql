-- Hive DWD DDL. Each ds partition is a complete cleaned snapshot through ds.

CREATE DATABASE IF NOT EXISTS cdm COMMENT '电商离线数仓公共数据模型层';

CREATE EXTERNAL TABLE IF NOT EXISTS cdm.dwd_trd_pay_dtl_df (
  order_detail_id BIGINT COMMENT '订单明细唯一标识',
  order_id BIGINT COMMENT '订单唯一标识',
  payment_id BIGINT COMMENT '成功支付记录唯一标识',
  user_id BIGINT COMMENT '下单用户唯一标识',
  shop_id BIGINT COMMENT '订单所属店铺唯一标识',
  shop_name STRING COMMENT '订单所属店铺名称',
  sku_id BIGINT COMMENT '成交SKU唯一标识',
  sku_name STRING COMMENT '成交SKU名称',
  category_id BIGINT COMMENT '成交SKU所属类目唯一标识',
  category_name STRING COMMENT '成交SKU所属类目名称',
  sku_num INT COMMENT '购买商品数量',
  original_amount DECIMAL(20,2) COMMENT '优惠前明细金额，单位人民币元',
  final_amount DECIMAL(20,2) COMMENT '优惠分摊后的明细成交金额，单位人民币元',
  order_amount DECIMAL(20,2) COMMENT '订单金额，单位人民币元',
  payment_amount DECIMAL(20,2) COMMENT '成功支付金额，单位人民币元',
  create_time TIMESTAMP COMMENT '订单创建时间',
  payment_time TIMESTAMP COMMENT '成功支付业务时间',
  order_date DATE COMMENT '订单创建日期',
  payment_date DATE COMMENT '成功支付日期'
)
COMMENT 'DWD成功支付订单明细完整快照表，粒度为ds加order_detail_id'
PARTITIONED BY (ds STRING COMMENT '快照截止日，格式yyyyMMdd')
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS cdm.dwd_user_behavior_df (
  event_id BIGINT COMMENT '行为事件唯一标识',
  session_id STRING COMMENT '用户会话唯一标识',
  user_id BIGINT COMMENT '行为用户唯一标识',
  event_type STRING COMMENT '行为类型：view、favorite、cart',
  sku_id BIGINT COMMENT '行为关联SKU唯一标识',
  sku_name STRING COMMENT '行为关联SKU名称',
  category_id BIGINT COMMENT '行为关联类目唯一标识',
  category_name STRING COMMENT '行为关联类目名称',
  shop_id BIGINT COMMENT '行为关联店铺唯一标识',
  shop_name STRING COMMENT '行为关联店铺名称',
  event_time TIMESTAMP COMMENT '用户行为发生时间',
  event_date DATE COMMENT '用户行为发生日期'
)
COMMENT 'DWD用户有效行为完整快照表，粒度为ds加event_id'
PARTITIONED BY (ds STRING COMMENT '快照截止日，格式yyyyMMdd')
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS cdm.dwd_shop_rating_df (
  rating_id BIGINT COMMENT '评分唯一标识',
  order_id BIGINT COMMENT '被评价订单唯一标识',
  shop_id BIGINT COMMENT '被评价店铺唯一标识',
  shop_name STRING COMMENT '被评价店铺名称',
  shop_score TINYINT COMMENT '店铺评分，取值1至5',
  score_level STRING COMMENT '评分分类：good、mid、bad',
  rating_time TIMESTAMP COMMENT '评分发生时间',
  rating_date DATE COMMENT '评分发生日期'
)
COMMENT 'DWD有效店铺评分完整快照表，粒度为ds加rating_id'
PARTITIONED BY (ds STRING COMMENT '快照截止日，格式yyyyMMdd')
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');
