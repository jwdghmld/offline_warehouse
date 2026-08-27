-- Hive DWS DDL. Each target snapshot contains 1d, 7d, 30d and all ranges.

CREATE DATABASE IF NOT EXISTS cdm COMMENT '电商离线数仓公共数据模型层';

CREATE EXTERNAL TABLE IF NOT EXISTS cdm.dws_site_stats_df (
  stat_period STRING COMMENT '统计范围：1d、7d、30d、all',
  pv_count BIGINT COMMENT '行为事件次数',
  uv_count BIGINT COMMENT '浏览行为去重用户数',
  favorite_count BIGINT COMMENT '收藏行为次数',
  cart_count BIGINT COMMENT '加购行为次数',
  order_count BIGINT COMMENT '创建订单去重订单数',
  paid_order_count BIGINT COMMENT '成功支付去重订单数',
  paid_user_count BIGINT COMMENT '成功支付去重用户数',
  paid_sku_num BIGINT COMMENT '成功支付商品件数',
  gmv DECIMAL(20,2) COMMENT '订单级成功支付成交总额，单位人民币元'
)
COMMENT 'DWS全站经营统计完整快照表，粒度为ds加stat_period'
PARTITIONED BY (ds STRING COMMENT '统计截止日，格式yyyyMMdd')
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS cdm.dws_sku_trade_df (
  stat_period STRING COMMENT '统计范围：1d、7d、30d、all',
  sku_id BIGINT COMMENT 'SKU唯一标识',
  sku_name STRING COMMENT 'SKU名称',
  category_id BIGINT COMMENT 'SKU所属类目唯一标识',
  category_name STRING COMMENT 'SKU所属类目名称',
  shop_id BIGINT COMMENT 'SKU所属店铺唯一标识',
  shop_name STRING COMMENT 'SKU所属店铺名称',
  pv_count BIGINT COMMENT 'SKU浏览行为次数',
  visitor_count BIGINT COMMENT 'SKU浏览去重用户数',
  cart_user_count BIGINT COMMENT 'SKU加购去重用户数',
  paid_order_count BIGINT COMMENT 'SKU成功支付去重订单数',
  paid_user_count BIGINT COMMENT 'SKU成功支付去重用户数',
  paid_sku_num BIGINT COMMENT 'SKU成功支付商品件数',
  gmv DECIMAL(20,2) COMMENT 'SKU明细成交总额，单位人民币元'
)
COMMENT 'DWS商品交易统计完整快照表，粒度为ds加stat_period加sku_id'
PARTITIONED BY (ds STRING COMMENT '统计截止日，格式yyyyMMdd')
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS cdm.dws_shop_rating_df (
  stat_period STRING COMMENT '统计范围：1d、7d、30d、all',
  shop_id BIGINT COMMENT '店铺唯一标识',
  shop_name STRING COMMENT '店铺名称',
  rating_count BIGINT COMMENT '有效店铺评分数',
  score_sum BIGINT COMMENT '店铺评分总和',
  good_count BIGINT COMMENT '好评数，评分4至5分',
  mid_count BIGINT COMMENT '中评数，评分3分',
  bad_count BIGINT COMMENT '差评数，评分1至2分'
)
COMMENT 'DWS店铺评分统计完整快照表，粒度为ds加stat_period加shop_id'
PARTITIONED BY (ds STRING COMMENT '统计截止日，格式yyyyMMdd')
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');
