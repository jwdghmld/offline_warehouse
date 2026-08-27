-- Hive ADS DDL. Result snapshots are published to MySQL offline tables.

CREATE DATABASE IF NOT EXISTS ads COMMENT '电商离线数仓ADS应用数据层';

CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_biz_overview_df (
  stat_period STRING COMMENT '统计范围：1d、7d、30d、all',
  pv_count BIGINT COMMENT '行为事件次数',
  uv_count BIGINT COMMENT '浏览行为去重用户数',
  favorite_count BIGINT COMMENT '收藏行为次数',
  cart_count BIGINT COMMENT '加购行为次数',
  order_count BIGINT COMMENT '创建订单去重订单数',
  paid_order_count BIGINT COMMENT '成功支付去重订单数',
  paid_user_count BIGINT COMMENT '成功支付去重用户数',
  paid_sku_num BIGINT COMMENT '成功支付商品件数',
  gmv DECIMAL(20,2) COMMENT '订单级成功支付成交总额，单位人民币元',
  avg_order_amount DECIMAL(20,2) COMMENT '支付客单价，分母为0时取0',
  view_to_pay_rate DECIMAL(10,6) COMMENT '浏览到支付转化率，分母为0时取0'
)
COMMENT 'ADS经营总览展示快照表，粒度为ds加stat_period'
PARTITIONED BY (ds STRING COMMENT '展示截止日，格式yyyyMMdd')
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_sku_topn_df (
  stat_period STRING COMMENT '统计范围：1d、7d、30d、all',
  rank_type STRING COMMENT '排行类型：gmv、paid_sku_num、paid_order_count',
  rank_no INT COMMENT '当前排行类型内的连续名次，从1开始',
  sku_id BIGINT COMMENT '排行SKU唯一标识',
  sku_name STRING COMMENT '排行SKU名称',
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
COMMENT 'ADS商品TopN展示快照表，粒度为ds加stat_period加rank_type加rank_no'
PARTITIONED BY (ds STRING COMMENT '展示截止日，格式yyyyMMdd')
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_shop_rating_df (
  stat_period STRING COMMENT '统计范围：1d、7d、30d、all',
  shop_id BIGINT COMMENT '店铺唯一标识',
  shop_name STRING COMMENT '店铺名称',
  rating_count BIGINT COMMENT '有效店铺评分数',
  avg_score DECIMAL(10,6) COMMENT '店铺平均评分，分母为0时取0',
  good_count BIGINT COMMENT '好评数，评分4至5分',
  mid_count BIGINT COMMENT '中评数，评分3分',
  bad_count BIGINT COMMENT '差评数，评分1至2分',
  good_rate DECIMAL(10,6) COMMENT '店铺好评率，分母为0时取0',
  bad_rate DECIMAL(10,6) COMMENT '店铺差评率，分母为0时取0',
  wilson_good_rate DECIMAL(10,6) COMMENT 'Wilson好评率95%置信下限',
  risk_flag BOOLEAN COMMENT '风险标记：rating_count不少于20且bad_rate不少于0.30时为true'
)
COMMENT 'ADS店铺评分展示快照表，粒度为ds加stat_period加shop_id'
PARTITIONED BY (ds STRING COMMENT '展示截止日，格式yyyyMMdd')
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');
