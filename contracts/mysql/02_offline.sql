-- C11 MySQL离线结果表DDL
-- 离线结果保留历史ds；stat_period固定为1d、7d、30d、all。
-- 所有索引均内联在对应CREATE TABLE中，不维护独立索引脚本。

SET NAMES utf8mb4;

CREATE DATABASE IF NOT EXISTS offline
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS offline.ads_biz_overview_df (
  ds CHAR(8) NOT NULL COMMENT '展示截止日期，格式yyyyMMdd',
  stat_period VARCHAR(8) NOT NULL COMMENT '统计范围：1d、7d、30d、all',
  pv_count BIGINT NOT NULL DEFAULT 0 COMMENT '行为事件次数',
  uv_count BIGINT NOT NULL DEFAULT 0 COMMENT '去重浏览用户数',
  favorite_count BIGINT NOT NULL DEFAULT 0 COMMENT '收藏行为次数',
  cart_count BIGINT NOT NULL DEFAULT 0 COMMENT '加购行为次数',
  order_count BIGINT NOT NULL DEFAULT 0 COMMENT '创建去重订单数',
  paid_order_count BIGINT NOT NULL DEFAULT 0 COMMENT '成功支付去重订单数',
  paid_user_count BIGINT NOT NULL DEFAULT 0 COMMENT '成功支付去重用户数',
  paid_sku_num BIGINT NOT NULL DEFAULT 0 COMMENT '成功支付商品件数',
  gmv DECIMAL(20,2) NOT NULL DEFAULT 0 COMMENT '成功支付订单payment_amount合计',
  avg_order_amount DECIMAL(20,2) NOT NULL DEFAULT 0 COMMENT 'gmv除以paid_order_count',
  view_to_pay_rate DECIMAL(10,6) NOT NULL DEFAULT 0 COMMENT '支付用户数除以浏览用户数',
  PRIMARY KEY (ds, stat_period),
  KEY idx_offline_overview_period (stat_period, ds),
  CONSTRAINT chk_offline_overview_ds CHECK (ds REGEXP '^[0-9]{8}$'),
  CONSTRAINT chk_offline_overview_period CHECK (stat_period IN ('1d', '7d', '30d', 'all')),
  CONSTRAINT chk_offline_overview_counts CHECK (
    pv_count >= 0 AND uv_count >= 0 AND favorite_count >= 0 AND cart_count >= 0
    AND order_count >= 0 AND paid_order_count >= 0 AND paid_user_count >= 0
    AND paid_sku_num >= 0
  ),
  CONSTRAINT chk_offline_overview_amount CHECK (gmv >= 0 AND avg_order_amount >= 0),
  CONSTRAINT chk_offline_overview_rate CHECK (view_to_pay_rate BETWEEN 0 AND 1)
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='离线经营总览快照，一行代表一个截止日期和统计范围';

CREATE TABLE IF NOT EXISTS offline.ads_sku_topn_df (
  ds CHAR(8) NOT NULL COMMENT '展示截止日期，格式yyyyMMdd',
  stat_period VARCHAR(8) NOT NULL COMMENT '统计范围：1d、7d、30d、all',
  rank_type VARCHAR(16) NOT NULL COMMENT '排行类型：gmv、paid_sku_num、paid_order_count',
  rank_no TINYINT NOT NULL COMMENT '排名序号，1至10',
  sku_id BIGINT NOT NULL COMMENT 'SKU唯一标识',
  sku_name VARCHAR(128) NOT NULL COMMENT 'SKU名称快照',
  category_id BIGINT NOT NULL COMMENT '类目唯一标识',
  category_name VARCHAR(64) NOT NULL COMMENT '类目名称快照',
  shop_id BIGINT NOT NULL COMMENT '店铺唯一标识',
  shop_name VARCHAR(64) NOT NULL COMMENT '店铺名称快照',
  paid_order_count BIGINT NOT NULL DEFAULT 0 COMMENT '成功支付订单数',
  paid_user_count BIGINT NOT NULL DEFAULT 0 COMMENT '成功支付用户数',
  paid_sku_num BIGINT NOT NULL DEFAULT 0 COMMENT '成功支付商品件数',
  gmv DECIMAL(20,2) NOT NULL DEFAULT 0 COMMENT 'SKU明细final_amount合计',
  PRIMARY KEY (ds, stat_period, rank_type, rank_no),
  UNIQUE KEY uk_offline_sku_rank_entity (ds, stat_period, rank_type, sku_id),
  KEY idx_offline_sku_history (sku_id, ds, stat_period),
  CONSTRAINT chk_offline_sku_ds CHECK (ds REGEXP '^[0-9]{8}$'),
  CONSTRAINT chk_offline_sku_period CHECK (stat_period IN ('1d', '7d', '30d', 'all')),
  CONSTRAINT chk_offline_sku_rank_type CHECK (
    rank_type IN ('gmv', 'paid_sku_num', 'paid_order_count')
  ),
  CONSTRAINT chk_offline_sku_rank_no CHECK (rank_no BETWEEN 1 AND 10),
  CONSTRAINT chk_offline_sku_metric CHECK (
    paid_order_count >= 0 AND paid_user_count >= 0 AND paid_sku_num >= 0 AND gmv >= 0
  )
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='离线SKU销售排行快照，一行代表一个统计范围的一个名次';

CREATE TABLE IF NOT EXISTS offline.ads_shop_rating_df (
  ds CHAR(8) NOT NULL COMMENT '展示截止日期，格式yyyyMMdd',
  stat_period VARCHAR(8) NOT NULL COMMENT '统计范围：1d、7d、30d、all',
  shop_id BIGINT NOT NULL COMMENT '店铺唯一标识',
  rating_count BIGINT NOT NULL DEFAULT 0 COMMENT '有效评分记录数',
  avg_score DECIMAL(10,6) NOT NULL DEFAULT 0 COMMENT '店铺平均评分',
  good_count BIGINT NOT NULL DEFAULT 0 COMMENT '好评数，4至5分',
  mid_count BIGINT NOT NULL DEFAULT 0 COMMENT '中评数，3分',
  bad_count BIGINT NOT NULL DEFAULT 0 COMMENT '差评数，1至2分',
  good_rate DECIMAL(10,6) NOT NULL DEFAULT 0 COMMENT '好评率',
  bad_rate DECIMAL(10,6) NOT NULL DEFAULT 0 COMMENT '差评率',
  wilson_good_rate DECIMAL(10,6) NOT NULL DEFAULT 0 COMMENT 'Wilson好评率下限',
  risk_flag TINYINT NOT NULL DEFAULT 0 COMMENT '风险标记：rating_count>=20且bad_rate>=0.30为1',
  PRIMARY KEY (ds, stat_period, shop_id),
  KEY idx_offline_rating_period (stat_period, ds),
  KEY idx_offline_rating_risk (ds, stat_period, risk_flag, shop_id),
  CONSTRAINT chk_offline_rating_ds CHECK (ds REGEXP '^[0-9]{8}$'),
  CONSTRAINT chk_offline_rating_period CHECK (stat_period IN ('1d', '7d', '30d', 'all')),
  CONSTRAINT chk_offline_rating_counts CHECK (
    rating_count = good_count + mid_count + bad_count
  ),
  CONSTRAINT chk_offline_rating_avg CHECK (avg_score BETWEEN 0 AND 5),
  CONSTRAINT chk_offline_rating_good CHECK (good_rate BETWEEN 0 AND 1),
  CONSTRAINT chk_offline_rating_bad CHECK (bad_rate BETWEEN 0 AND 1),
  CONSTRAINT chk_offline_rating_wilson CHECK (wilson_good_rate BETWEEN 0 AND 1),
  CONSTRAINT chk_offline_rating_risk CHECK (risk_flag IN (0, 1))
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='离线店铺评分快照，一行代表一个截止日期、统计范围和店铺';
