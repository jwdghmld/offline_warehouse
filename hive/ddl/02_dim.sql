-- Hive DIM DDL. The two dimension tables are initialized once and then frozen.

CREATE DATABASE IF NOT EXISTS cdm COMMENT '电商离线数仓公共数据模型层';

CREATE EXTERNAL TABLE IF NOT EXISTS cdm.dim_shop_df (
  shop_id BIGINT COMMENT '店铺唯一标识',
  shop_name STRING COMMENT '店铺名称',
  province_name STRING COMMENT '店铺所在省份名称',
  open_time TIMESTAMP COMMENT '店铺开店时间',
  status TINYINT COMMENT '店铺状态'
)
COMMENT 'DIM店铺完整快照表，初始化后冻结，粒度为shop_id'
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS cdm.dim_sku_df (
  sku_id BIGINT COMMENT 'SKU唯一标识',
  sku_name STRING COMMENT 'SKU名称',
  category_id BIGINT COMMENT 'SKU所属类目唯一标识',
  category_name STRING COMMENT 'SKU所属类目名称',
  shop_id BIGINT COMMENT 'SKU所属店铺唯一标识',
  shop_name STRING COMMENT 'SKU所属店铺名称',
  province_name STRING COMMENT '店铺所在省份名称',
  price DECIMAL(20,2) COMMENT 'SKU销售价格，单位人民币元',
  status TINYINT COMMENT 'SKU上下架状态：0下架、1上架'
)
COMMENT 'DIM商品完整快照表，初始化后冻结，粒度为sku_id'
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');
