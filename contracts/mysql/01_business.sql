-- C03 MySQL业务源表DDL
-- 最新大纲：2张冻结维表、5张每日覆盖事实表。
-- 所有索引均内联在对应CREATE TABLE中，不维护独立索引脚本。

SET NAMES utf8mb4;

CREATE DATABASE IF NOT EXISTS ecommerce_business
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS ecommerce_business.shop_info (
  shop_id BIGINT NOT NULL COMMENT '店铺ID',
  shop_name VARCHAR(64) NOT NULL COMMENT '店铺名称',
  province_name VARCHAR(32) NOT NULL COMMENT '店铺省份',
  open_time DATETIME NOT NULL COMMENT '开店时间',
  status TINYINT NOT NULL COMMENT '店铺状态：1正常、0停用',
  PRIMARY KEY (shop_id),
  KEY idx_shop_status (status, shop_id),
  KEY idx_shop_province (province_name, shop_id),
  CONSTRAINT chk_shop_status CHECK (status IN (0, 1))
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='冻结店铺维度，一行代表一个店铺';

CREATE TABLE IF NOT EXISTS ecommerce_business.sku_info (
  sku_id BIGINT NOT NULL COMMENT 'SKU ID',
  sku_name VARCHAR(128) NOT NULL COMMENT '商品名称',
  category_id BIGINT NOT NULL COMMENT '类目ID',
  category_name VARCHAR(64) NOT NULL COMMENT '类目名称',
  shop_id BIGINT NOT NULL COMMENT '所属店铺ID',
  price DECIMAL(10,2) NOT NULL COMMENT '标准单价',
  status TINYINT NOT NULL COMMENT 'SKU状态：1上架、0下架',
  create_time DATETIME(3) NOT NULL COMMENT '创建时间',
  update_time DATETIME(3) NOT NULL COMMENT '更新时间',
  PRIMARY KEY (sku_id),
  KEY idx_sku_shop (shop_id, sku_id),
  KEY idx_sku_category (category_id, sku_id),
  KEY idx_sku_status (status, sku_id),
  CONSTRAINT chk_sku_price CHECK (price >= 0),
  CONSTRAINT chk_sku_status CHECK (status IN (0, 1)),
  CONSTRAINT chk_sku_update_time CHECK (update_time >= create_time)
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='冻结SKU维度，一行代表一个SKU';

CREATE TABLE IF NOT EXISTS ecommerce_business.order_info (
  order_id BIGINT NOT NULL COMMENT '订单ID',
  user_id BIGINT NOT NULL COMMENT '下单用户ID',
  shop_id BIGINT NOT NULL COMMENT '下单店铺ID',
  order_status VARCHAR(16) NOT NULL COMMENT '订单状态：CREATED、PAID、CANCELLED',
  order_amount DECIMAL(20,2) NOT NULL COMMENT '明细分摊成交额之和',
  create_time DATETIME(3) NOT NULL COMMENT '下单业务时间',
  update_time DATETIME(3) NOT NULL COMMENT '最后状态更新时间',
  PRIMARY KEY (order_id),
  KEY idx_order_shop (shop_id, order_id),
  KEY idx_order_user (user_id, order_id),
  KEY idx_order_create_time (create_time, order_id),
  KEY idx_order_update_time (update_time, order_id),
  CONSTRAINT chk_order_status CHECK (
    order_status IN ('CREATED', 'PAID', 'CANCELLED')
  ),
  CONSTRAINT chk_order_amount CHECK (order_amount >= 0),
  CONSTRAINT chk_order_update_time CHECK (update_time >= create_time)
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='每日订单事实表，一行代表一个单店铺订单';

CREATE TABLE IF NOT EXISTS ecommerce_business.order_detail (
  order_detail_id BIGINT NOT NULL COMMENT '明细ID',
  order_id BIGINT NOT NULL COMMENT '所属订单ID',
  sku_id BIGINT NOT NULL COMMENT 'SKU ID',
  sku_num INT NOT NULL COMMENT '购买件数，大于0',
  original_amount DECIMAL(20,2) NOT NULL COMMENT '原始金额',
  final_amount DECIMAL(20,2) NOT NULL COMMENT '分摊成交金额',
  create_time DATETIME(3) NOT NULL COMMENT '明细创建时间',
  PRIMARY KEY (order_detail_id),
  KEY idx_order_detail_order (order_id, order_detail_id),
  KEY idx_order_detail_sku (sku_id, order_detail_id),
  KEY idx_order_detail_create_time (create_time, order_detail_id),
  CONSTRAINT chk_order_detail_num CHECK (sku_num > 0),
  CONSTRAINT chk_order_detail_original CHECK (original_amount >= 0),
  CONSTRAINT chk_order_detail_final CHECK (
    final_amount >= 0 AND final_amount <= original_amount
  )
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='每日订单商品明细事实表，一行代表一个订单商品明细';

CREATE TABLE IF NOT EXISTS ecommerce_business.payment_info (
  payment_id BIGINT NOT NULL COMMENT '支付尝试ID',
  order_id BIGINT NOT NULL COMMENT '对应订单ID',
  user_id BIGINT NOT NULL COMMENT '支付用户ID，必须等于订单用户',
  payment_type VARCHAR(16) NOT NULL COMMENT '支付方式：ALIPAY、WECHAT、CARD',
  payment_status VARCHAR(16) NOT NULL COMMENT '支付状态：SUCCESS、FAILED',
  payment_amount DECIMAL(20,2) NOT NULL COMMENT '支付金额，成功时通常等于订单金额',
  payment_time DATETIME(3) NOT NULL COMMENT '支付业务时间',
  create_time DATETIME(3) NOT NULL COMMENT '支付记录创建时间',
  PRIMARY KEY (payment_id),
  UNIQUE KEY uk_payment_success_order (
    (CASE WHEN payment_status = 'SUCCESS' THEN order_id ELSE NULL END)
  ),
  KEY idx_payment_order_time (order_id, payment_time, payment_id),
  KEY idx_payment_user_time (user_id, payment_time, payment_id),
  KEY idx_payment_status_time (payment_status, payment_time, payment_id),
  CONSTRAINT chk_payment_type CHECK (
    payment_type IN ('ALIPAY', 'WECHAT', 'CARD')
  ),
  CONSTRAINT chk_payment_status CHECK (
    payment_status IN ('SUCCESS', 'FAILED')
  ),
  CONSTRAINT chk_payment_amount CHECK (
    (payment_status = 'SUCCESS' AND payment_amount > 0)
    OR (payment_status = 'FAILED' AND payment_amount >= 0)
  ),
  CONSTRAINT chk_payment_create_time CHECK (create_time >= payment_time)
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='每日支付事实表，一行代表一次支付尝试';

CREATE TABLE IF NOT EXISTS ecommerce_business.user_behavior (
  event_id BIGINT NOT NULL COMMENT '行为事件ID',
  session_id VARCHAR(64) NOT NULL COMMENT '会话ID',
  user_id BIGINT NOT NULL COMMENT '用户ID',
  sku_id BIGINT NOT NULL COMMENT '对应SKU ID',
  event_type VARCHAR(16) NOT NULL COMMENT '行为类型：view、favorite、cart',
  event_time DATETIME(3) NOT NULL COMMENT '行为业务时间',
  PRIMARY KEY (event_id),
  KEY idx_behavior_time (event_time, event_id),
  KEY idx_behavior_user_time (user_id, event_time, event_id),
  KEY idx_behavior_sku_time (sku_id, event_time, event_id),
  CONSTRAINT chk_behavior_type CHECK (
    event_type IN ('view', 'favorite', 'cart')
  )
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='每日用户行为事实表，一行代表一次用户行为，仅服务离线';

CREATE TABLE IF NOT EXISTS ecommerce_business.rating_info (
  rating_id BIGINT NOT NULL COMMENT '评分ID',
  order_id BIGINT NOT NULL COMMENT '被评分成功订单ID',
  shop_id BIGINT NOT NULL COMMENT '被评分店铺ID，必须等于订单店铺',
  shop_score TINYINT NOT NULL COMMENT '店铺评分，1至5分',
  rating_time DATETIME(3) NOT NULL COMMENT '评分业务时间',
  PRIMARY KEY (rating_id),
  UNIQUE KEY uk_rating_order (order_id),
  KEY idx_rating_shop_time (shop_id, rating_time, rating_id),
  KEY idx_rating_time (rating_time, rating_id),
  CONSTRAINT chk_rating_score CHECK (shop_score BETWEEN 1 AND 5)
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='每日店铺评分事实表，一行代表一个成功订单的店铺评分';

-- 每日事实覆盖由造数器在一个InnoDB事务中执行：
-- DELETE rating_info -> payment_info -> order_detail -> order_info -> user_behavior，
-- 再按order_info -> order_detail -> payment_info -> rating_info -> user_behavior插入。
-- 少量时序或金额异常由造数器受控注入，因此DDL不约束跨表时间和金额等式。
-- 表之间只保留逻辑关联，不创建FOREIGN KEY；引用完整性由造数器和质量校验保证。
-- 所有索引均已内联声明。
