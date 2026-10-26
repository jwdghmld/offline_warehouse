#!/usr/bin/env python3
"""Batch 1: generate and freeze shop_info and sku_info."""

from __future__ import annotations

import hashlib
import json
import os
import random
from dataclasses import dataclass, fields
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Iterable


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "default.json"
MONEY = Decimal("0.01")


# 1. Configuration and row models
@dataclass(frozen=True)
class GeneratorConfig:
    base_seed: int
    profile: str
    shop_count: int
    sku_count: int
    category_count: int
    mysql: dict[str, Any]

    @classmethod
    def load(cls, path: Path | None = None) -> "GeneratorConfig":
        config_path = path or Path(
            os.environ.get("GENERATOR_CONFIG", str(DEFAULT_CONFIG_PATH))
        )
        with config_path.resolve().open("r", encoding="utf-8") as handle:
            raw = json.load(handle)
        dimension = raw["dimensions"]
        mysql = dict(raw["mysql"])
        for key, env_name in (
            ("host", "MYSQL_HOST"),
            ("port", "MYSQL_PORT"),
            ("user", "MYSQL_USER"),
            ("password", "MYSQL_PASSWORD"),
            ("database", "ECOMMERCE_MYSQL_DATABASE"),
        ):
            if env_name in os.environ:
                mysql[key] = os.environ[env_name]
        return cls(
            base_seed=int(raw.get("base_seed", 20260826)),
            profile=str(raw.get("profile", "formal")),
            shop_count=int(dimension["shops"]),
            sku_count=int(dimension["skus"]),
            category_count=int(dimension["categories"]),
            mysql=mysql,
        )


@dataclass(frozen=True)
class ShopInfo:
    shop_id: int
    shop_name: str
    province_name: str
    open_time: datetime
    status: int


@dataclass(frozen=True)
class SkuInfo:
    sku_id: int
    sku_name: str
    category_id: int
    category_name: str
    shop_id: int
    price: Decimal
    status: int
    create_time: datetime
    update_time: datetime


@dataclass
class DimensionData:
    shops: list[ShopInfo]
    skus: list[SkuInfo]


@dataclass(frozen=True)
class CategoryFamily:
    department: str
    shop_keyword: str
    brands: tuple[str, ...]
    price_range: tuple[int, int]
    products: tuple[str, ...]
    specifications: tuple[str, ...]


@dataclass(frozen=True)
class CategoryOption:
    category_id: int
    family_index: int
    category_name: str
    product_name: str
    price_range: tuple[int, int]
    brands: tuple[str, ...]
    specifications: tuple[str, ...]


# 2. Deterministic helpers
def money(value: Decimal | float | int | str) -> Decimal:
    return Decimal(str(value)).quantize(MONEY, rounding=ROUND_HALF_UP)


def stable_seed(base_seed: int, namespace: str, entity_id: int) -> int:
    digest = hashlib.sha256(
        f"{base_seed}:{namespace}:{entity_id}".encode("ascii")
    ).digest()
    return int.from_bytes(digest[:8], "big")


def row_values(row: Any) -> tuple[Any, ...]:
    return tuple(getattr(row, item.name) for item in fields(row))


def ensure_unique(name: str, values: Iterable[Any]) -> None:
    rows = list(values)
    if len(rows) != len(set(rows)):
        raise ValueError(f"{name} contains duplicate keys")


PROVINCES = (
    "北京", "上海", "广东", "浙江", "江苏", "四川", "湖北", "湖南",
    "山东", "河南", "福建", "安徽", "河北", "陕西", "重庆", "辽宁",
)

SHOP_NAME_TEMPLATES = (
    "{brand}{keyword}旗舰店",
    "{brand}{keyword}官方店",
    "{province}{brand}{keyword}专营店",
    "{brand}{province}{keyword}生活馆",
    "{province}{keyword}优选店",
    "{brand}{keyword}体验中心",
    "{province}{brand}{keyword}直营店",
    "{brand}{keyword}集合店",
    "{province}{keyword}仓储店",
    "{brand}{province}{keyword}品牌店",
)

# All brands below are fictional. Each family contributes ten natural subcategories,
# which gives the formal profile its configured 200 category names.
CATEGORY_FAMILIES = (
    CategoryFamily("手机通讯", "通讯", ("北辰", "澄光", "远航", "星瀚"), (499, 7999),
                   ("智能手机", "折叠屏手机", "老人手机", "智能手表", "智能手环", "儿童电话手表", "平板电脑", "电子阅读器", "对讲机", "车载导航仪"),
                   ("曜石黑", "晨雾白", "远峰蓝", "256GB", "长续航版")),
    CategoryFamily("电脑办公", "电脑", ("凌云", "拓维", "云迹", "蓝拓"), (299, 9999),
                   ("轻薄笔记本", "游戏笔记本", "台式电脑", "机械键盘", "无线鼠标", "显示器", "打印机", "扫描仪", "移动硬盘", "路由器"),
                   ("标准版", "商务版", "专业版", "深空灰", "静音版")),
    CategoryFamily("数码影音", "数码", ("声屿", "澄光", "极影", "朗月"), (79, 3999),
                   ("无线蓝牙耳机", "头戴式耳机", "便携音箱", "运动相机", "数码相机", "微单相机", "投影仪", "录音笔", "智能摄像头", "无人机"),
                   ("降噪版", "长续航版", "曜石黑", "月光白", "旅行套装")),
    CategoryFamily("大家电", "家电", ("暖森", "清川", "沐风", "朗月"), (399, 8999),
                   ("空调", "冰箱", "洗衣机", "电视机", "热水器", "油烟机", "燃气灶", "洗碗机", "烘干机", "空气净化器"),
                   ("节能款", "静音款", "智能版", "大容量", "云雾白")),
    CategoryFamily("厨房电器", "厨电", ("食光", "暖森", "清川", "原野"), (69, 2999),
                   ("电饭煲", "破壁机", "空气炸锅", "电烤箱", "咖啡机", "电热水壶", "电磁炉", "料理机", "榨汁机", "多功能锅"),
                   ("家用版", "智能版", "轻享款", "奶油白", "大容量")),
    CategoryFamily("家具家装", "家居", ("木言", "栖居", "简境", "云舍"), (79, 5999),
                   ("布艺沙发", "实木餐桌", "书桌", "人体工学椅", "衣柜", "床头柜", "落地灯", "吸顶灯", "窗帘", "装饰画"),
                   ("原木色", "胡桃色", "简约款", "加厚款", "组合装")),
    CategoryFamily("家纺家饰", "家纺", ("棉语", "云栖", "暖居", "初棉"), (29, 699),
                   ("四件套", "乳胶枕", "羽绒被", "夏凉被", "毛毯", "浴巾", "地毯", "抱枕", "收纳盒", "香薰蜡烛"),
                   ("1.5米床", "1.8米床", "加厚款", "柔软款", "浅灰色")),
    CategoryFamily("个护清洁", "个护", ("净屿", "清川", "柔棉", "沐语"), (19, 599),
                   ("电动牙刷", "吹风机", "剃须刀", "冲牙器", "洗衣凝珠", "洗衣液", "抽纸", "湿巾", "洗手液", "除菌喷雾"),
                   ("家庭装", "旅行装", "清新款", "温和型", "升级版")),
    CategoryFamily("美妆护肤", "美妆", ("花间", "润泽", "初颜", "白昼"), (29, 899),
                   ("洁面乳", "精华液", "保湿面霜", "防晒霜", "面膜", "粉底液", "口红", "眼影盘", "卸妆水", "香水"),
                   ("自然色", "滋润款", "敏感肌适用", "30ml", "礼盒装")),
    CategoryFamily("女装", "女装", ("云裳", "绮遇", "轻语", "鹿鸣"), (69, 999),
                   ("连衣裙", "针织衫", "风衣", "半身裙", "牛仔裤", "衬衫", "卫衣", "羽绒服", "西装外套", "家居服"),
                   ("米白色", "烟灰色", "M码", "L码", "秋冬款")),
    CategoryFamily("男装", "男装", ("行川", "简行", "北陆", "远野"), (69, 999),
                   ("休闲衬衫", "圆领T恤", "连帽卫衣", "夹克", "牛仔裤", "休闲裤", "羽绒服", "针织开衫", "西装", "家居服"),
                   ("藏青色", "深灰色", "L码", "XL码", "商务款")),
    CategoryFamily("鞋靴箱包", "鞋包", ("风驰", "行旅", "漫步", "山川"), (79, 1299),
                   ("跑步鞋", "休闲鞋", "篮球鞋", "短靴", "单肩包", "双肩包", "行李箱", "钱包", "运动袜", "皮带"),
                   ("黑灰色", "42码", "43码", "轻量款", "升级款")),
    CategoryFamily("运动户外", "运动", ("风驰", "山野", "逐光", "远行"), (59, 1499),
                   ("运动T恤", "瑜伽垫", "哑铃", "跳绳", "露营帐篷", "折叠椅", "登山包", "保温杯", "骑行头盔", "羽毛球拍"),
                   ("专业版", "轻量款", "曜石黑", "双人装", "大容量")),
    CategoryFamily("母婴玩具", "母婴", ("芽芽", "童趣", "小树", "晴天"), (29, 999),
                   ("纸尿裤", "婴儿湿巾", "奶瓶", "婴儿推车", "儿童餐椅", "积木", "拼图", "遥控车", "毛绒玩具", "儿童绘本"),
                   ("家庭装", "S码", "M码", "益智款", "礼盒装")),
    CategoryFamily("休闲食品", "食品", ("山野", "谷香", "食光", "甘田"), (9, 299),
                   ("混合坚果", "每日坚果", "牛肉干", "薯片", "饼干", "巧克力", "果冻", "话梅", "蛋糕", "燕麦片"),
                   ("250g", "500g", "原味", "分享装", "礼盒装")),
    CategoryFamily("生鲜果蔬", "生鲜", ("鲜田", "果园", "绿野", "海湾"), (9, 199),
                   ("红富士苹果", "进口橙子", "阳光玫瑰葡萄", "小番茄", "西兰花", "鲜鸡蛋", "牛排", "三文鱼", "鲜虾", "冷冻水饺"),
                   ("1kg", "500g", "精选装", "家庭装", "新鲜直达")),
    CategoryFamily("家居日用", "生活", ("宜居", "简物", "日常", "清居"), (9, 399),
                   ("保温饭盒", "玻璃水杯", "衣架", "垃圾桶", "拖把", "扫把", "晾衣架", "工具箱", "雨伞", "驱蚊液"),
                   ("家庭装", "加厚款", "大号", "简约款", "灰色")),
    CategoryFamily("图书文具", "文具", ("知页", "墨舟", "纸间", "星笔"), (9, 199),
                   ("文学图书", "儿童读物", "考试辅导书", "笔记本", "中性笔", "钢笔", "文件夹", "书包", "彩色铅笔", "地球仪"),
                   ("套装", "16K", "A5", "经典版", "礼盒装")),
    CategoryFamily("宠物生活", "宠物", ("爪印", "萌友", "小满", "欢宠"), (19, 499),
                   ("猫粮", "狗粮", "猫砂", "宠物零食", "宠物玩具", "猫抓板", "宠物牵引绳", "宠物窝", "宠物洗护", "自动喂食器"),
                   ("2kg", "成猫款", "成犬款", "家庭装", "智能版")),
    CategoryFamily("汽车用品", "车品", ("驰途", "远行", "路野", "星途"), (29, 999),
                   ("行车记录仪", "车载充电器", "汽车坐垫", "方向盘套", "车载香薰", "洗车液", "玻璃水", "应急启动电源", "车载吸尘器", "儿童安全座椅"),
                   ("通用款", "升级版", "黑色", "双接口", "大容量")),
)


def build_category_options(config: GeneratorConfig) -> list[CategoryOption]:
    maximum = sum(len(family.products) for family in CATEGORY_FAMILIES)
    if not 1 <= config.category_count <= maximum:
        raise ValueError(f"categories must be between 1 and {maximum}")
    family_count = min(config.category_count, config.shop_count, len(CATEGORY_FAMILIES))
    available = sum(
        len(CATEGORY_FAMILIES[index].products) for index in range(family_count)
    )
    if config.category_count > available:
        raise ValueError(
            "configured category count cannot be covered by the configured shop pool"
        )
    options: list[CategoryOption] = []
    level = 0
    while len(options) < config.category_count:
        for family_index in range(family_count):
            family = CATEGORY_FAMILIES[family_index]
            if level >= len(family.products):
                continue
            product_name = family.products[level]
            options.append(
                CategoryOption(
                    category_id=len(options) + 1,
                    family_index=family_index,
                    category_name=f"{family.department}-{product_name}",
                    product_name=product_name,
                    price_range=family.price_range,
                    brands=family.brands,
                    specifications=family.specifications,
                )
            )
            if len(options) == config.category_count:
                return options
        level += 1
    return options


def build_shop_name(
    rng: random.Random, province_name: str, family: CategoryFamily
) -> str:
    template = SHOP_NAME_TEMPLATES[rng.randrange(len(SHOP_NAME_TEMPLATES))]
    return template.format(
        brand=family.brands[rng.randrange(len(family.brands))],
        province=province_name,
        keyword=family.shop_keyword,
    )


# 3. Frozen dimension generation and in-memory validation
class DimensionGenerator:
    def __init__(self, config: GeneratorConfig) -> None:
        self.config = config

    def generate(self) -> DimensionData:
        categories = build_category_options(self.config)
        shops = self._generate_shops(categories)
        skus = self._generate_skus(shops, categories)
        validate_dimensions(DimensionData(shops, skus), self.config, categories)
        return DimensionData(shops, skus)

    def _shop_family_index(
        self, shop_id: int, family_indexes: tuple[int, ...]
    ) -> int:
        if shop_id <= len(family_indexes):
            return family_indexes[shop_id - 1]
        rng = random.Random(stable_seed(self.config.base_seed, "shop-family", shop_id))
        return family_indexes[rng.randrange(len(family_indexes))]

    def _generate_shops(self, categories: list[CategoryOption]) -> list[ShopInfo]:
        anchor = datetime(2026, 1, 1)
        family_indexes = tuple(sorted({category.family_index for category in categories}))
        rows: list[ShopInfo] = []
        for shop_id in range(1, self.config.shop_count + 1):
            rng = random.Random(stable_seed(self.config.base_seed, "shop", shop_id))
            province_name = PROVINCES[rng.randrange(len(PROVINCES))]
            family = CATEGORY_FAMILIES[
                self._shop_family_index(shop_id, family_indexes)
            ]
            rows.append(
                ShopInfo(
                    shop_id=shop_id,
                    shop_name=build_shop_name(rng, province_name, family),
                    province_name=province_name,
                    open_time=anchor - timedelta(days=rng.randint(30, 1800)),
                    status=0 if shop_id % 113 == 0 else 1,
                )
            )
        return rows

    def _generate_skus(
        self, shops: list[ShopInfo], categories: list[CategoryOption]
    ) -> list[SkuInfo]:
        anchor = datetime(2026, 1, 1)
        categories_by_family: dict[int, list[CategoryOption]] = {}
        for category in categories:
            categories_by_family.setdefault(category.family_index, []).append(category)
        family_indexes = tuple(sorted(categories_by_family))
        active_shops_by_family: dict[int, list[ShopInfo]] = {}
        for shop in shops:
            if shop.status == 1:
                family_index = self._shop_family_index(shop.shop_id, family_indexes)
                active_shops_by_family.setdefault(family_index, []).append(shop)
        if set(categories_by_family) - set(active_shops_by_family):
            raise ValueError("frozen shop pool has no active shop for a category family")
        rows: list[SkuInfo] = []
        for sku_id in range(1, self.config.sku_count + 1):
            rng = random.Random(stable_seed(self.config.base_seed, "sku", sku_id))
            category = categories[(sku_id - 1) % len(categories)]
            shops_for_category = active_shops_by_family[category.family_index]
            shop = shops_for_category[rng.randrange(len(shops_for_category))]
            create_time = anchor - timedelta(
                days=rng.randint(1, 1000),
                seconds=rng.randint(0, 86399),
                milliseconds=rng.randint(0, 999),
            )
            brand = category.brands[rng.randrange(len(category.brands))]
            specification = category.specifications[
                rng.randrange(len(category.specifications))
            ]
            rows.append(
                SkuInfo(
                    sku_id=sku_id,
                    sku_name=(
                        f"{brand}{category.product_name} {specification} "
                        f"型号{chr(65 + category.family_index)}{sku_id:05d}"
                    ),
                    category_id=category.category_id,
                    category_name=category.category_name,
                    shop_id=shop.shop_id,
                    price=money(
                        Decimal(rng.randint(*category.price_range))
                    ),
                    status=0 if sku_id % 157 == 0 else 1,
                    create_time=create_time,
                    update_time=create_time,
                )
            )
        return rows


def validate_dimensions(
    data: DimensionData,
    config: GeneratorConfig,
    categories: list[CategoryOption],
) -> None:
    if len(data.shops) != config.shop_count or len(data.skus) != config.sku_count:
        raise ValueError("dimension row count does not match configured frozen pool")
    ensure_unique("shop_info.shop_id", (row.shop_id for row in data.shops))
    ensure_unique("sku_info.sku_id", (row.sku_id for row in data.skus))
    shop_ids = {row.shop_id for row in data.shops}
    category_by_id = {category.category_id: category for category in categories}
    if len(category_by_id) != config.category_count:
        raise ValueError("configured category pool is incomplete")
    for sku in data.skus:
        if sku.shop_id not in shop_ids:
            raise ValueError(f"sku {sku.sku_id} references missing shop")
        if sku.price < 0 or sku.status not in (0, 1):
            raise ValueError(f"sku {sku.sku_id} has invalid values")
        category = category_by_id.get(sku.category_id)
        if category is None or sku.category_name != category.category_name:
            raise ValueError(f"sku {sku.sku_id} has invalid category data")
        if not category.price_range[0] <= sku.price <= category.price_range[1]:
            raise ValueError(f"sku {sku.sku_id} is outside its category price range")
        if sku.sku_name.startswith("商品-") or "型号" not in sku.sku_name:
            raise ValueError(f"sku {sku.sku_id} has placeholder name")
    if any(row.shop_name.startswith("店铺-") for row in data.shops):
        raise ValueError("shop dimension has placeholder name")


# 4. One-transaction MySQL replacement
def connect_mysql(config: GeneratorConfig) -> Any:
    try:
        import pymysql
    except ImportError as exc:
        raise RuntimeError("PyMySQL is required; install requirements.txt") from exc
    connection = pymysql.connect(
        host=config.mysql["host"],
        port=int(config.mysql.get("port", 3306)),
        user=config.mysql["user"],
        password=config.mysql["password"],
        database=config.mysql.get("database", "ecommerce_business"),
        charset="utf8mb4",
        autocommit=False,
        connect_timeout=int(config.mysql.get("connect_timeout_seconds", 10)),
        read_timeout=int(config.mysql.get("read_timeout_seconds", 120)),
        write_timeout=int(config.mysql.get("write_timeout_seconds", 120)),
    )
    with connection.cursor() as cursor:
        cursor.execute("SET time_zone = '+08:00'")
    return connection


def insert_rows(cursor: Any, table: str, rows: list[Any], batch_size: int) -> None:
    if not rows:
        return
    columns = [item.name for item in fields(rows[0])]
    sql = (
        f"INSERT INTO `{table}` ("
        + ", ".join(f"`{column}`" for column in columns)
        + ") VALUES ("
        + ", ".join(["%s"] * len(columns))
        + ")"
    )
    for offset in range(0, len(rows), batch_size):
        cursor.executemany(
            sql, [row_values(row) for row in rows[offset : offset + batch_size]]
        )


def replace_dimensions(config: GeneratorConfig, data: DimensionData) -> None:
    connection = connect_mysql(config)
    try:
        with connection.cursor() as cursor:
            for table in (
                "rating_info",
                "payment_info",
                "order_detail",
                "order_info",
                "user_behavior",
            ):
                cursor.execute(f"SELECT COUNT(*) FROM `{table}`")
                if cursor.fetchone()[0] != 0:
                    raise RuntimeError(
                        "dimension initialization requires all five fact tables to be empty"
                    )
            cursor.execute("DELETE FROM sku_info")
            cursor.execute("DELETE FROM shop_info")
            batch_size = int(config.mysql.get("batch_size", 1000))
            insert_rows(cursor, "shop_info", data.shops, batch_size)
            insert_rows(cursor, "sku_info", data.skus, batch_size)
            cursor.execute("SELECT COUNT(*), COUNT(DISTINCT shop_id) FROM shop_info")
            if cursor.fetchone() != (len(data.shops), len(data.shops)):
                raise RuntimeError("shop dimension validation failed")
            cursor.execute("SELECT COUNT(*), COUNT(DISTINCT sku_id) FROM sku_info")
            if cursor.fetchone() != (len(data.skus), len(data.skus)):
                raise RuntimeError("SKU dimension validation failed")
            cursor.execute(
                "SELECT COUNT(*) FROM sku_info s LEFT JOIN shop_info p "
                "ON s.shop_id=p.shop_id WHERE p.shop_id IS NULL"
            )
            if cursor.fetchone()[0] != 0:
                raise RuntimeError("SKU-shop foreign key validation failed")
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


# 5. Direct batch entry
def main() -> int:
    config = GeneratorConfig.load()
    data = DimensionGenerator(config).generate()
    replace_dimensions(config, data)
    print(
        f"dimension initialization complete: profile={config.profile}, "
        f"shop_info={len(data.shops)}, sku_info={len(data.skus)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
