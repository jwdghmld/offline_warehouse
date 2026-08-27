#!/usr/bin/env python3
"""Batch 2: generate one fact day and optionally publish transaction events."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
from collections import Counter, OrderedDict, defaultdict
from dataclasses import dataclass, fields
from datetime import date, datetime, time, timedelta
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "default.json"
MONEY = Decimal("0.01")
DAY_ID_FACTOR = 10_000_000
PERIODS = ((0, 6), (6, 9), (9, 12), (12, 14), (14, 18), (18, 22), (22, 24))
PERIOD_WEIGHTS = (0.05, 0.08, 0.17, 0.11, 0.21, 0.30, 0.08)


# 1. Configuration and row models
@dataclass(frozen=True)
class GeneratorConfig:
    base_seed: int
    profile: str
    shop_count: int
    sku_count: int
    category_count: int
    user_count: int
    behavior_min: int
    behavior_max: int
    order_min: int
    order_max: int
    detail_min: int
    detail_max: int
    fixed_daily_counts: bool
    weekend_multiplier: tuple[float, float]
    risk_ratio: tuple[float, float]
    mysql: dict[str, Any]
    kafka: dict[str, Any]

    @classmethod
    def load(cls, path: Path | None = None) -> "GeneratorConfig":
        config_path = path or Path(
            os.environ.get("GENERATOR_CONFIG", str(DEFAULT_CONFIG_PATH))
        )
        with config_path.resolve().open("r", encoding="utf-8") as handle:
            raw = json.load(handle)
        dimension = raw["dimensions"]
        facts = raw["facts"]
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
        kafka = dict(raw["kafka"])
        if "KAFKA_BOOTSTRAP_SERVERS" in os.environ:
            kafka["bootstrap_servers"] = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
        if "KAFKA_CLIENT_ID" in os.environ:
            kafka["client_id"] = os.environ["KAFKA_CLIENT_ID"]
        config = cls(
            base_seed=int(raw.get("base_seed", 20260826)),
            profile=str(raw.get("profile", "formal")),
            shop_count=int(dimension["shops"]),
            sku_count=int(dimension["skus"]),
            category_count=int(dimension["categories"]),
            user_count=int(dimension["users"]),
            behavior_min=int(facts["behavior_min"]),
            behavior_max=int(facts["behavior_max"]),
            order_min=int(facts["order_min"]),
            order_max=int(facts["order_max"]),
            detail_min=int(facts["detail_min"]),
            detail_max=int(facts["detail_max"]),
            fixed_daily_counts=bool(facts.get("fixed_daily_counts", False)),
            weekend_multiplier=tuple(facts.get("weekend_multiplier", [1.15, 1.30])),
            risk_ratio=tuple(facts.get("risk_ratio", [0.001, 0.005])),
            mysql=mysql,
            kafka=kafka,
        )
        for name, minimum, maximum in (
            ("behavior", config.behavior_min, config.behavior_max),
            ("order", config.order_min, config.order_max),
            ("detail", config.detail_min, config.detail_max),
        ):
            if minimum <= 0 or minimum > maximum:
                raise ValueError(f"invalid {name} count range in generator config")
        if config.fixed_daily_counts and any(
            minimum != maximum
            for minimum, maximum in (
                (config.behavior_min, config.behavior_max),
                (config.order_min, config.order_max),
                (config.detail_min, config.detail_max),
            )
        ):
            raise ValueError("fixed_daily_counts requires identical min/max counts")
        return config


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


@dataclass(frozen=True)
class OrderInfo:
    order_id: int
    user_id: int
    shop_id: int
    order_status: str
    order_amount: Decimal
    create_time: datetime
    update_time: datetime


@dataclass(frozen=True)
class OrderDetail:
    order_detail_id: int
    order_id: int
    sku_id: int
    sku_num: int
    original_amount: Decimal
    final_amount: Decimal
    create_time: datetime


@dataclass(frozen=True)
class PaymentInfo:
    payment_id: int
    order_id: int
    user_id: int
    payment_type: str
    payment_status: str
    payment_amount: Decimal
    payment_time: datetime
    create_time: datetime


@dataclass(frozen=True)
class UserBehavior:
    event_id: int
    session_id: str
    user_id: int
    sku_id: int
    event_type: str
    event_time: datetime


@dataclass(frozen=True)
class RatingInfo:
    rating_id: int
    order_id: int
    shop_id: int
    shop_score: int
    rating_time: datetime


@dataclass(frozen=True)
class KafkaEvent:
    topic: str
    key: str
    event_id: str
    event_type: str
    event_time: datetime
    business_date: str
    payload: dict[str, Any]


@dataclass
class DimensionData:
    shops: list[ShopInfo]
    skus: list[SkuInfo]


@dataclass
class DailyFacts:
    ds: str
    orders: list[OrderInfo]
    details: list[OrderDetail]
    payments: list[PaymentInfo]
    behaviors: list[UserBehavior]
    ratings: list[RatingInfo]
    scenario_by_order: dict[int, str]
    kafka_events: list[KafkaEvent]

    def counts(self) -> dict[str, int]:
        return {
            "order_info": len(self.orders),
            "order_detail": len(self.details),
            "payment_info": len(self.payments),
            "user_behavior": len(self.behaviors),
            "rating_info": len(self.ratings),
        }


# 2. Deterministic time, amount and distribution helpers
def money(value: Decimal | float | int | str) -> Decimal:
    return Decimal(str(value)).quantize(MONEY, rounding=ROUND_HALF_UP)


def millis(value: datetime) -> datetime:
    return value.replace(microsecond=(value.microsecond // 1000) * 1000)


def daily_seed(base_seed: int, ds: str) -> int:
    digest = hashlib.sha256(f"{base_seed}:{ds}".encode("ascii")).digest()
    return int.from_bytes(digest[:8], "big")


def day_prefix(ds: str) -> int:
    return int(ds) * DAY_ID_FACTOR


def row_values(row: Any) -> tuple[Any, ...]:
    return tuple(getattr(row, item.name) for item in fields(row))


def ensure_unique(name: str, values: Iterable[Any]) -> None:
    rows = list(values)
    if len(rows) != len(set(rows)):
        raise ValueError(f"{name} contains duplicate keys")


def allocate_count(total: int, weights: tuple[float, ...]) -> list[int]:
    exact = [total * weight / sum(weights) for weight in weights]
    allocated = [int(value) for value in exact]
    remaining = total - sum(allocated)
    order = sorted(
        range(len(exact)), key=lambda index: exact[index] - allocated[index], reverse=True
    )
    for index in order[:remaining]:
        allocated[index] += 1
    return allocated


def sample_day_times(
    target_date: date,
    count: int,
    rng: random.Random,
    *,
    cover_24_hours: bool = False,
) -> list[datetime]:
    day_start = datetime.combine(target_date, time.min)
    reserved = 24 if cover_24_hours and count >= 24 else 0
    allocations = allocate_count(count - reserved, PERIOD_WEIGHTS)
    values = (
        [
            millis(
                day_start
                + timedelta(hours=hour, seconds=rng.uniform(0, 3599.999))
            )
            for hour in range(24)
        ]
        if reserved
        else []
    )
    for (start_hour, end_hour), allocation in zip(PERIODS, allocations):
        start_second = start_hour * 3600
        end_second = end_hour * 3600 - 0.001
        for _ in range(allocation):
            values.append(
                millis(
                    day_start
                    + timedelta(seconds=rng.uniform(start_second, end_second))
                )
            )
    values.sort()
    return values


def zipf_index(length: int, rng: random.Random, exponent: float = 2.4) -> int:
    return min(length - 1, int(length * (rng.random() ** exponent)))


def format_time(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S.") + f"{value.microsecond // 1000:03d}"


# 3. Five-table daily fact generation
class DailyFactGenerator:
    def __init__(self, config: GeneratorConfig, ds: str, dimensions: DimensionData) -> None:
        self.config = config
        self.ds = ds
        self.target_date = datetime.strptime(ds, "%Y%m%d").date()
        self.seed = daily_seed(config.base_seed, ds)
        self.rng = random.Random(self.seed)
        self.dimensions = dimensions
        self.day_start = datetime.combine(self.target_date, time.min)
        self.day_end = datetime.combine(self.target_date, time.max).replace(microsecond=999000)

    def generate(self) -> DailyFacts:
        order_count, behavior_count, detail_target = self._daily_counts()
        scenario_by_order = self._scenario_plan(order_count)
        orders, details, payments, ratings = self._generate_trade(
            order_count, detail_target, scenario_by_order
        )
        behaviors = self._generate_behaviors(behavior_count, orders, details)
        facts = DailyFacts(
            ds=self.ds,
            orders=orders,
            details=details,
            payments=payments,
            behaviors=behaviors,
            ratings=ratings,
            scenario_by_order=scenario_by_order,
            kafka_events=[],
        )
        validate_daily_facts(facts, self.dimensions)
        return facts

    def _daily_counts(self) -> tuple[int, int, int]:
        if self.config.fixed_daily_counts:
            return (
                self.config.order_min,
                self.config.behavior_min,
                max(self.config.order_min, self.config.detail_min),
            )

        multiplier = 1.0
        if self.target_date.weekday() >= 5:
            multiplier = self.rng.uniform(*self.config.weekend_multiplier)
        order_count = round(
            self.rng.randint(self.config.order_min, self.config.order_max) * multiplier
        )
        behavior_count = round(
            self.rng.randint(self.config.behavior_min, self.config.behavior_max)
            * multiplier
        )
        minimum = max(order_count, round(self.config.detail_min * multiplier))
        maximum = max(minimum, round(self.config.detail_max * multiplier))
        detail_target = self.rng.randint(minimum, maximum)
        return order_count, behavior_count, detail_target

    def _scenario_plan(self, order_count: int) -> dict[int, str]:
        prefix = day_prefix(self.ds)
        target = max(9, round(order_count * self.rng.uniform(*self.config.risk_ratio)))
        target = min(order_count, target)
        labels = [
            "PAY_BEFORE_CREATED",
            "DETAIL_ORDER_MISMATCH",
            *("USER_HIGH_FREQ_UNPAID" for _ in range(6)),
            "USER_CONSECUTIVE_PAYMENT_FAILED",
        ]
        while len(labels) < target:
            labels.append(
                "PAY_BEFORE_CREATED"
                if len(labels) % 2 == 0
                else "DETAIL_ORDER_MISMATCH"
            )
        return {prefix + index + 1: label for index, label in enumerate(labels[:target])}

    def _generate_trade(
        self,
        order_count: int,
        detail_target: int,
        scenario_by_order: dict[int, str],
    ) -> tuple[list[OrderInfo], list[OrderDetail], list[PaymentInfo], list[RatingInfo]]:
        active_skus = [row for row in self.dimensions.skus if row.status == 1]
        skus_by_shop: dict[int, list[SkuInfo]] = defaultdict(list)
        for sku in active_skus:
            skus_by_shop[sku.shop_id].append(sku)
        order_times = sample_day_times(self.target_date, order_count, self.rng)
        order_times = [
            min(
                self.day_end - timedelta(minutes=20),
                max(self.day_start + timedelta(minutes=10), value),
            )
            for value in order_times
        ]
        order_times.sort()

        detail_counts = [1] * order_count
        remaining = detail_target - order_count
        candidate_indexes = list(range(order_count))
        while remaining > 0:
            index = candidate_indexes[self.rng.randrange(len(candidate_indexes))]
            if detail_counts[index] < 3:
                detail_counts[index] += 1
                remaining -= 1

        prefix = day_prefix(self.ds)
        orders: list[OrderInfo] = []
        details: list[OrderDetail] = []
        payments: list[PaymentInfo] = []
        ratings: list[RatingInfo] = []
        payment_seq = detail_seq = rating_seq = 0
        forced_rating_created = False

        high_freq_ids = [
            order_id
            for order_id, label in scenario_by_order.items()
            if label == "USER_HIGH_FREQ_UNPAID"
        ]
        high_freq_user = 1 + self.rng.randrange(self.config.user_count)
        high_freq_start = self.day_start + timedelta(hours=10, minutes=15)
        failure_order_id = next(
            order_id
            for order_id, label in scenario_by_order.items()
            if label == "USER_CONSECUTIVE_PAYMENT_FAILED"
        )
        failure_user = 1 + self.rng.randrange(self.config.user_count)

        for index in range(order_count):
            order_id = prefix + index + 1
            scenario = scenario_by_order.get(order_id)
            if order_id in high_freq_ids:
                high_index = high_freq_ids.index(order_id)
                create_time = high_freq_start + timedelta(seconds=high_index * 8)
                user_id = high_freq_user
            else:
                create_time = order_times[index]
                user_id = (
                    failure_user
                    if order_id == failure_order_id
                    else 1 + zipf_index(self.config.user_count, self.rng, 4.5)
                )
            eligible_skus = [
                sku
                for sku in active_skus
                if len(skus_by_shop[sku.shop_id]) >= detail_counts[index]
            ]
            if not eligible_skus:
                raise ValueError("frozen SKU pool cannot satisfy per-order detail count")
            preferred_sku = eligible_skus[
                zipf_index(len(eligible_skus), self.rng, 3.3)
            ]
            shop_id = preferred_sku.shop_id
            shop_skus = skus_by_shop[shop_id]
            chosen: list[SkuInfo] = [preferred_sku]
            while len(chosen) < detail_counts[index]:
                sku = shop_skus[zipf_index(len(shop_skus), self.rng, 3.3)]
                if sku not in chosen:
                    chosen.append(sku)

            order_details: list[OrderDetail] = []
            for sku in chosen:
                detail_seq += 1
                sku_num = self.rng.choices((1, 2, 3, 4), weights=(72, 20, 6, 2), k=1)[0]
                original_amount = money(sku.price * sku_num)
                final_amount = money(
                    original_amount * Decimal(str(self.rng.uniform(0.82, 1.0)))
                )
                detail_time = min(
                    self.day_end,
                    create_time + timedelta(milliseconds=100 + len(order_details) * 50),
                )
                order_details.append(
                    OrderDetail(
                        order_detail_id=prefix + detail_seq,
                        order_id=order_id,
                        sku_id=sku.sku_id,
                        sku_num=sku_num,
                        original_amount=original_amount,
                        final_amount=final_amount,
                        create_time=detail_time,
                    )
                )
            order_amount = money(sum((row.final_amount for row in order_details), Decimal("0")))
            if scenario == "DETAIL_ORDER_MISMATCH":
                first = order_details[0]
                changed = money(max(Decimal("0"), first.final_amount - Decimal("1.00")))
                order_details[0] = OrderDetail(
                    first.order_detail_id,
                    first.order_id,
                    first.sku_id,
                    first.sku_num,
                    first.original_amount,
                    changed,
                    first.create_time,
                )
            details.extend(order_details)

            if scenario in {"PAY_BEFORE_CREATED", "DETAIL_ORDER_MISMATCH"}:
                order_status = "PAID"
            elif scenario in {"USER_HIGH_FREQ_UNPAID", "USER_CONSECUTIVE_PAYMENT_FAILED"}:
                order_status = "CREATED"
            else:
                order_status = self.rng.choices(
                    ("PAID", "CANCELLED", "CREATED"), weights=(82, 10, 8), k=1
                )[0]

            order_payments: list[PaymentInfo] = []
            if scenario == "USER_CONSECUTIVE_PAYMENT_FAILED":
                for attempt in range(3):
                    payment_seq += 1
                    payment_time = create_time + timedelta(seconds=20 + attempt * 50)
                    order_payments.append(
                        PaymentInfo(
                            prefix + payment_seq,
                            order_id,
                            user_id,
                            self.rng.choice(("ALIPAY", "WECHAT", "CARD")),
                            "FAILED",
                            Decimal("0.00"),
                            payment_time,
                            payment_time + timedelta(milliseconds=50),
                        )
                    )
            elif order_status == "PAID":
                if self.rng.random() < 0.12:
                    payment_seq += 1
                    failed_time = create_time + timedelta(seconds=20)
                    order_payments.append(
                        PaymentInfo(
                            prefix + payment_seq,
                            order_id,
                            user_id,
                            self.rng.choice(("ALIPAY", "WECHAT", "CARD")),
                            "FAILED",
                            Decimal("0.00"),
                            failed_time,
                            failed_time + timedelta(milliseconds=50),
                        )
                    )
                payment_seq += 1
                if scenario == "PAY_BEFORE_CREATED":
                    payment_time = create_time - timedelta(seconds=30)
                else:
                    payment_time = millis(
                        create_time
                        + timedelta(
                            minutes=self.rng.uniform(1, 30),
                            milliseconds=self.rng.randint(0, 999),
                        )
                    )
                payment_time = min(self.day_end - timedelta(milliseconds=100), payment_time)
                order_payments.append(
                    PaymentInfo(
                        prefix + payment_seq,
                        order_id,
                        user_id,
                        self.rng.choice(("ALIPAY", "WECHAT", "CARD")),
                        "SUCCESS",
                        order_amount,
                        payment_time,
                        max(payment_time, create_time) + timedelta(milliseconds=50),
                    )
                )
            elif order_status == "CANCELLED" and self.rng.random() < 0.35:
                payment_seq += 1
                payment_time = millis(
                    create_time + timedelta(minutes=self.rng.uniform(1, 5))
                )
                order_payments.append(
                    PaymentInfo(
                        prefix + payment_seq,
                        order_id,
                        user_id,
                        self.rng.choice(("ALIPAY", "WECHAT", "CARD")),
                        "FAILED",
                        Decimal("0.00"),
                        payment_time,
                        payment_time + timedelta(milliseconds=50),
                    )
                )
            payments.extend(order_payments)

            success = next(
                (row for row in order_payments if row.payment_status == "SUCCESS"), None
            )
            if success:
                update_time = min(self.day_end, success.create_time)
            elif order_status == "CANCELLED":
                update_time = min(self.day_end, create_time + timedelta(minutes=5))
            else:
                update_time = create_time
            orders.append(
                OrderInfo(
                    order_id=order_id,
                    user_id=user_id,
                    shop_id=shop_id,
                    order_status=order_status,
                    order_amount=order_amount,
                    create_time=create_time,
                    update_time=update_time,
                )
            )
            should_rate = success is not None and (
                not forced_rating_created or self.rng.random() < 0.35
            )
            if should_rate:
                available = int((self.day_end - success.payment_time).total_seconds())
                if available > 1:
                    rating_seq += 1
                    ratings.append(
                        RatingInfo(
                            rating_id=prefix + rating_seq,
                            order_id=order_id,
                            shop_id=shop_id,
                            shop_score=self.rng.choices(
                                (1, 2, 3, 4, 5), weights=(4, 6, 12, 34, 44), k=1
                            )[0],
                            rating_time=success.payment_time
                            + timedelta(seconds=self.rng.randint(1, available)),
                        )
                    )
                    forced_rating_created = True
        return orders, details, payments, ratings

    def _generate_behaviors(
        self,
        count: int,
        orders: list[OrderInfo],
        details: list[OrderDetail],
    ) -> list[UserBehavior]:
        details_by_order: dict[int, list[OrderDetail]] = defaultdict(list)
        for detail in details:
            details_by_order[detail.order_id].append(detail)
        rows: list[UserBehavior] = []
        prefix = day_prefix(self.ds)
        sequence = 0
        for order in orders:
            sku_id = details_by_order[order.order_id][0].sku_id
            cart_time = max(self.day_start, order.create_time - timedelta(minutes=5))
            view_time = max(self.day_start, cart_time - timedelta(minutes=5))
            for event_type, event_time in (("view", view_time), ("cart", cart_time)):
                sequence += 1
                rows.append(
                    UserBehavior(
                        prefix + sequence,
                        f"{self.ds}-S-{order.user_id:06d}-{sequence // 6:08d}",
                        order.user_id,
                        sku_id,
                        event_type,
                        event_time,
                    )
                )
        filler_count = count - len(rows)
        if filler_count < 0:
            raise ValueError("behavior count is too small for order-related behavior")
        times = sample_day_times(
            self.target_date, filler_count, self.rng, cover_24_hours=True
        )
        active_skus = [row for row in self.dimensions.skus if row.status == 1]
        for event_time in times:
            sequence += 1
            sku = active_skus[zipf_index(len(active_skus), self.rng, 3.3)]
            user_id = 1 + zipf_index(self.config.user_count, self.rng, 4.5)
            rows.append(
                UserBehavior(
                    prefix + sequence,
                    f"{self.ds}-S-{user_id:06d}-{sequence // 6:08d}",
                    user_id,
                    sku.sku_id,
                    self.rng.choices(
                        ("view", "favorite", "cart"), weights=(74, 9, 17), k=1
                    )[0],
                    event_time,
                )
            )
        rows.sort(key=lambda row: (row.event_time, row.event_id))
        return rows


# 4. Kafka event construction and controlled disorder
def build_kafka_events(facts: DailyFacts) -> list[KafkaEvent]:
    events: list[KafkaEvent] = []
    for row in facts.orders:
        events.append(
            KafkaEvent(
                "ods_order_info",
                str(row.order_id),
                f"{facts.ds}:ORDER_CREATED:{row.order_id}",
                "ORDER_CREATED",
                row.create_time,
                facts.ds,
                {
                    "order_id": row.order_id,
                    "user_id": row.user_id,
                    "shop_id": row.shop_id,
                    "order_amount": row.order_amount,
                    "create_time": row.create_time,
                },
            )
        )
    for row in facts.details:
        events.append(
            KafkaEvent(
                "ods_order_detail",
                str(row.order_id),
                f"{facts.ds}:ORDER_DETAIL:{row.order_detail_id}",
                "ORDER_DETAIL",
                row.create_time,
                facts.ds,
                {
                    "order_detail_id": row.order_detail_id,
                    "order_id": row.order_id,
                    "sku_id": row.sku_id,
                    "sku_num": row.sku_num,
                    "original_amount": row.original_amount,
                    "final_amount": row.final_amount,
                    "create_time": row.create_time,
                },
            )
        )
    for row in facts.payments:
        event_type = (
            "PAYMENT_SUCCESS" if row.payment_status == "SUCCESS" else "PAYMENT_FAILED"
        )
        events.append(
            KafkaEvent(
                "ods_payment_info",
                str(row.order_id),
                f"{facts.ds}:{event_type}:{row.payment_id}",
                event_type,
                row.payment_time,
                facts.ds,
                {
                    "payment_id": row.payment_id,
                    "order_id": row.order_id,
                    "user_id": row.user_id,
                    "payment_status": row.payment_status,
                    "payment_amount": row.payment_amount,
                    "payment_time": row.payment_time,
                },
            )
        )
    priority = {
        "ORDER_CREATED": 0,
        "ORDER_DETAIL": 1,
        "PAYMENT_SUCCESS": 2,
        "PAYMENT_FAILED": 2,
    }
    events.sort(key=lambda row: (row.event_time, priority[row.event_type], row.event_id))

    order_positions: dict[str, int] = {}
    first_detail_seen: set[str] = set()
    disorder_candidates: list[tuple[int, int]] = []
    for index, event in enumerate(events):
        if event.event_type == "ORDER_CREATED":
            order_positions[event.key] = index
        elif event.event_type == "ORDER_DETAIL" and event.key not in first_detail_seen:
            first_detail_seen.add(event.key)
            order_index = order_positions.get(event.key)
            if order_index is not None:
                delta = (event.event_time - events[order_index].event_time).total_seconds()
                if 0 <= delta <= 120:
                    disorder_candidates.append((order_index, index))
    for order_index, detail_index in disorder_candidates[::97]:
        events[order_index], events[detail_index] = events[detail_index], events[order_index]
    return events


# 5. In-memory fact and Kafka contract validation
def validate_daily_facts(facts: DailyFacts, dimensions: DimensionData) -> None:
    ensure_unique("order_info.order_id", (row.order_id for row in facts.orders))
    ensure_unique("order_detail.order_detail_id", (row.order_detail_id for row in facts.details))
    ensure_unique("payment_info.payment_id", (row.payment_id for row in facts.payments))
    ensure_unique("user_behavior.event_id", (row.event_id for row in facts.behaviors))
    ensure_unique("rating_info.rating_id", (row.rating_id for row in facts.ratings))
    ensure_unique("rating_info.order_id", (row.order_id for row in facts.ratings))
    shop_ids = {row.shop_id for row in dimensions.shops}
    sku_map = {row.sku_id: row for row in dimensions.skus}
    order_map = {row.order_id: row for row in facts.orders}
    details_by_order: dict[int, list[OrderDetail]] = defaultdict(list)
    success_by_order: dict[int, list[PaymentInfo]] = defaultdict(list)
    for detail in facts.details:
        details_by_order[detail.order_id].append(detail)
        if detail.order_id not in order_map or detail.sku_id not in sku_map:
            raise ValueError(f"detail {detail.order_detail_id} has invalid foreign key")
        if sku_map[detail.sku_id].shop_id != order_map[detail.order_id].shop_id:
            raise ValueError(f"detail {detail.order_detail_id} belongs to another shop")
        if (
            detail.sku_num <= 0
            or detail.original_amount < 0
            or detail.final_amount < 0
            or detail.final_amount > detail.original_amount
        ):
            raise ValueError(f"detail {detail.order_detail_id} has invalid amount/quantity")
    for payment in facts.payments:
        order = order_map.get(payment.order_id)
        if order is None or payment.user_id != order.user_id:
            raise ValueError(f"payment {payment.payment_id} has invalid order/user")
        if payment.payment_type not in {"ALIPAY", "WECHAT", "CARD"}:
            raise ValueError(f"payment {payment.payment_id} has invalid payment type")
        if payment.payment_status not in {"SUCCESS", "FAILED"}:
            raise ValueError(f"payment {payment.payment_id} has invalid status")
        if payment.payment_amount < 0 or (
            payment.payment_status == "SUCCESS" and payment.payment_amount <= 0
        ):
            raise ValueError(f"payment {payment.payment_id} has invalid amount semantics")
        if payment.payment_status == "SUCCESS":
            success_by_order[payment.order_id].append(payment)
    for order_id, rows in success_by_order.items():
        if len(rows) > 1:
            raise ValueError(f"order {order_id} has more than one successful payment")
    for order in facts.orders:
        if order.shop_id not in shop_ids:
            raise ValueError(f"order {order.order_id} references missing shop")
        if order.order_status not in {"CREATED", "PAID", "CANCELLED"}:
            raise ValueError(f"order {order.order_id} has invalid status")
        if not details_by_order[order.order_id] or order.order_amount < 0:
            raise ValueError(f"order {order.order_id} has invalid detail/amount")
        scenario = facts.scenario_by_order.get(order.order_id)
        detail_total = money(
            sum((row.final_amount for row in details_by_order[order.order_id]), Decimal("0"))
        )
        success = success_by_order.get(order.order_id, [])
        if scenario != "DETAIL_ORDER_MISMATCH" and detail_total != order.order_amount:
            raise ValueError(f"order {order.order_id} amount mismatch")
        if success and success[0].payment_amount != order.order_amount:
            raise ValueError(f"order {order.order_id} payment amount mismatch")
        if (order.order_status == "PAID") != (len(success) == 1):
            raise ValueError(f"order {order.order_id} final status does not match payment")
    for behavior in facts.behaviors:
        if behavior.sku_id not in sku_map or behavior.event_type not in {
            "view",
            "favorite",
            "cart",
        }:
            raise ValueError(f"behavior {behavior.event_id} has invalid SKU/event type")
    for rating in facts.ratings:
        order = order_map.get(rating.order_id)
        if (
            order is None
            or rating.shop_id != order.shop_id
            or rating.order_id not in success_by_order
            or not 1 <= rating.shop_score <= 5
        ):
            raise ValueError(f"rating {rating.rating_id} is not from a successful order")
    for table_rows, names in (
        (facts.orders, ("create_time", "update_time")),
        (facts.details, ("create_time",)),
        (facts.payments, ("payment_time", "create_time")),
        (facts.behaviors, ("event_time",)),
        (facts.ratings, ("rating_time",)),
    ):
        for row in table_rows:
            if any(getattr(row, name).strftime("%Y%m%d") != facts.ds for name in names):
                raise ValueError(f"{type(row).__name__} contains time outside target ds")
            if any(getattr(row, name).microsecond % 1000 != 0 for name in names):
                raise ValueError(f"{type(row).__name__} contains time below millisecond precision")
    if not any(label == "PAY_BEFORE_CREATED" for label in facts.scenario_by_order.values()):
        raise ValueError("missing PAY_BEFORE_CREATED risk sample")
    if sum(label == "USER_HIGH_FREQ_UNPAID" for label in facts.scenario_by_order.values()) < 6:
        raise ValueError("missing high-frequency unpaid sample")
    if not any(label == "USER_CONSECUTIVE_PAYMENT_FAILED" for label in facts.scenario_by_order.values()):
        raise ValueError("missing consecutive payment failure sample")

    pay_before_ids = {
        order_id
        for order_id, label in facts.scenario_by_order.items()
        if label == "PAY_BEFORE_CREATED"
    }
    if not any(
        row.order_id in pay_before_ids
        and row.payment_status == "SUCCESS"
        and row.payment_time < order_map[row.order_id].create_time
        for row in facts.payments
    ):
        raise ValueError("PAY_BEFORE_CREATED sample is not effective")
    mismatch_ids = {
        order_id
        for order_id, label in facts.scenario_by_order.items()
        if label == "DETAIL_ORDER_MISMATCH"
    }
    if not any(
        money(sum((row.final_amount for row in details_by_order[order_id]), Decimal("0")))
        != order_map[order_id].order_amount
        for order_id in mismatch_ids
    ):
        raise ValueError("DETAIL_ORDER_MISMATCH sample is not effective")
    high_freq_orders = [
        order_map[order_id]
        for order_id, label in facts.scenario_by_order.items()
        if label == "USER_HIGH_FREQ_UNPAID"
    ]
    if (
        len({row.user_id for row in high_freq_orders}) != 1
        or (max(row.create_time for row in high_freq_orders) - min(row.create_time for row in high_freq_orders)).total_seconds() > 60
        or any(row.order_id in success_by_order for row in high_freq_orders)
    ):
        raise ValueError("USER_HIGH_FREQ_UNPAID sample is not effective")
    failure_ids = {
        order_id
        for order_id, label in facts.scenario_by_order.items()
        if label == "USER_CONSECUTIVE_PAYMENT_FAILED"
    }
    failure_rows = [
        row
        for row in facts.payments
        if row.order_id in failure_ids and row.payment_status == "FAILED"
    ]
    if (
        len(failure_rows) < 3
        or len({row.user_id for row in failure_rows}) != 1
        or (max(row.payment_time for row in failure_rows) - min(row.payment_time for row in failure_rows)).total_seconds() > 180
    ):
        raise ValueError("USER_CONSECUTIVE_PAYMENT_FAILED sample is not effective")


def validate_kafka_events(facts: DailyFacts) -> None:
    ensure_unique("kafka.event_id", (row.event_id for row in facts.kafka_events))
    expected_count = len(facts.orders) + len(facts.details) + len(facts.payments)
    if len(facts.kafka_events) != expected_count:
        raise ValueError("Kafka event count does not match transaction facts")
    if any(row.event_time.microsecond % 1000 != 0 for row in facts.kafka_events):
        raise ValueError("Kafka event contains time below millisecond precision")
    expected_topic = {
        "ORDER_CREATED": "ods_order_info",
        "ORDER_DETAIL": "ods_order_detail",
        "PAYMENT_SUCCESS": "ods_payment_info",
        "PAYMENT_FAILED": "ods_payment_info",
    }
    order_arrival: dict[str, int] = {}
    has_same_order_disorder = False
    latest_event_time = facts.kafka_events[0].event_time if facts.kafka_events else None
    for index, event in enumerate(facts.kafka_events):
        if expected_topic.get(event.event_type) != event.topic:
            raise ValueError(f"Kafka event {event.event_id} has invalid topic/type")
        if event.business_date != event.event_time.strftime("%Y%m%d"):
            raise ValueError(f"Kafka event {event.event_id} has invalid business_date")
        if latest_event_time is not None:
            if (latest_event_time - event.event_time).total_seconds() > 120:
                raise ValueError("Kafka disorder exceeds two minutes")
            latest_event_time = max(latest_event_time, event.event_time)
        if event.event_type == "ORDER_CREATED":
            order_arrival[event.key] = index
        elif event.event_type == "ORDER_DETAIL":
            order_index = order_arrival.get(event.key)
            if order_index is None:
                has_same_order_disorder = True
    if facts.orders and facts.details and not has_same_order_disorder:
        raise ValueError("missing controlled same-order cross-topic disorder")


# 6. One-transaction MySQL replacement
class MySqlStore:
    def __init__(self, config: GeneratorConfig) -> None:
        self.config = config
        self.mysql = config.mysql

    def connect(self) -> Any:
        try:
            import pymysql
        except ImportError as exc:
            raise RuntimeError("PyMySQL is required; install requirements.txt") from exc
        connection = pymysql.connect(
            host=self.mysql["host"],
            port=int(self.mysql.get("port", 3306)),
            user=self.mysql["user"],
            password=self.mysql["password"],
            database=self.mysql.get("database", "ecommerce_business"),
            charset="utf8mb4",
            autocommit=False,
            connect_timeout=int(self.mysql.get("connect_timeout_seconds", 10)),
            read_timeout=int(self.mysql.get("read_timeout_seconds", 120)),
            write_timeout=int(self.mysql.get("write_timeout_seconds", 120)),
        )
        with connection.cursor() as cursor:
            cursor.execute("SET time_zone = '+08:00'")
        return connection

    @staticmethod
    def insert_sql(table: str, sample: Any) -> str:
        columns = [item.name for item in fields(sample)]
        return (
            f"INSERT INTO `{table}` ("
            + ", ".join(f"`{column}`" for column in columns)
            + ") VALUES ("
            + ", ".join(["%s"] * len(columns))
            + ")"
        )

    def insert_rows(self, cursor: Any, table: str, rows: list[Any]) -> None:
        if not rows:
            return
        batch_size = int(self.mysql.get("batch_size", 1000))
        sql = self.insert_sql(table, rows[0])
        for offset in range(0, len(rows), batch_size):
            cursor.executemany(
                sql, [row_values(row) for row in rows[offset : offset + batch_size]]
            )

    def load_dimensions(self) -> DimensionData:
        connection = self.connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT shop_id, shop_name, province_name, open_time, status "
                    "FROM shop_info ORDER BY shop_id"
                )
                shops = [ShopInfo(*row) for row in cursor.fetchall()]
                cursor.execute(
                    "SELECT sku_id, sku_name, category_id, category_name, shop_id, price, "
                    "status, create_time, update_time FROM sku_info ORDER BY sku_id"
                )
                skus = [SkuInfo(*row) for row in cursor.fetchall()]
            if len(shops) != self.config.shop_count or len(skus) != self.config.sku_count:
                raise RuntimeError(
                    "frozen dimension count differs from configured profile; "
                    "run 1_generate_dimensions.py with the same GENERATOR_CONFIG first"
                )
            return DimensionData(shops, skus)
        finally:
            connection.close()

    def replace_daily_facts(self, facts: DailyFacts) -> None:
        connection = self.connect()
        try:
            with connection.cursor() as cursor:
                for table in (
                    "rating_info",
                    "payment_info",
                    "order_detail",
                    "order_info",
                    "user_behavior",
                ):
                    cursor.execute(f"DELETE FROM `{table}`")
                self.insert_rows(cursor, "order_info", facts.orders)
                self.insert_rows(cursor, "order_detail", facts.details)
                self.insert_rows(cursor, "payment_info", facts.payments)
                self.insert_rows(cursor, "rating_info", facts.ratings)
                self.insert_rows(cursor, "user_behavior", facts.behaviors)
                self.validate_daily_transaction(cursor, facts)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    @staticmethod
    def validate_daily_transaction(cursor: Any, facts: DailyFacts) -> None:
        expected = {
            "order_info": len(facts.orders),
            "order_detail": len(facts.details),
            "payment_info": len(facts.payments),
            "user_behavior": len(facts.behaviors),
            "rating_info": len(facts.ratings),
        }
        date_columns = {
            "order_info": ("create_time", "update_time"),
            "order_detail": ("create_time",),
            "payment_info": ("payment_time", "create_time"),
            "user_behavior": ("event_time",),
            "rating_info": ("rating_time",),
        }
        for table, count in expected.items():
            cursor.execute(f"SELECT COUNT(*) FROM `{table}`")
            if cursor.fetchone()[0] != count:
                raise RuntimeError(f"{table} row count validation failed")
            date_predicate = " OR ".join(
                f"DATE_FORMAT(`{column}`, '%%Y%%m%%d') <> %s"
                for column in date_columns[table]
            )
            cursor.execute(
                f"SELECT COUNT(*) FROM `{table}` WHERE {date_predicate}",
                tuple(facts.ds for _ in date_columns[table]),
            )
            if cursor.fetchone()[0] != 0:
                raise RuntimeError(f"{table} business date validation failed")

        value_checks = (
            (
                "SELECT COUNT(*) FROM order_info WHERE order_status NOT IN "
                "('CREATED','PAID','CANCELLED') OR order_amount<0",
                "order status/amount validation failed",
            ),
            (
                "SELECT COUNT(*) FROM order_detail WHERE sku_num<=0 OR original_amount<0 "
                "OR final_amount<0 OR final_amount>original_amount",
                "order detail amount/quantity validation failed",
            ),
            (
                "SELECT COUNT(*) FROM payment_info WHERE payment_type NOT IN "
                "('ALIPAY','WECHAT','CARD') OR payment_status NOT IN ('SUCCESS','FAILED') "
                "OR (payment_status='SUCCESS' AND payment_amount<=0) OR payment_amount<0",
                "payment enum/amount validation failed",
            ),
            (
                "SELECT COUNT(*) FROM user_behavior WHERE event_type NOT IN "
                "('view','favorite','cart')",
                "behavior event type validation failed",
            ),
            (
                "SELECT COUNT(*) FROM rating_info WHERE shop_score NOT BETWEEN 1 AND 5",
                "rating score validation failed",
            ),
        )
        for sql, message in value_checks:
            cursor.execute(sql)
            if cursor.fetchone()[0] != 0:
                raise RuntimeError(message)

        cursor.execute(
            "SELECT COUNT(*) FROM order_info o LEFT JOIN shop_info s ON o.shop_id=s.shop_id "
            "WHERE s.shop_id IS NULL"
        )
        if cursor.fetchone()[0] != 0:
            raise RuntimeError("order-shop foreign key validation failed")
        cursor.execute(
            "SELECT COUNT(*) FROM order_detail d LEFT JOIN order_info o ON d.order_id=o.order_id "
            "LEFT JOIN sku_info s ON d.sku_id=s.sku_id "
            "WHERE o.order_id IS NULL OR s.sku_id IS NULL OR o.shop_id<>s.shop_id"
        )
        if cursor.fetchone()[0] != 0:
            raise RuntimeError("order detail foreign key/shop validation failed")
        cursor.execute(
            "SELECT COUNT(*) FROM payment_info p LEFT JOIN order_info o ON p.order_id=o.order_id "
            "WHERE o.order_id IS NULL OR p.user_id<>o.user_id"
        )
        if cursor.fetchone()[0] != 0:
            raise RuntimeError("payment user validation failed")
        cursor.execute(
            "SELECT COUNT(*) FROM (SELECT order_id FROM payment_info "
            "WHERE payment_status='SUCCESS' GROUP BY order_id HAVING COUNT(*)>1) t"
        )
        if cursor.fetchone()[0] != 0:
            raise RuntimeError("multiple successful payments detected")
        cursor.execute(
            "SELECT COUNT(*) FROM order_info o LEFT JOIN ("
            "SELECT order_id, COUNT(*) success_count FROM payment_info "
            "WHERE payment_status='SUCCESS' AND payment_amount>0 GROUP BY order_id"
            ") p ON o.order_id=p.order_id WHERE "
            "(o.order_status='PAID' AND COALESCE(p.success_count,0)<>1) OR "
            "(o.order_status<>'PAID' AND COALESCE(p.success_count,0)<>0)"
        )
        if cursor.fetchone()[0] != 0:
            raise RuntimeError("order final status/payment validation failed")
        cursor.execute(
            "SELECT COUNT(*) FROM user_behavior b LEFT JOIN sku_info s ON b.sku_id=s.sku_id "
            "WHERE s.sku_id IS NULL"
        )
        if cursor.fetchone()[0] != 0:
            raise RuntimeError("behavior-SKU foreign key validation failed")
        cursor.execute(
            "SELECT COUNT(*) FROM rating_info r LEFT JOIN order_info o ON r.order_id=o.order_id "
            "LEFT JOIN payment_info p ON p.order_id=r.order_id "
            "AND p.payment_status='SUCCESS' AND p.payment_amount>0 "
            "WHERE o.order_id IS NULL OR r.shop_id<>o.shop_id OR p.payment_id IS NULL"
        )
        if cursor.fetchone()[0] != 0:
            raise RuntimeError("rating source validation failed")

        cursor.execute(
            "SELECT o.order_id FROM order_info o JOIN ("
            "SELECT order_id, ROUND(SUM(final_amount), 2) detail_amount "
            "FROM order_detail GROUP BY order_id"
            ") d ON o.order_id=d.order_id WHERE d.detail_amount<>o.order_amount "
            "ORDER BY o.order_id"
        )
        actual_detail_mismatch = {row[0] for row in cursor.fetchall()}
        expected_detail_mismatch = {
            order_id
            for order_id, label in facts.scenario_by_order.items()
            if label == "DETAIL_ORDER_MISMATCH"
        }
        if actual_detail_mismatch != expected_detail_mismatch:
            raise RuntimeError("detail/order amount anomaly set validation failed")
        cursor.execute(
            "SELECT p.order_id FROM payment_info p JOIN order_info o ON p.order_id=o.order_id "
            "WHERE p.payment_status='SUCCESS' AND p.payment_amount<>o.order_amount"
        )
        if cursor.fetchall():
            raise RuntimeError("unexpected payment/order amount mismatch")
        cursor.execute(
            "SELECT p.order_id FROM payment_info p JOIN order_info o ON p.order_id=o.order_id "
            "WHERE p.payment_status='SUCCESS' AND p.payment_time<o.create_time "
            "ORDER BY p.order_id"
        )
        actual_pay_before = {row[0] for row in cursor.fetchall()}
        expected_pay_before = {
            order_id
            for order_id, label in facts.scenario_by_order.items()
            if label == "PAY_BEFORE_CREATED"
        }
        if actual_pay_before != expected_pay_before:
            raise RuntimeError("payment sequence anomaly set validation failed")


# 7. Post-commit Kafka publishing
def event_payload(event: KafkaEvent) -> OrderedDict[str, Any]:
    payload: OrderedDict[str, Any] = OrderedDict()
    payload["event_id"] = event.event_id
    payload["event_type"] = event.event_type
    payload["event_time"] = format_time(event.event_time)
    payload["business_date"] = event.business_date
    for key, value in event.payload.items():
        payload[key] = format_time(value) if isinstance(value, datetime) else value
    return payload


def publish_kafka(config: GeneratorConfig, events: list[KafkaEvent]) -> dict[str, int]:
    try:
        import simplejson
        from kafka import KafkaProducer
    except ImportError as exc:
        raise RuntimeError(
            "kafka-python and simplejson are required; install requirements.txt"
        ) from exc
    kafka = config.kafka
    producer = KafkaProducer(
        bootstrap_servers=kafka["bootstrap_servers"],
        client_id=kafka.get("client_id", "offline-data-generator"),
        acks="all",
        retries=int(kafka.get("retries", 3)),
        enable_idempotence=True,
        max_in_flight_requests_per_connection=1,
        linger_ms=int(kafka.get("linger_ms", 20)),
        batch_size=int(kafka.get("batch_size", 65536)),
        request_timeout_ms=int(kafka.get("request_timeout_ms", 30000)),
    )
    counts: dict[str, int] = {}
    pending: list[Any] = []
    try:
        for event in events:
            value = simplejson.dumps(
                event_payload(event),
                ensure_ascii=False,
                use_decimal=True,
                separators=(",", ":"),
            ).encode("utf-8")
            pending.append(
                producer.send(
                    event.topic,
                    key=event.key.encode("ascii"),
                    value=value,
                )
            )
            counts[event.topic] = counts.get(event.topic, 0) + 1
            if len(pending) >= 10000:
                for future in pending:
                    future.get(timeout=30)
                pending.clear()
        for future in pending:
            future.get(timeout=30)
        producer.flush(timeout=120)
        return counts
    finally:
        producer.close(timeout=30)


# 8. Direct batch entry
def default_ds() -> str:
    target = datetime.now(ZoneInfo("Asia/Shanghai")).date() - timedelta(days=1)
    return target.strftime("%Y%m%d")


def valid_ds(value: str) -> str:
    try:
        parsed = datetime.strptime(value, "%Y%m%d")
    except ValueError as exc:
        raise argparse.ArgumentTypeError("--ds must be a valid yyyyMMdd date") from exc
    if parsed.strftime("%Y%m%d") != value:
        raise argparse.ArgumentTypeError("--ds must use yyyyMMdd format")
    return value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate and transactionally replace one complete business day."
    )
    parser.add_argument("--ds", type=valid_ds, default=default_ds())
    parser.add_argument(
        "--publish",
        action="store_true",
        help="publish committed order/detail/payment events to Kafka",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = GeneratorConfig.load()
    store = MySqlStore(config)
    dimensions = store.load_dimensions()
    facts = DailyFactGenerator(config, args.ds, dimensions).generate()
    scenario_counts = Counter(facts.scenario_by_order.values())
    print(
        "generated: "
        + ", ".join(f"{name}={count}" for name, count in facts.counts().items())
    )
    print(
        "risk samples: "
        + ", ".join(f"{name}={count}" for name, count in sorted(scenario_counts.items()))
    )
    store.replace_daily_facts(facts)
    print(f"MySQL transaction committed for ds={args.ds}")
    if args.publish:
        try:
            facts.kafka_events = build_kafka_events(facts)
            validate_kafka_events(facts)
            published = publish_kafka(config, facts.kafka_events)
        except Exception:
            print(
                "Kafka event preparation/publish failed after MySQL commit. "
                "Rerun the same --ds with --publish to resend deterministic events."
            )
            raise
        print(
            "Kafka publish complete: "
            + ", ".join(f"{topic}={count}" for topic, count in sorted(published.items()))
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
