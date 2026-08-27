from __future__ import annotations

import sys
import unittest
import importlib.util
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = (
    SCRIPT_DIR
    if (SCRIPT_DIR / "1_generate_dimensions.py").is_file()
    else SCRIPT_DIR.parent
)


def load_batch(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, PROJECT_ROOT / filename)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {filename}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


dimensions_batch = load_batch("dimensions_batch", "1_generate_dimensions.py")
daily_batch = load_batch("daily_batch", "2_generate_daily_facts.py")


class GeneratorTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        config_path = PROJECT_ROOT / "config" / "test-small.json"
        cls.dimension_config = dimensions_batch.GeneratorConfig.load(config_path)
        cls.config = daily_batch.GeneratorConfig.load(config_path)
        cls.dimensions = dimensions_batch.DimensionGenerator(
            cls.dimension_config
        ).generate()

    def generate(self):
        facts = daily_batch.DailyFactGenerator(
            self.config, "20260825", self.dimensions
        ).generate()
        facts.kafka_events = daily_batch.build_kafka_events(facts)
        daily_batch.validate_kafka_events(facts)
        return facts

    def test_frozen_dimension_profile(self) -> None:
        self.assertEqual(4, len(self.dimensions.shops))
        self.assertEqual(12, len(self.dimensions.skus))
        self.assertTrue(
            {row.shop_id for row in self.dimensions.skus}
            <= {row.shop_id for row in self.dimensions.shops}
        )

    def test_dimension_names_categories_and_prices_are_business_like(self) -> None:
        self.assertTrue(
            all(not row.shop_name.startswith("店铺-") for row in self.dimensions.shops)
        )
        self.assertTrue(
            all(not row.sku_name.startswith("商品-") for row in self.dimensions.skus)
        )
        self.assertTrue(all("-" in row.category_name for row in self.dimensions.skus))
        self.assertTrue(all("型号" in row.sku_name for row in self.dimensions.skus))
        self.assertTrue(all(row.price > 0 for row in self.dimensions.skus))
        self.assertGreaterEqual(len(dimensions_batch.SHOP_NAME_TEMPLATES), 8)

    def test_small_daily_profile_and_dates(self) -> None:
        facts = self.generate()
        self.assertEqual(50, len(facts.orders))
        self.assertEqual(50, len(facts.details))
        self.assertEqual(500, len(facts.behaviors))
        self.assertGreaterEqual(len(facts.ratings), 1)
        for rows, names in (
            (facts.orders, ("create_time", "update_time")),
            (facts.details, ("create_time",)),
            (facts.payments, ("payment_time", "create_time")),
            (facts.behaviors, ("event_time",)),
            (facts.ratings, ("rating_time",)),
        ):
            self.assertTrue(
                all(
                    getattr(row, name).strftime("%Y%m%d") == "20260825"
                    for row in rows
                    for name in names
                )
            )
        self.assertEqual(
            set(range(24)), {row.event_time.hour for row in facts.behaviors}
        )

    def test_small_daily_profile_is_fixed_on_weekend(self) -> None:
        facts = daily_batch.DailyFactGenerator(
            self.config, "20260829", self.dimensions
        ).generate()
        self.assertEqual(50, len(facts.orders))
        self.assertEqual(50, len(facts.details))
        self.assertEqual(500, len(facts.behaviors))

    def test_daily_generation_defers_kafka_until_after_commit(self) -> None:
        facts = daily_batch.DailyFactGenerator(
            self.config, "20260825", self.dimensions
        ).generate()
        self.assertEqual([], facts.kafka_events)

    def test_risk_samples_are_present(self) -> None:
        facts = self.generate()
        counts = {
            label: list(facts.scenario_by_order.values()).count(label)
            for label in set(facts.scenario_by_order.values())
        }
        self.assertGreaterEqual(counts["PAY_BEFORE_CREATED"], 1)
        self.assertGreaterEqual(counts["DETAIL_ORDER_MISMATCH"], 1)
        self.assertGreaterEqual(counts["USER_HIGH_FREQ_UNPAID"], 6)
        self.assertGreaterEqual(counts["USER_CONSECUTIVE_PAYMENT_FAILED"], 1)

    def test_same_input_is_deterministic(self) -> None:
        first = self.generate()
        second = self.generate()
        self.assertEqual(first.orders, second.orders)
        self.assertEqual(first.details, second.details)
        self.assertEqual(first.payments, second.payments)
        self.assertEqual(first.behaviors, second.behaviors)
        self.assertEqual(first.ratings, second.ratings)
        self.assertEqual(first.kafka_events, second.kafka_events)

    def test_kafka_contract_contains_only_transaction_topics(self) -> None:
        facts = self.generate()
        self.assertEqual(
            {"ods_order_info", "ods_order_detail", "ods_payment_info"},
            {row.topic for row in facts.kafka_events},
        )
        event = facts.kafka_events[0]
        payload = daily_batch.event_payload(event)
        self.assertEqual(
            ["event_id", "event_type", "event_time", "business_date"],
            list(payload.keys())[:4],
        )
        self.assertEqual(event.event_time.strftime("%Y%m%d"), payload["business_date"])
        self.assertNotIn("scenario_tag", payload)

    def test_kafka_disorder_is_bounded_and_affects_same_order(self) -> None:
        facts = self.generate()
        latest_time = facts.kafka_events[0].event_time
        order_seen: set[str] = set()
        same_order_disorder = False
        for event in facts.kafka_events:
            self.assertLessEqual((latest_time - event.event_time).total_seconds(), 120)
            latest_time = max(latest_time, event.event_time)
            if event.event_type == "ORDER_CREATED":
                order_seen.add(event.key)
            elif event.event_type == "ORDER_DETAIL" and event.key not in order_seen:
                same_order_disorder = True
        self.assertTrue(same_order_disorder)

    def test_kafka_event_ids_are_stable_and_unique(self) -> None:
        facts = self.generate()
        event_ids = [row.event_id for row in facts.kafka_events]
        self.assertEqual(len(event_ids), len(set(event_ids)))
        self.assertTrue(all(value.startswith("20260825:") for value in event_ids))

    def test_all_business_times_use_millisecond_precision(self) -> None:
        facts = self.generate()
        for rows, names in (
            (facts.orders, ("create_time", "update_time")),
            (facts.details, ("create_time",)),
            (facts.payments, ("payment_time", "create_time")),
            (facts.behaviors, ("event_time",)),
            (facts.ratings, ("rating_time",)),
        ):
            self.assertTrue(
                all(
                    getattr(row, name).microsecond % 1000 == 0
                    for row in rows
                    for name in names
                )
            )
        self.assertTrue(
            all(event.event_time.microsecond % 1000 == 0 for event in facts.kafka_events)
        )


if __name__ == "__main__":
    unittest.main()
