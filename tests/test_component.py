import csv
import os
import time
import unittest
from pathlib import Path
from unittest import mock

import duckdb
from freezegun import freeze_time

import component
from component import (
    CUSTOMER_JOURNEY_SUMMARY_COLUMNS,
    Component,
    duckdb_memory_limit_for_container_mb,
    resolve_duckdb_memory_limit_mb,
)
from configuration import Configuration


class TestParseLoadingOptionDates(unittest.TestCase):
    def setUp(self):
        with (
            mock.patch.dict(os.environ, {"KBC_DATADIR": "/tmp"}),
            mock.patch("component.Component.__init__", return_value=None),
        ):
            self.comp = Component.__new__(Component)

    @freeze_time("2026-03-19T13:56:13")
    def test_both_none_returns_none_tuple(self):
        result = self.comp._parse_loading_option_dates(None, None)
        self.assertEqual(result, (None, None))

    @freeze_time("2026-03-19T13:56:13")
    def test_absolute_dates(self):
        # Explicit bare-date user values still anchor to UTC midnight; both bounds are emitted as
        # full timestamps, so date-style configs produce the same window as before (L1-151).
        start, end = self.comp._parse_loading_option_dates("2026-03-12", "2026-03-19")
        self.assertEqual(start, "2026-03-12T00:00:00Z")
        self.assertEqual(end, "2026-03-19T00:00:00Z")

    @freeze_time("2026-03-19T13:56:13")
    def test_date_to_now_includes_same_day_records(self):
        # date_to="now" must resolve to the actual run moment (not midnight), so a record updated
        # earlier the same day (e.g. 2026-03-19T11:32:42Z) falls below the upper bound and is included.
        start, end = self.comp._parse_loading_option_dates("1 week ago", "now")
        self.assertEqual(start, "2026-03-12T13:56:13Z")
        self.assertEqual(end, "2026-03-19T13:56:13Z")
        self.assertGreater(end, "2026-03-19T11:32:42Z")

    @freeze_time("2026-03-19T13:56:13")
    def test_date_since_relative_resolves_to_full_timestamp(self):
        # Relative date_since resolves to the exact run time-of-day (no midnight flooring): a
        # 13:56 run of "12 hours ago" means exactly 12 hours back, not ~26 h to the prior midnight.
        start, end = self.comp._parse_loading_option_dates("12 hours ago", "now")
        self.assertEqual(start, "2026-03-19T01:56:13Z")
        self.assertEqual(end, "2026-03-19T13:56:13Z")

    @freeze_time("2026-03-19T13:56:13")
    def test_date_since_bare_date_unchanged(self):
        # Bare calendar dates parse to UTC midnight, so date-style configs are unaffected by the
        # timestamp change: the resolved lower bound is midnight of that day.
        start, _ = self.comp._parse_loading_option_dates("2026-03-12", "now")
        self.assertEqual(start, "2026-03-12T00:00:00Z")

    @freeze_time("2026-03-19T13:56:13")
    def test_only_date_since_set(self):
        start, end = self.comp._parse_loading_option_dates("2026-01-01", None)
        self.assertEqual(start, "2026-01-01T00:00:00Z")
        self.assertIsNone(end)

    @freeze_time("2026-03-19T13:56:13")
    def test_unset_date_to_emits_no_upper_bound(self):
        start, end = self.comp._parse_loading_option_dates("7 years ago", None)
        self.assertEqual(start, "2019-03-19T13:56:13Z")
        self.assertIsNone(end)

    @freeze_time("2026-03-19T13:56:13")
    def test_only_date_to_set(self):
        start, end = self.comp._parse_loading_option_dates(None, "2026-03-19")
        self.assertIsNone(start)
        self.assertEqual(end, "2026-03-19T00:00:00Z")

    def test_bounds_are_identical_across_timezones(self):
        # The emitted bounds must be a real UTC instant regardless of the container's TZ env
        # (dateparser resolves in UTC), not "correct only while the container runs UTC".
        results = []
        original_tz = os.environ.get("TZ")
        try:
            for tz in ("America/New_York", "Europe/Prague"):
                os.environ["TZ"] = tz
                time.tzset()
                with freeze_time("2026-03-19T13:56:13"):
                    results.append(self.comp._parse_loading_option_dates("7 years ago", "now"))
        finally:
            if original_tz is None:
                os.environ.pop("TZ", None)
            else:
                os.environ["TZ"] = original_tz
            time.tzset()
        self.assertEqual(results[0], results[1])
        self.assertEqual(results[0], ("2019-03-19T13:56:13Z", "2026-03-19T13:56:13Z"))


class TestGetPrimaryKey(unittest.TestCase):
    def setUp(self):
        with (
            mock.patch.dict(os.environ, {"KBC_DATADIR": "/tmp"}),
            mock.patch("component.Component.__init__", return_value=None),
        ):
            self.comp = Component.__new__(Component)

    def test_products_child_tables_have_id_primary_key(self):
        # metafield / product_variant / product_image are written incremental, so they must declare
        # a primary key or Storage appends on every run (L1-151, same bug class as line_item PR #26).
        self.assertEqual(self.comp._get_primary_key("metafield"), ["id"])
        self.assertEqual(self.comp._get_primary_key("product_variant"), ["id"])
        self.assertEqual(self.comp._get_primary_key("product_image"), ["id"])


class TestLegacyOrdersQueryBuilder(unittest.TestCase):
    """Pin the legacy get_orders query string: timestamp date bounds must be quoted
    (Shopify search uses ':' as its field separator) and the upper bound must be exclusive."""

    def _build_orders_query(self, date_since, date_to):
        from shopify_cli.client import ShopifyGraphQLClient

        with mock.patch.object(ShopifyGraphQLClient, "_setup_session", return_value=None):
            client = ShopifyGraphQLClient(
                store_name="test-shop", api_token="TEST_TOKEN", api_version="2025-10", debug=False
            )
        captured: dict[str, str] = {}

        def fake_paginate(query, root_field, batch_size):
            captured["query"] = query
            return iter([])

        with mock.patch.object(client, "_paginate", side_effect=fake_paginate):
            list(client.get_orders(date_since=date_since, date_to=date_to))
        return captured["query"]

    def test_legacy_orders_timestamp_bounds_quoted_and_exclusive(self):
        query = self._build_orders_query("2026-03-12", "2026-03-19T13:56:13Z")
        self.assertIn("created_at:>='2026-03-12'", query)
        self.assertIn("created_at:<'2026-03-19T13:56:13Z'", query)
        # Upper bound must be exclusive ('<'), never inclusive ('<='), matching the bulk paths.
        self.assertNotIn("created_at:<=", query)


class TestBulkOrdersQueryBuilder(unittest.TestCase):
    """Pin the bulk get_orders_bulk query string. The VCR functional cassettes match requests on
    method/host/path/query only (not body), so they cannot assert what date bound the bulk mutation
    actually carries; this captures the mutation sent to Shopify and asserts the timestamp date_to
    comes out quoted and exclusive."""

    def _build_bulk_orders_mutation(self, date_since, date_to):
        from shopify_cli.client import ShopifyGraphQLClient

        with mock.patch.object(ShopifyGraphQLClient, "_setup_session", return_value=None):
            client = ShopifyGraphQLClient(
                store_name="test-shop", api_token="TEST_TOKEN", api_version="2025-10", debug=False
            )
        captured: dict[str, str] = {}
        responses = iter(
            [
                {"bulkOperationRunQuery": {"bulkOperation": {"id": "gid://shopify/BulkOperation/1"}, "userErrors": []}},
                {"currentBulkOperation": {"status": "COMPLETED", "url": None, "objectCount": "0"}},
            ]
        )

        def fake_execute_query(query, variables=None):
            if "bulkOperationRunQuery" in query:
                captured["mutation"] = query
            return next(responses)

        with (
            mock.patch.object(client, "execute_query", side_effect=fake_execute_query),
            mock.patch("shopify_cli.client.time.sleep", return_value=None),
        ):
            client.get_orders_bulk(temp_file_path="/tmp/bulk_orders_test.jsonl", date_since=date_since, date_to=date_to)
        return captured["mutation"]

    def test_bulk_orders_timestamp_bound_quoted_and_exclusive(self):
        mutation = self._build_bulk_orders_mutation("2026-03-12", "2026-03-19T13:56:13Z")
        self.assertIn("updated_at:>='2026-03-12'", mutation)
        self.assertIn("updated_at:<'2026-03-19T13:56:13Z'", mutation)
        self.assertNotIn("updated_at:<=", mutation)


class TestComponent(unittest.TestCase):
    # set global time to 2010-10-10 - affects functions like datetime.now()
    @freeze_time("2010-10-10")
    # set KBC_DATADIR env to non-existing dir
    @mock.patch.dict(os.environ, {"KBC_DATADIR": "./non-existing-dir"})
    def test_run_no_cfg_fails(self):
        with self.assertRaises(ValueError):
            comp = Component()
            comp.run()

    def test_configuration_validation(self):
        """Test configuration validation"""
        # Test valid configuration
        valid_config = {
            "store_name": "test-shop",
            "#api_token": "TEST_TOKEN",
            "endpoints": {"orders": True, "products": True},
            "batch_size": 50,
        }

        config = Configuration(**valid_config)
        self.assertEqual(config.store_name, "test-shop")
        self.assertEqual(config.api_token, "TEST_TOKEN")
        self.assertTrue(config.endpoints.orders)
        self.assertTrue(config.endpoints.products)
        self.assertEqual(config.batch_size, 50)

    def test_configuration_get_enabled_endpoints(self):
        """Test getting enabled endpoints"""
        config_data = {
            "store_name": "test-shop",
            "#api_token": "TEST_TOKEN",
            "endpoints": {"orders": True, "products": True, "customers": False},
        }

        config = Configuration(**config_data)
        enabled = config.enabled_endpoints
        self.assertIn("orders", enabled)
        self.assertIn("products", enabled)
        self.assertNotIn("customers", enabled)

    def test_configuration_store_name_cleanup(self):
        """Test store name cleanup removes .myshopify.com"""
        config_data = {"store_name": "test-shop.myshopify.com", "#api_token": "TEST_TOKEN"}

        config = Configuration(**config_data)
        self.assertEqual(config.store_name, "test-shop")


class TestCustomerJourneyFlattening(unittest.TestCase):
    """Assert Order.customerJourneySummary is flattened to fixed columns on the order table.

    Reads the 11_orders_customer_journey functional fixture output, which covers three
    attribution cases: an attributed order (ready=true, both visits + UTM populated), an order
    with ready=false and null visits, and an order with customerJourneySummary null entirely.
    Every one of the flattened columns must be present in all three cases.
    """

    @classmethod
    def setUpClass(cls):
        order_csv = (
            Path(__file__).parent
            / "functional"
            / "11_orders_customer_journey"
            / "expected"
            / "data"
            / "out"
            / "tables"
            / "order.csv"
        )
        with open(order_csv, newline="") as f:
            reader = csv.DictReader(f)
            cls.header = reader.fieldnames
            cls.rows = {row["name"]: row for row in reader}

    def test_all_flattened_columns_present(self):
        expected_columns = [name for name, _ in CUSTOMER_JOURNEY_SUMMARY_COLUMNS]
        self.assertEqual(len(expected_columns), 29)
        for col in expected_columns:
            self.assertIn(col, self.header)
            for order_name, row in self.rows.items():
                self.assertIn(col, row, f"{col} missing for order {order_name}")

    def test_attributed_order_values(self):
        row = self.rows["#1001"]
        self.assertEqual(row["customer_journey_summary__ready"], "true")
        self.assertEqual(row["customer_journey_summary__first_visit__source"], "google")
        self.assertEqual(row["customer_journey_summary__first_visit__utm_parameters__campaign"], "brand")
        self.assertEqual(row["customer_journey_summary__last_visit__referral_code"], "INFL10")

    def test_not_ready_order_has_null_visits(self):
        row = self.rows["#1002"]
        self.assertEqual(row["customer_journey_summary__ready"], "false")
        self.assertEqual(row["customer_journey_summary__first_visit__landing_page"], "")
        self.assertEqual(row["customer_journey_summary__last_visit__source"], "")

    def test_null_summary_order_all_empty(self):
        row = self.rows["#1003"]
        for name, _ in CUSTOMER_JOURNEY_SUMMARY_COLUMNS:
            self.assertEqual(row[name], "", f"{name} should be empty when summary is null")


class TestResolveDuckdbMemoryLimit(unittest.TestCase):
    """The DuckDB memory budget must scale with the container, never shrink below 0.3.3."""

    @staticmethod
    def _cgroup(values: dict[str, str]):
        """Patch Path.read_text so only the given cgroup paths exist."""
        real_read_text = Path.read_text

        def fake_read_text(self, *args, **kwargs):
            key = str(self)
            if key in values:
                return values[key]
            if key in component.CGROUP_MEMORY_LIMIT_PATHS:
                raise FileNotFoundError(key)
            return real_read_text(self, *args, **kwargs)

        return mock.patch.object(Path, "read_text", fake_read_text)

    def test_small_backend_matches_previous_hardcoded_limit(self):
        # 512 MiB container (Keboola "small") must resolve to exactly the 320MB that
        # 0.3.3 hardcoded - this change must be a no-op on the default backend.
        with self._cgroup({"/sys/fs/cgroup/memory.max": str(512 * 1024 * 1024)}):
            self.assertEqual(resolve_duckdb_memory_limit_mb(), 320)

    def test_larger_backend_gets_proportionally_more(self):
        # 1 GiB container ("medium") gets 640MB instead of being capped at 320MB.
        with self._cgroup({"/sys/fs/cgroup/memory.max": str(1024 * 1024 * 1024)}):
            self.assertEqual(resolve_duckdb_memory_limit_mb(), 640)

    def test_cgroup_v1_is_read_when_v2_absent(self):
        with self._cgroup({"/sys/fs/cgroup/memory/memory.limit_in_bytes": str(2048 * 1024 * 1024)}):
            self.assertEqual(resolve_duckdb_memory_limit_mb(), 1280)

    def test_unlimited_cgroup_v2_falls_back_to_floor(self):
        with self._cgroup({"/sys/fs/cgroup/memory.max": "max"}):
            self.assertEqual(resolve_duckdb_memory_limit_mb(), 320)

    def test_cgroup_v1_unlimited_sentinel_falls_back_to_floor(self):
        # cgroup v1 reports "no limit" as a huge sentinel, which must not be trusted.
        with self._cgroup({"/sys/fs/cgroup/memory/memory.limit_in_bytes": "9223372036854771712"}):
            self.assertEqual(resolve_duckdb_memory_limit_mb(), 320)

    def test_implausibly_large_limit_falls_back_to_floor(self):
        # An unconstrained container reports host memory; refuse to size against it.
        with self._cgroup({"/sys/fs/cgroup/memory.max": str(64 * 1024 * 1024 * 1024)}):
            self.assertEqual(resolve_duckdb_memory_limit_mb(), 320)

    def test_unreadable_cgroup_falls_back_to_floor(self):
        with self._cgroup({}):
            self.assertEqual(resolve_duckdb_memory_limit_mb(), 320)

    def test_garbage_cgroup_value_falls_back_to_floor(self):
        with self._cgroup({"/sys/fs/cgroup/memory.max": "not-a-number"}):
            self.assertEqual(resolve_duckdb_memory_limit_mb(), 320)

    def test_tiny_container_never_drops_below_floor(self):
        # Smaller than "small" must still behave exactly as 0.3.3 did, not worse.
        with self._cgroup({"/sys/fs/cgroup/memory.max": str(256 * 1024 * 1024)}):
            self.assertEqual(resolve_duckdb_memory_limit_mb(), 320)

    def test_clamp_helper_is_pure_and_handles_undetected(self):
        # The clamp is separated from the I/O so the cgroup file is read exactly once.
        self.assertEqual(duckdb_memory_limit_for_container_mb(None), 320)
        self.assertEqual(duckdb_memory_limit_for_container_mb(512), 320)
        self.assertEqual(duckdb_memory_limit_for_container_mb(1024), 640)

    def test_emitted_setting_is_byte_identical_to_the_previous_literal(self):
        # End-to-end proof of the no-op claim: on a 512 MiB container the value DuckDB
        # actually ends up with must equal what `SET memory_limit='320MB'` produced.
        with self._cgroup({"/sys/fs/cgroup/memory.max": str(512 * 1024 * 1024)}):
            emitted = f"{resolve_duckdb_memory_limit_mb()}MB"
        self.assertEqual(emitted, "320MB")

        with duckdb.connect(":memory:") as conn:
            conn.execute("SET memory_limit='320MB'")
            previous = conn.execute("SELECT current_setting('memory_limit')").fetchone()[0]
            conn.execute(f"SET memory_limit='{emitted}'")
            current = conn.execute("SELECT current_setting('memory_limit')").fetchone()[0]
        self.assertEqual(current, previous)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
