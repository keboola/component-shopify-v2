import os
import unittest
from unittest import mock

from freezegun import freeze_time

from component import Component
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
        # Explicit date-only user values are still accepted; the lower bound stays midnight-floored
        # and the upper bound is emitted as a full timestamp.
        start, end = self.comp._parse_loading_option_dates("2026-03-12", "2026-03-19")
        self.assertEqual(start, "2026-03-12")
        self.assertEqual(end, "2026-03-19T00:00:00Z")

    @freeze_time("2026-03-19T13:56:13")
    def test_date_to_now_includes_same_day_records(self):
        # date_to="now" must resolve to the actual run moment (not midnight), so a record updated
        # earlier the same day (e.g. 2026-03-19T11:32:42Z) falls below the upper bound and is included.
        start, end = self.comp._parse_loading_option_dates("1 week ago", "now")
        self.assertEqual(start, "2026-03-12")
        self.assertEqual(end, "2026-03-19T13:56:13Z")
        self.assertGreater(end, "2026-03-19T11:32:42Z")

    @freeze_time("2026-03-19T13:56:13")
    def test_date_since_relative_floored_to_midnight(self):
        # Relative date_since values are floored to midnight of their day, not the run time-of-day.
        start, end = self.comp._parse_loading_option_dates("7 years ago", "now")
        self.assertEqual(start, "2019-03-19")
        self.assertEqual(end, "2026-03-19T13:56:13Z")

    @freeze_time("2026-03-19T13:56:13")
    def test_only_date_since_set(self):
        start, end = self.comp._parse_loading_option_dates("2026-01-01", None)
        self.assertEqual(start, "2026-01-01")
        self.assertIsNone(end)

    @freeze_time("2026-03-19T13:56:13")
    def test_unset_date_to_emits_no_upper_bound(self):
        start, end = self.comp._parse_loading_option_dates("7 years ago", None)
        self.assertEqual(start, "2019-03-19")
        self.assertIsNone(end)

    @freeze_time("2026-03-19T13:56:13")
    def test_only_date_to_set(self):
        start, end = self.comp._parse_loading_option_dates(None, "2026-03-19")
        self.assertIsNone(start)
        self.assertEqual(end, "2026-03-19T00:00:00Z")


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


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
