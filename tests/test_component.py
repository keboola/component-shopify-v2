import os
import tempfile
import unittest
from unittest import mock

from freezegun import freeze_time

from component import Component
from configuration import Configuration
from shopify_cli.client import ShopifyGraphQLClient


class TestCollectionsBulkQueryFilter(unittest.TestCase):
    """The bulk `collections` connection only returns Online-Store-published collections when no
    query argument is supplied, so a publish-status filter must always be injected."""

    def _run_and_capture_mutation(self, date_since=None, date_to=None) -> str:
        with mock.patch.object(ShopifyGraphQLClient, "_setup_session", return_value=None):
            client = ShopifyGraphQLClient("shop", "token", "2025-10", False)

        captured = {}

        def fake_execute_query(query, variables=None, max_retries=5):
            if "bulkOperationRunQuery" in query:
                captured["mutation"] = query
                return {"bulkOperationRunQuery": {"bulkOperation": {"id": "gid://1"}, "userErrors": []}}
            return {"currentBulkOperation": {"status": "COMPLETED", "url": None, "objectCount": 0}}

        with (
            mock.patch.object(client, "execute_query", side_effect=fake_execute_query),
            mock.patch("shopify_cli.client.time.sleep", return_value=None),
            tempfile.NamedTemporaryFile(suffix=".jsonl") as tmp,
        ):
            client.get_collections_bulk(tmp.name, date_since=date_since, date_to=date_to)

        return captured["mutation"]

    def test_publish_status_filter_injected_without_dates(self):
        mutation = self._run_and_capture_mutation()
        self.assertIn('collections(query: "published_status:online_store_channel")', mutation)

    def test_publish_status_filter_anded_with_dates(self):
        mutation = self._run_and_capture_mutation(date_since="2024-01-01", date_to="2025-01-01")
        self.assertIn(
            'collections(query: "published_status:online_store_channel '
            "AND updated_at:>='2024-01-01' AND updated_at:<'2025-01-01'\")",
            mutation,
        )


class TestParseLoadingOptionDates(unittest.TestCase):
    def setUp(self):
        with (
            mock.patch.dict(os.environ, {"KBC_DATADIR": "/tmp"}),
            mock.patch("component.Component.__init__", return_value=None),
        ):
            self.comp = Component.__new__(Component)

    @freeze_time("2026-03-19")
    def test_both_none_returns_none_tuple(self):
        result = self.comp._parse_loading_option_dates(None, None)
        self.assertEqual(result, (None, None))

    @freeze_time("2026-03-19")
    def test_absolute_dates(self):
        start, end = self.comp._parse_loading_option_dates("2026-03-12", "2026-03-19")
        self.assertEqual(start, "2026-03-12")
        self.assertEqual(end, "2026-03-19")

    @freeze_time("2026-03-19")
    def test_relative_date_since(self):
        start, end = self.comp._parse_loading_option_dates("1 week ago", "now")
        self.assertEqual(start, "2026-03-12")
        self.assertEqual(end, "2026-03-19")

    @freeze_time("2026-03-19")
    def test_only_date_since_set(self):
        start, end = self.comp._parse_loading_option_dates("2026-01-01", None)
        self.assertEqual(start, "2026-01-01")
        self.assertIsNone(end)

    @freeze_time("2026-03-19")
    def test_only_date_to_set(self):
        start, end = self.comp._parse_loading_option_dates(None, "2026-03-19")
        self.assertIsNone(start)
        self.assertEqual(end, "2026-03-19")


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
