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


class TestCollectionsDiagnosticClient(unittest.TestCase):
    """Verify the read-only collections diagnostic probes build the expected GraphQL."""

    def _client(self):
        with mock.patch.object(ShopifyGraphQLClient, "_setup_session", return_value=None):
            return ShopifyGraphQLClient("shop", "token", "2025-10", False)

    def test_collections_count_query(self):
        client = self._client()
        captured = {}

        def fake_execute_query(query, variables=None, max_retries=5):
            captured["query"] = query
            captured["variables"] = variables
            return {"collectionsCount": {"count": 65}}

        with mock.patch.object(client, "execute_query", side_effect=fake_execute_query):
            self.assertEqual(client.get_collections_count(), 65)
        self.assertIn("collectionsCount", captured["query"])
        self.assertEqual(captured["variables"], {"query": None})

    def test_collection_ids_returns_nodes(self):
        client = self._client()
        captured = {}

        def fake_execute_query(query, variables=None, max_retries=5):
            captured["variables"] = variables
            return {"collections": {"edges": [{"node": {"id": "gid://shopify/Collection/1", "title": "A"}}]}}

        with mock.patch.object(client, "execute_query", side_effect=fake_execute_query):
            nodes = client.get_collection_ids(query="published_status:unpublished", first=250)
        self.assertEqual(nodes, [{"id": "gid://shopify/Collection/1", "title": "A"}])
        self.assertEqual(captured["variables"], {"first": 250, "query": "published_status:unpublished"})

    def test_collection_by_id_query(self):
        client = self._client()
        captured = {}

        def fake_execute_query(query, variables=None, max_retries=5):
            captured["query"] = query
            captured["variables"] = variables
            return {"collection": {"id": variables["id"], "title": "Aurora"}}

        gid = "gid://shopify/Collection/674851291520"
        with mock.patch.object(client, "execute_query", side_effect=fake_execute_query):
            collection = client.get_collection_by_id(gid)
        self.assertEqual(collection["title"], "Aurora")
        self.assertIn("resourcePublications", captured["query"])
        self.assertIn("unpublishedPublications", captured["query"])
        self.assertEqual(captured["variables"], {"id": gid})


class TestCollectionsDiagnosticOrchestration(unittest.TestCase):
    def setUp(self):
        with (
            mock.patch.dict(os.environ, {"KBC_DATADIR": "/tmp"}),
            mock.patch("component.Component.__init__", return_value=None),
        ):
            self.comp = Component.__new__(Component)
        import logging

        self.comp.logger = logging.getLogger("test-diagnostic")

    def test_collection_id_from_gid(self):
        self.assertEqual(self.comp._collection_id_from_gid("gid://shopify/Collection/674851291520"), "674851291520")
        self.assertEqual(self.comp._collection_id_from_gid(None), "")

    def test_diagnostic_probes_all_filters_and_tolerates_publication_error(self):
        client = mock.Mock()
        client.get_collections_count.return_value = 65
        # Only the baseline (no filter) returns one of the Aurora IDs
        client.get_collection_ids.side_effect = lambda query=None, first=250: (
            [{"id": "gid://shopify/Collection/674851291520"}] if query is None else []
        )
        # Direct lookup fails as if the token lacks read_publications — must not raise
        client.get_collection_by_id.side_effect = Exception("Access denied for publications field")

        with self.assertLogs("test-diagnostic", level="INFO") as logs:
            self.comp._run_collections_diagnostic(client)

        from shopify_cli.client import AURORA_COLLECTION_IDS, COLLECTIONS_DIAGNOSTIC_FILTERS

        self.assertEqual(client.get_collections_count.call_count, 1)
        self.assertEqual(client.get_collection_ids.call_count, len(COLLECTIONS_DIAGNOSTIC_FILTERS))
        self.assertEqual(client.get_collection_by_id.call_count, len(AURORA_COLLECTION_IDS))

        output = "\n".join(logs.output)
        self.assertIn("collectionsCount (no filter) = 65", output)
        self.assertIn("674851291520=PRESENT", output)
        self.assertIn("lookup FAILED", output)


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
