import csv
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import duckdb
from freezegun import freeze_time

from component import CUSTOMER_JOURNEY_SUMMARY_COLUMNS, Component
from configuration import Configuration


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


class TestEntityKeysColumnInvariant(unittest.TestCase):
    """Every key in entity_keys[entity] must materialize as a CSV column, even when it is
    absent from DuckDB's inferred (sampled) schema. This guards the nondeterministic
    read_json_auto sampling miss that dropped `transactions` from order.csv on large datasets.
    """

    def setUp(self):
        with (
            mock.patch.dict(os.environ, {"KBC_DATADIR": "/tmp"}),
            mock.patch("component.Component.__init__", return_value=None),
        ):
            self.comp = Component.__new__(Component)
        self.comp.logger = mock.MagicMock()
        self.comp.conn = duckdb.connect(":memory:")
        self.comp.params = SimpleNamespace(loading_options=SimpleNamespace(incremental_output=0))
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.addCleanup(self.comp.conn.close)

    def _run_export(self, table_meta, entity_keys):
        out_path = os.path.join(self._tmp.name, "order.csv")
        out_table = SimpleNamespace(full_path=out_path)
        with (
            mock.patch.object(self.comp, "create_out_table_definition", return_value=out_table),
            mock.patch.object(self.comp, "write_manifest"),
        ):
            self.comp._export_entity_type("orders", "order", "Order", table_meta, entity_keys)
        with open(out_path, newline="") as f:
            return next(csv.reader(f))

    def test_scanned_key_absent_from_inferred_schema_is_exported(self):
        # DuckDB inferred schema (the sampled DESCRIBE) is missing `transactions`.
        self.comp.conn.execute("""
            CREATE TABLE orders AS
            SELECT 'gid://shopify/Order/1' AS id, '#1001' AS name
        """)
        table_meta = self.comp.conn.execute('DESCRIBE "orders"').fetchall()
        # entity_keys is the authoritative full JSONL scan and DOES contain transactions.
        entity_keys = {"Order": {"id", "name", "transactions"}}

        header = self._run_export(table_meta, entity_keys)

        self.assertIn("transactions", header, "transactions must be materialized as a NULL-filled column")
        self.assertIn("id", header)
        self.assertIn("name", header)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
