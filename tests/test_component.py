"""
Tests for Shopify GraphQL Extractor Component

Created on 12. 11. 2018
@author: esner
Updated for GraphQL version
"""

import unittest
import mock
import os
from freezegun import freeze_time

from component import Component
from configuration import Configuration


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
            "endpoints": ["orders", "products"],
            "batch_size": 50,
        }

        config = Configuration(**valid_config)
        self.assertEqual(config.store_name, "test-shop")
        self.assertEqual(config.api_token, "TEST_TOKEN")
        self.assertEqual(config.endpoints, ["orders", "products"])
        self.assertEqual(config.batch_size, 50)

    def test_configuration_invalid_endpoint(self):
        """Test configuration with invalid endpoint"""
        invalid_config = {"store_name": "test-shop", "#api_token": "TEST_TOKEN", "endpoints": ["invalid_endpoint"]}

        with self.assertRaises(Exception):
            Configuration(**invalid_config)

    def test_configuration_store_name_cleanup(self):
        """Test store name cleanup removes .myshopify.com"""
        config_data = {"store_name": "test-shop.myshopify.com", "#api_token": "TEST_TOKEN"}

        config = Configuration(**config_data)
        self.assertEqual(config.store_name, "test-shop")

    def test_flatten_dict(self):
        """Test dictionary flattening functionality"""
        comp = Component()

        # Test simple dict
        simple_dict = {"key1": "value1", "key2": "value2"}
        flattened = comp._flatten_dict(simple_dict)
        self.assertEqual(flattened, simple_dict)

        # Test nested dict
        nested_dict = {"order": {"id": "123", "customer": {"name": "John Doe", "email": "john@example.com"}}}
        flattened = comp._flatten_dict(nested_dict)
        expected = {"order_id": "123", "order_customer_name": "John Doe", "order_customer_email": "john@example.com"}
        self.assertEqual(flattened, expected)

        # Test list handling
        list_dict = {"items": [{"name": "item1", "price": 10}, {"name": "item2", "price": 20}]}
        flattened = comp._flatten_dict(list_dict)
        self.assertIn("items_0_name", flattened)
        self.assertIn("items_1_name", flattened)
        self.assertEqual(flattened["items_0_name"], "item1")
        self.assertEqual(flattened["items_1_name"], "item2")


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
