import csv
import json
import logging
from typing import Any

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

from configuration import Configuration
from shopify.client import ShopifyGraphQLClient


class Component(ComponentBase):
    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(__name__)

    def run(self):
        """
        Main execution code
        """
        params = Configuration(**self.configuration.parameters)

        # Initialize Shopify client
        client = ShopifyGraphQLClient(
            store_name=params.store_name, api_token=params.api_token, api_version=params.api_version
        )

        self.logger.info(f"Starting data extraction for endpoints: {params.endpoints}")

        # Process each endpoint
        for endpoint in params.endpoints:
            self.logger.info(f"Processing endpoint: {endpoint}")
            self._process_endpoint(client, endpoint, params)

        self.logger.info("Data extraction completed successfully")

    def _process_endpoint(self, client: ShopifyGraphQLClient, endpoint: str, params: Configuration):
        """
        Process a specific endpoint

        Args:
            client: Shopify GraphQL client
            endpoint: Endpoint name to process
            params: Configuration parameters
        """
        # Dictionary mapping endpoints to their extraction methods
        endpoint_methods = {
            "orders": self._extract_orders,
            "products": self._extract_products,
            "customers": self._extract_customers,
            "inventory_items": self._extract_inventory_items,
            "locations": self._extract_locations,
            "products_drafts": self._extract_product_drafts,
            "product_metafields": self._extract_product_metafields,
            "variant_metafields": self._extract_variant_metafields,
            "inventory": self._extract_inventory_levels,
            "products_archived": self._extract_products_archived,
            "transactions": self._extract_transactions,
            "payments_transactions": self._extract_payment_transactions,
            "events": self._extract_events,
        }

        try:
            # Get the extraction method for the endpoint
            extraction_method = endpoint_methods.get(endpoint)

            if extraction_method:
                extraction_method(client, params)
            else:
                self.logger.warning(f"Unknown endpoint: {endpoint}")
        except Exception as e:
            self.logger.error(f"Error processing endpoint {endpoint}: {str(e)}")
            raise UserException(f"Failed to process endpoint {endpoint}: {str(e)}")

    def _extract_orders(self, client: ShopifyGraphQLClient, params: Configuration):
        """Extract orders data"""
        self.logger.info("Extracting orders data")

        orders_data = []
        total_orders = 0

        for batch in client.get_orders(
            date_from=params.date_from, date_to=params.date_to, batch_size=params.batch_size
        ):
            orders_data.extend(batch)
            total_orders += len(batch)
            self.logger.info(f"Extracted {total_orders} orders so far")

        if orders_data:
            self._write_to_csv("orders", orders_data)
            self.logger.info(f"Successfully extracted {total_orders} orders")
        else:
            self.logger.info("No orders found")

    def _extract_products(self, client: ShopifyGraphQLClient, params: Configuration):
        """Extract products data"""
        self.logger.info("Extracting products data")

        products_data = []
        total_products = 0

        for batch in client.get_products(batch_size=params.batch_size):
            products_data.extend(batch)
            total_products += len(batch)
            self.logger.info(f"Extracted {total_products} products so far")

        if products_data:
            self._write_to_csv("products", products_data)
            self.logger.info(f"Successfully extracted {total_products} products")
        else:
            self.logger.info("No products found")

    def _extract_customers(self, client: ShopifyGraphQLClient, params: Configuration):
        """Extract customers data"""
        self.logger.info("Extracting customers data")

        customers_data = []
        total_customers = 0

        for batch in client.get_customers(batch_size=params.batch_size):
            customers_data.extend(batch)
            total_customers += len(batch)
            self.logger.info(f"Extracted {total_customers} customers so far")

        if customers_data:
            self._write_to_csv("customers", customers_data)
            self.logger.info(f"Successfully extracted {total_customers} customers")
        else:
            self.logger.info("No customers found")

    def _extract_inventory_items(self, client: ShopifyGraphQLClient, params: Configuration):
        """Extract inventory items data"""
        self.logger.info("Extracting inventory items data")

        inventory_data = []
        total_items = 0

        for batch in client.get_inventory_items(batch_size=params.batch_size):
            inventory_data.extend(batch)
            total_items += len(batch)
            self.logger.info(f"Extracted {total_items} inventory items so far")

        if inventory_data:
            self._write_to_csv("inventory_items", inventory_data)
            self.logger.info(f"Successfully extracted {total_items} inventory items")
        else:
            self.logger.info("No inventory items found")

    def _extract_locations(self, client: ShopifyGraphQLClient, params: Configuration):
        """Extract locations data"""
        self.logger.info("Extracting locations data")

        locations_data = client.get_locations()

        if locations_data:
            self._write_to_csv("locations", locations_data)
            self.logger.info(f"Successfully extracted {len(locations_data)} locations")
        else:
            self.logger.info("No locations found")

    def _extract_product_drafts(self, client: ShopifyGraphQLClient, params: Configuration):
        """Extract product drafts data"""
        self.logger.info("Extracting product drafts data")

        drafts_data = []
        total_drafts = 0

        for batch in client.get_product_drafts(batch_size=params.batch_size):
            drafts_data.extend(batch)
            total_drafts += len(batch)
            self.logger.info(f"Extracted {total_drafts} product drafts so far")

        if drafts_data:
            self._write_to_csv("product_drafts", drafts_data)
            self.logger.info(f"Successfully extracted {total_drafts} product drafts")
        else:
            self.logger.info("No product drafts found")

    def _extract_product_metafields(self, client: ShopifyGraphQLClient, params: Configuration):
        """Extract product metafields data"""
        self.logger.info("Extracting product metafields data")

        metafields_data = []
        total_metafields = 0

        for batch in client.get_product_metafields(batch_size=params.batch_size):
            metafields_data.extend(batch)
            total_metafields += len(batch)
            self.logger.info(f"Extracted {total_metafields} product metafields so far")

        if metafields_data:
            self._write_to_csv("product_metafields", metafields_data)
            self.logger.info(f"Successfully extracted {total_metafields} product metafields")
        else:
            self.logger.info("No product metafields found")

    def _extract_variant_metafields(self, client: ShopifyGraphQLClient, params: Configuration):
        """Extract variant metafields data"""
        self.logger.info("Extracting variant metafields data")

        metafields_data = []
        total_metafields = 0

        for batch in client.get_variant_metafields(batch_size=params.batch_size):
            metafields_data.extend(batch)
            total_metafields += len(batch)
            self.logger.info(f"Extracted {total_metafields} variant metafields so far")

        if metafields_data:
            self._write_to_csv("variant_metafields", metafields_data)
            self.logger.info(f"Successfully extracted {total_metafields} variant metafields")
        else:
            self.logger.info("No variant metafields found")

    def _extract_inventory_levels(self, client: ShopifyGraphQLClient, params: Configuration):
        """Extract inventory levels data"""
        self.logger.info("Extracting inventory levels data")

        inventory_data = []
        total_levels = 0

        for batch in client.get_inventory_levels(batch_size=params.batch_size):
            inventory_data.extend(batch)
            total_levels += len(batch)
            self.logger.info(f"Extracted {total_levels} inventory levels so far")

        if inventory_data:
            self._write_to_csv("inventory_levels", inventory_data)
            self.logger.info(f"Successfully extracted {total_levels} inventory levels")
        else:
            self.logger.info("No inventory levels found")

    def _extract_products_archived(self, client: ShopifyGraphQLClient, params: Configuration):
        """Extract archived products data"""
        self.logger.info("Extracting archived products data")

        products_data = []
        total_products = 0

        for batch in client.get_products_archived(batch_size=params.batch_size):
            products_data.extend(batch)
            total_products += len(batch)
            self.logger.info(f"Extracted {total_products} archived products so far")

        if products_data:
            self._write_to_csv("products_archived", products_data)
            self.logger.info(f"Successfully extracted {total_products} archived products")
        else:
            self.logger.info("No archived products found")

    def _extract_transactions(self, client: ShopifyGraphQLClient, params: Configuration):
        """Extract transactions data"""
        self.logger.info("Extracting transactions data")

        transactions_data = []
        total_transactions = 0

        for batch in client.get_transactions(batch_size=params.batch_size):
            transactions_data.extend(batch)
            total_transactions += len(batch)
            self.logger.info(f"Extracted {total_transactions} transactions so far")

        if transactions_data:
            self._write_to_csv("transactions", transactions_data)
            self.logger.info(f"Successfully extracted {total_transactions} transactions")
        else:
            self.logger.info("No transactions found")

    def _extract_payment_transactions(self, client: ShopifyGraphQLClient, params: Configuration):
        """Extract payment transactions data"""
        self.logger.info("Extracting payment transactions data")

        transactions_data = []
        total_transactions = 0

        for batch in client.get_payment_transactions(batch_size=params.batch_size):
            transactions_data.extend(batch)
            total_transactions += len(batch)
            self.logger.info(f"Extracted {total_transactions} payment transactions so far")

        if transactions_data:
            self._write_to_csv("payment_transactions", transactions_data)
            self.logger.info(f"Successfully extracted {total_transactions} payment transactions")
        else:
            self.logger.info("No payment transactions found")

    def _extract_events(self, client: ShopifyGraphQLClient, params: Configuration):
        """Extract events data"""
        self.logger.info("Extracting events data")

        events_data = []
        total_events = 0

        # Note: Events extraction would need additional configuration for event types and subject types
        # For now, we'll extract all events
        for batch in client.get_events(batch_size=params.batch_size):
            events_data.extend(batch)
            total_events += len(batch)
            self.logger.info(f"Extracted {total_events} events so far")

        if events_data:
            self._write_to_csv("events", events_data)
            self.logger.info(f"Successfully extracted {total_events} events")
        else:
            self.logger.info("No events found")

    def _write_to_csv(self, table_name: str, data: list[dict[str, Any]]):
        """
        Write data to CSV file

        Args:
            table_name: Name of the table/file
            data: List of dictionaries to write
        """
        if not data:
            return

        # Flatten nested data structures
        flattened_data = []
        for item in data:
            flattened_item = self._flatten_dict(item)
            flattened_data.append(flattened_item)

        # Get output file path
        output_file = self.data_out_tables / f"{table_name}.csv"

        # Write to CSV
        with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
            if flattened_data:
                fieldnames = flattened_data[0].keys()
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(flattened_data)

        self._write_manifest(table_name, flattened_data, output_file)

        self.logger.info(f"Written {len(flattened_data)} records to {output_file}")

    def _flatten_dict(self, d: dict[str, Any], parent_key: str = "", sep: str = "_") -> dict[str, Any]:
        """
        Flatten nested dictionary

        Args:
            d: Dictionary to flatten
            parent_key: Parent key for nested items
            sep: Separator for nested keys

        Returns:
            Flattened dictionary
        """
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k

            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                # Handle lists by converting to JSON string or flattening if they contain dicts
                if v and isinstance(v[0], dict):
                    # If list contains dicts, flatten each dict with index
                    for i, item in enumerate(v):
                        if isinstance(item, dict):
                            items.extend(self._flatten_dict(item, f"{new_key}_{i}", sep=sep).items())
                        else:
                            items.append((f"{new_key}_{i}", item))
                else:
                    # Convert simple lists to JSON string
                    items.append((new_key, json.dumps(v) if v else ""))
            else:
                items.append((new_key, v))

        return dict(items)


if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
