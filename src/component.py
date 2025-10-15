# src/component.py
import json
import logging
from pathlib import Path
from typing import Any

import duckdb
from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

from configuration import Configuration
from shopify_cli.client import ShopifyGraphQLClient


class Component(ComponentBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(__name__)
        # Inicializace DuckDB
        self.conn = duckdb.connect("shopify_data.duckdb")

    def run(self):
        """
        Main execution code
        """
        params = Configuration(**self.configuration.parameters)

        # Initialize Shopify client
        client = ShopifyGraphQLClient(
            store_name=params.store_name,
            api_token=params.api_token,
            api_version=params.api_version,
        )

        self.logger.info(f"Starting data extraction for endpoints: {params.endpoints}")

        # Process each endpoint
        for endpoint in params.endpoints:
            self.logger.info(f"Processing endpoint: {endpoint}")
            self._process_endpoint(client, endpoint, params)

        self.logger.info("Data extraction completed successfully")

    def _process_endpoint(self, client: ShopifyGraphQLClient, endpoint: str, params: Configuration):
        """
        Process a specific endpoint using DuckDB
        """
        endpoint_methods = {
            "orders": self._extract_orders,
            "products": self._extract_products,
            "products_bulk": self._extract_products_bulk,
            "customers": self._extract_customers,
            "customers_bulk": self._extract_customers_bulk,
            "inventory_items": self._extract_inventory_items,
            "locations": self._extract_locations,
            "products_drafts": self._extract_product_drafts,  # ❌ not working, use extract_products with status: draft query # noqa: E501
            "product_metafields": self._extract_product_metafields,  # ❌ not working, use product endpoint, include metafields node # noqa: E501
            "variant_metafields": self._extract_variant_metafields,  # ❌ not working, probably implemented in GetVariantMetafieldsByVariant # noqa: E501
            "inventory": self._extract_inventory_levels,  # ❌ not working, needs to be examined
            "products_archived": self._extract_products_archived,  # ❌ not working, use extract_products with status: archived query # noqa: E501
            "transactions": self._extract_transactions,  # ❌ not working, needs to be examined (there is many transaction-related GraphQL endpoints) # noqa: E501
            # "payments_transactions": self._extract_payment_transactions,  # ❌ not working, same as above 👆
            "events": self._extract_events,
        }

        try:
            extraction_method = endpoint_methods.get(endpoint)
            if extraction_method:
                extraction_method(client, params)
            else:
                self.logger.warning(f"Unknown endpoint: {endpoint}")
        except Exception as e:
            self.logger.error(f"Error processing endpoint {endpoint}: {str(e)}")
            raise UserException(f"Failed to process endpoint {endpoint}: {str(e)}")

    def _extract_orders(self, client: ShopifyGraphQLClient, params: Configuration):
        """Extract orders data using DuckDB"""
        self.logger.info("Extracting orders data")

        # Collect all data
        all_orders = []
        for batch in client.get_orders(
            date_since=params.loading_options.date_since,
            date_to=params.loading_options.date_to,
            batch_size=params.batch_size,
        ):
            all_orders.extend(batch)

        if all_orders:
            self._process_with_duckdb("orders", all_orders, params)
            self.logger.info(f"Successfully extracted {len(all_orders)} orders")
        else:
            self.logger.info("No orders found")

    def _extract_products(self, client: ShopifyGraphQLClient, params: Configuration):
        """Extract products data using DuckDB"""
        self.logger.info("Extracting products data")

        # Collect all data
        all_products = []
        for batch in client.get_products(batch_size=params.batch_size):
            all_products.extend(batch)

        if all_products:
            self._process_with_duckdb("products", all_products, params)
            self.logger.info(f"Successfully extracted {len(all_products)} products")
        else:
            self.logger.info("No products found")

    def _extract_products_bulk(self, client: ShopifyGraphQLClient, params: Configuration):
        """Extract products data using Shopify bulk operations"""
        self.logger.info("Extracting products data via bulk operation")

        all_products = client.get_products_bulk()

        if all_products:
            # Bulk results are already flattened, use simple export
            self._process_bulk_products(all_products)
            self.logger.info(f"Successfully extracted {len(all_products)} products via bulk")
        else:
            self.logger.info("No products found")

    def _process_bulk_products(self, data: list[dict[str, Any]]):
        """Process bulk products data - already flattened by Shopify"""
        from pathlib import Path

        # Separate records by type
        products = [r for r in data if r.get("__typename") == "Product"]
        variants = [r for r in data if r.get("__typename") == "ProductVariant"]
        images = [r for r in data if r.get("__typename") == "ProductImage"]

        self.logger.info(f"Bulk data: {len(products)} products, {len(variants)} variants, {len(images)} images")

        # Process products
        if products:
            self.conn.execute("DROP TABLE IF EXISTS products_bulk")
            self.conn.execute("CREATE TABLE products_bulk AS SELECT * FROM read_json_auto(?)", [json.dumps(products)])

            table = self.create_out_table_definition("products_bulk.csv", incremental=True)
            output_file = Path(table.full_path)
            self.conn.execute(f"COPY products_bulk TO '{output_file}' WITH (FORMAT CSV, HEADER, DELIMITER ',')")

            columns_info = self.conn.execute("DESCRIBE products_bulk").fetchall()
            self._create_typed_manifest("products_bulk", output_file, columns_info)

        # Process variants
        if variants:
            self.conn.execute("DROP TABLE IF EXISTS product_variants_bulk")
            self.conn.execute(
                "CREATE TABLE product_variants_bulk AS SELECT * FROM read_json_auto(?)", [json.dumps(variants)]
            )

            table = self.create_out_table_definition("product_variants_bulk.csv", incremental=True)
            output_file = Path(table.full_path)
            self.conn.execute(f"COPY product_variants_bulk TO '{output_file}' WITH (FORMAT CSV, HEADER, DELIMITER ',')")

            columns_info = self.conn.execute("DESCRIBE product_variants_bulk").fetchall()
            self._create_typed_manifest("product_variants_bulk", output_file, columns_info)

        # Process images
        if images:
            self.conn.execute("DROP TABLE IF EXISTS product_images_bulk")
            self.conn.execute(
                "CREATE TABLE product_images_bulk AS SELECT * FROM read_json_auto(?)", [json.dumps(images)]
            )

            table = self.create_out_table_definition("product_images_bulk.csv", incremental=True)
            output_file = Path(table.full_path)
            self.conn.execute(f"COPY product_images_bulk TO '{output_file}' WITH (FORMAT CSV, HEADER, DELIMITER ',')")

            columns_info = self.conn.execute("DESCRIBE product_images_bulk").fetchall()
            self._create_typed_manifest("product_images_bulk", output_file, columns_info)

    def _extract_customers(self, client: ShopifyGraphQLClient, params: Configuration):
        """Extract customers data using DuckDB"""
        self.logger.info("Extracting customers data")

        # Collect all data
        all_customers = []
        for batch in client.get_customers(batch_size=params.batch_size):
            all_customers.extend(batch)

        if all_customers:
            self._process_with_duckdb("customers", all_customers, params)
            self.logger.info(f"Successfully extracted {len(all_customers)} customers")
        else:
            self.logger.info("No customers found")

    def _extract_customers_bulk(self, client: ShopifyGraphQLClient, params: Configuration):
        """Extract customers data using Shopify bulk operations"""
        self.logger.info("Extracting customers data via bulk operation")

        all_customers = client.get_customers_bulk()

        if all_customers:
            # Bulk results are already flattened, use simple export
            self._process_bulk_customers(all_customers)
            self.logger.info(f"Successfully extracted {len(all_customers)} customers via bulk")
        else:
            self.logger.info("No customers found")

    def _process_bulk_customers(self, data: list[dict[str, Any]]):
        """Process bulk customers data - already flattened by Shopify"""
        from pathlib import Path

        # Separate records by type
        customers = [r for r in data if r.get("__typename") == "Customer"]
        addresses = [r for r in data if r.get("__typename") == "MailingAddress"]

        self.logger.info(f"Bulk data: {len(customers)} customers, {len(addresses)} addresses")

        # Process customers
        if customers:
            self.conn.execute("DROP TABLE IF EXISTS customers_bulk")
            self.conn.execute("CREATE TABLE customers_bulk AS SELECT * FROM read_json_auto(?)", [json.dumps(customers)])

            table = self.create_out_table_definition("customers_bulk.csv", incremental=True)
            output_file = Path(table.full_path)
            self.conn.execute(f"COPY customers_bulk TO '{output_file}' WITH (FORMAT CSV, HEADER, DELIMITER ',')")

            columns_info = self.conn.execute("DESCRIBE customers_bulk").fetchall()
            self._create_typed_manifest("customers_bulk", output_file, columns_info)

        # Process addresses
        if addresses:
            self.conn.execute("DROP TABLE IF EXISTS customer_addresses_bulk")
            self.conn.execute(
                "CREATE TABLE customer_addresses_bulk AS SELECT * FROM read_json_auto(?)", [json.dumps(addresses)]
            )

            table = self.create_out_table_definition("customer_addresses_bulk.csv", incremental=True)
            output_file = Path(table.full_path)
            self.conn.execute(
                f"COPY customer_addresses_bulk TO '{output_file}' WITH (FORMAT CSV, HEADER, DELIMITER ',')"
            )

            columns_info = self.conn.execute("DESCRIBE customer_addresses_bulk").fetchall()
            self._create_typed_manifest("customer_addresses_bulk", output_file, columns_info)

    def _extract_inventory_items(self, client: ShopifyGraphQLClient, params: Configuration):
        """Extract inventory items data using DuckDB"""
        self.logger.info("Extracting inventory items data")

        # Collect all data
        all_inventory_items = []
        for batch in client.get_inventory_items(batch_size=params.batch_size):
            all_inventory_items.extend(batch)

        if all_inventory_items:
            self._process_with_duckdb("inventory_items", all_inventory_items, params)
            self.logger.info(f"Successfully extracted {len(all_inventory_items)} inventory items")
        else:
            self.logger.info("No inventory items found")

    def _extract_locations(self, client: ShopifyGraphQLClient, params: Configuration):
        """Extract locations data using DuckDB"""
        self.logger.info("Extracting locations data")

        # Collect all data
        all_locations = client.get_locations()
        if all_locations:
            self._process_with_duckdb("locations", all_locations, params)
            self.logger.info(f"Successfully extracted {len(all_locations)} locations")
        else:
            self.logger.info("No locations found")

    def _extract_product_drafts(self, client: ShopifyGraphQLClient, params: Configuration):
        """Extract product drafts data using DuckDB"""
        self.logger.info("Extracting product drafts data")

        # Collect all data
        all_product_drafts = []
        for batch in client.get_product_drafts(batch_size=params.batch_size):
            all_product_drafts.extend(batch)

        if all_product_drafts:
            self._process_with_duckdb("product_drafts", all_product_drafts, params)
            self.logger.info(f"Successfully extracted {len(all_product_drafts)} product drafts")
        else:
            self.logger.info("No product drafts found")

    def _extract_products_archived(self, client: ShopifyGraphQLClient, params: Configuration):
        """Extract archived products data using DuckDB"""
        # self.logger.info("Extracting archived products data")

        # # Collect all data
        # all_archived_products = []
        # for batch in client.get_archived_products(batch_size=params.batch_size):
        #     all_archived_products.extend(batch)

        # if all_archived_products:
        #     self._process_with_duckdb("products_archived", all_archived_products, params)
        #     self.logger.info(f"Successfully extracted {len(all_archived_products)} archived products")
        # else:
        #     self.logger.info("No archived products found")

    def _extract_product_metafields(self, client: ShopifyGraphQLClient, params: Configuration):
        """Extract product metafields data using DuckDB"""
        self.logger.info("Extracting product metafields data")

        # Collect all data
        all_product_metafields = []
        for batch in client.get_product_metafields(batch_size=params.batch_size):
            all_product_metafields.extend(batch)

        if all_product_metafields:
            self._process_with_duckdb("product_metafields", all_product_metafields, params)
            self.logger.info(f"Successfully extracted {len(all_product_metafields)} product metafields")
        else:
            self.logger.info("No product metafields found")

    def _extract_variant_metafields(self, client: ShopifyGraphQLClient, params: Configuration):
        """Extract variant metafields data using DuckDB"""
        self.logger.info("Extracting variant metafields data")

        # Collect all data
        all_variant_metafields = []
        for batch in client.get_variant_metafields(batch_size=params.batch_size):
            all_variant_metafields.extend(batch)

        if all_variant_metafields:
            self._process_with_duckdb("variant_metafields", all_variant_metafields, params)
            self.logger.info(f"Successfully extracted {len(all_variant_metafields)} variant metafields")
        else:
            self.logger.info("No variant metafields found")

    def _extract_inventory_levels(self, client: ShopifyGraphQLClient, params: Configuration):
        """Extract inventory levels data using DuckDB"""
        self.logger.info("Extracting inventory levels data")

        # Collect all data
        all_inventory_levels = []
        for batch in client.get_inventory_levels(batch_size=params.batch_size):
            all_inventory_levels.extend(batch)

        if all_inventory_levels:
            self._process_with_duckdb("inventory_levels", all_inventory_levels, params)
            self.logger.info(f"Successfully extracted {len(all_inventory_levels)} inventory levels")
        else:
            self.logger.info("No inventory levels found")

    def _extract_transactions(self, client: ShopifyGraphQLClient, params: Configuration):
        """Extract transactions data using DuckDB"""
        self.logger.info("Extracting transactions data")

        # Collect all data
        all_transactions = []
        for batch in client.get_transactions(batch_size=params.batch_size):
            all_transactions.extend(batch)

        if all_transactions:
            self._process_with_duckdb("transactions", all_transactions, params)
            self.logger.info(f"Successfully extracted {len(all_transactions)} transactions")
        else:
            self.logger.info("No transactions found")

    def _extract_events(self, client: ShopifyGraphQLClient, params: Configuration):
        """Extract events data using DuckDB"""
        self.logger.info("Extracting events data")

        # Collect all data
        all_events = []
        for batch in client.get_events(batch_size=params.batch_size):
            all_events.extend(batch)

        if all_events:
            self._process_with_duckdb("events", all_events, params)
            self.logger.info(f"Successfully extracted {len(all_events)} events")
        else:
            self.logger.info("No events found")

    def _process_with_duckdb(self, table_name: str, data: list[dict[str, Any]], params: Configuration):
        """
        Process data using DuckDB for type detection and normalization
        """
        if not data:
            return

        # Create temporary JSON file
        file_def = self.create_out_file_definition(f"{table_name}_temp.json")
        temp_json = Path(file_def.full_path)
        with open(temp_json, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        try:
            # Load JSON data into DuckDB with automatic type detection
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE {table_name}_raw AS
                SELECT * FROM read_json_auto('{temp_json}')
            """)

            # Get table schema for manifest
            schema_info = self.conn.execute(f"DESCRIBE {table_name}_raw").fetchall()

            # Create normalized tables based on endpoint
            if table_name == "orders":
                self._create_orders_tables(table_name)
            elif table_name == "products":
                self._create_products_tables(table_name)
            elif table_name == "inventory_items":
                self._create_inventory_tables(table_name)
            else:
                # For simple tables, just export as CSV
                self._export_simple_table(f"{table_name}_raw", schema_info)

        finally:
            # Clean up temporary file
            temp_json.unlink()

    def _create_orders_tables(self, table_name: str):
        """Create normalized order tables"""
        # Main orders table
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE orders AS
            SELECT
                id,
                name,
                email,
                phone,
                createdAt,
                updatedAt,
                processedAt,
                cancelledAt,
                cancelReason,
                currency,
                totalPriceSet.shopMoney.amount as totalPrice,
                totalPriceSet.shopMoney.currencyCode as totalPriceCurrency,
                subtotalPriceSet.shopMoney.amount as subtotalPrice,
                subtotalPriceSet.shopMoney.currencyCode as subtotalPriceCurrency,
                totalTaxSet.shopMoney.amount as totalTax,
                totalTaxSet.shopMoney.currencyCode as totalTaxCurrency,
                totalShippingPriceSet.shopMoney.amount as totalShippingPrice,
                totalShippingPriceSet.shopMoney.currencyCode as totalShippingPriceCurrency,
                customer.id as customerId,
                customer.firstName as customerFirstName,
                customer.lastName as customerLastName,
                customer.email as customerEmail,
                customer.phone as customerPhone
            FROM {table_name}_raw
        """)

        # Order line items table
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE order_line_items AS
            SELECT
                id as orderId,
                lineItems.edges[].node.id as lineItemId,
                lineItems.edges[].node.title as title,
                lineItems.edges[].node.quantity as quantity,
                lineItems.edges[].node.sku as sku,
                lineItems.edges[].node.variant.id as variantId,
                lineItems.edges[].node.variant.title as variantTitle,
                lineItems.edges[].node.variant.sku as variantSku,
                lineItems.edges[].node.variant.price as variantPrice
            FROM {table_name}_raw
            WHERE lineItems.edges IS NOT NULL
        """)

        # Export both tables
        self._export_table_to_csv("orders", "orders")
        self._export_table_to_csv("order_line_items", "order_line_items")

    def _create_products_tables(self, table_name: str):
        """Create normalized product tables"""
        # Main products table
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE products AS
            SELECT
                id,
                title,
                handle,
                description,
                productType,
                vendor,
                createdAt,
                updatedAt,
                publishedAt,
                status,
                tags
            FROM {table_name}_raw
        """)

        # Product variants table
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE product_variants AS
            SELECT
                id as productId,
                variants.edges[].node.id as variantId,
                variants.edges[].node.title as title,
                variants.edges[].node.sku as sku,
                variants.edges[].node.price as price,
                variants.edges[].node.compareAtPrice as compareAtPrice,
                variants.edges[].node.inventoryQuantity as inventoryQuantity,
                variants.edges[].node.weight as weight,
                variants.edges[].node.weightUnit as weightUnit
            FROM {table_name}_raw
            WHERE variants.edges IS NOT NULL
        """)

        # Export both tables
        self._export_table_to_csv("products", "products")
        self._export_table_to_csv("product_variants", "product_variants")

    def _create_inventory_tables(self, table_name: str):
        """Create normalized inventory tables"""
        # Main inventory items table
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE inventory_items AS
            SELECT
                id,
                sku,
                tracked,
                createdAt,
                updatedAt,
                countryCodeOfOrigin,
                harmonizedSystemCode,
                provinceCodeOfOrigin,
                requiresShipping,
                unitCost.amount as unitCostAmount,
                unitCost.currencyCode as unitCostCurrency,
                variant.id as variantId,
                variant.title as variantTitle,
                variant.sku as variantSku,
                variant.price as variantPrice,
                variant.product.id as productId,
                variant.product.title as productTitle,
                variant.product.handle as productHandle
            FROM {table_name}_raw
        """)

        # Inventory levels table
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE inventory_levels AS
            SELECT
                id as inventoryItemId,
                inventoryLevels.edges[].node.id as levelId,
                inventoryLevels.edges[].node.available as available,
                inventoryLevels.edges[].node.location.id as locationId,
                inventoryLevels.edges[].node.location.name as locationName
            FROM {table_name}_raw
            WHERE inventoryLevels.edges IS NOT NULL
        """)

        # Export both tables
        self._export_table_to_csv("inventory_items", "inventory_items")
        self._export_table_to_csv("inventory_levels", "inventory_levels")

    def _export_simple_table(self, table_name: str, schema_info):
        """Export simple table to CSV"""
        self._export_table_to_csv(table_name, table_name)

    def _export_table_to_csv(self, output_name: str, table_name: str):
        """Export DuckDB table to CSV with proper types"""
        table = self.create_out_table_definition(f"{output_name}.csv", incremental=True)
        output_file = Path(table.full_path)

        # Export to CSV
        self.conn.execute(f"""
            COPY {table_name} TO '{output_file}'
            WITH (FORMAT CSV, HEADER, DELIMITER ',')
        """)

        # Get column information for manifest
        columns_info = self.conn.execute(f"DESCRIBE {table_name}").fetchall()

        # Create manifest with data types
        self._create_typed_manifest(output_name, output_file, columns_info)

    def _create_typed_manifest(self, table_name: str, output_file: Path, columns_info):
        """Create manifest with DuckDB detected types"""
        manifest = {
            "id": f"in.c-shopify.{table_name}",
            "name": table_name,
            "primary_key": self._get_primary_key(table_name),
            "columns": [col[0] for col in columns_info],
            "metadata": [{"key": "KBC.createdBy.component.id", "value": "ex-shopify-v2"}],
            "column_metadata": [],
        }

        # Add column type metadata
        for col_name, col_type, null, key, default, extra in columns_info:
            # Map DuckDB types to Keboola base types
            keboola_type = self._map_duckdb_to_keboola_type(col_type)
            manifest["column_metadata"].append({"key": f"KBC.datatype.basetype.{col_name}", "value": keboola_type})

        # Write manifest
        manifest_file = output_file.with_suffix(".csv.manifest")
        with open(manifest_file, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2, ensure_ascii=False)

    def _map_duckdb_to_keboola_type(self, duckdb_type: str) -> str:
        """Map DuckDB types to Keboola base types"""
        type_mapping = {
            "VARCHAR": "STRING",
            "BIGINT": "INTEGER",
            "INTEGER": "INTEGER",
            "DOUBLE": "FLOAT",
            "DECIMAL": "NUMERIC",
            "BOOLEAN": "BOOLEAN",
            "DATE": "DATE",
            "TIMESTAMP": "TIMESTAMP",
            "TIMESTAMPTZ": "TIMESTAMP",
        }

        # Handle complex types
        if duckdb_type.startswith("VARCHAR"):
            return "STRING"
        elif duckdb_type.startswith("DECIMAL"):
            return "NUMERIC"
        elif duckdb_type.startswith("DOUBLE"):
            return "FLOAT"

        return type_mapping.get(duckdb_type, "STRING")

    def _get_primary_key(self, table_name: str) -> list[str]:
        """Define primary keys for different tables"""
        primary_keys = {
            "orders": ["id"],
            "order_line_items": ["orderId", "lineItemId"],
            "products": ["id"],
            "product_variants": ["productId", "variantId"],
            "inventory_items": ["id"],
            "inventory_levels": ["inventoryItemId", "levelId"],
            "customers": ["id"],
            "locations": ["id"],
        }
        return primary_keys.get(table_name, ["id"])

    # ... ostatní extract metody zůstávají stejné, jen volají _process_with_duckdb


"""
    Main entrypoint
"""
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
