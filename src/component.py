# src/component.py
import json
import logging
import shutil
import time
from collections import OrderedDict
from pathlib import Path
from typing import Any

import duckdb
from keboola.component.base import ComponentBase
from keboola.component.dao import BaseType, ColumnDefinition, SupportedDataTypes
from keboola.component.exceptions import UserException

from configuration import PRODUCTS_ENDPOINTS, Configuration
from shopify_cli.client import BulkOperationResult, ShopifyGraphQLClient


class Component(ComponentBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(__name__)
        # Inicializace DuckDB
        self.conn = duckdb.connect()
        self.conn.execute("SET temp_directory='./duckdb_temp'")
        self.conn.execute("SET preserve_insertion_order=false")
        self.params = Configuration(**self.configuration.parameters)

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
            debug=params.debug,
        )

        enabled_endpoints = params.enabled_endpoints
        self.logger.info(f"Starting data extraction for endpoints: {enabled_endpoints}")

        products_endpoints_processed = False

        for endpoint in enabled_endpoints:
            if endpoint in PRODUCTS_ENDPOINTS and products_endpoints_processed:
                self.logger.info(f"Skipping already processed products endpoint: {endpoint}")
                continue
            self.logger.info(f"Processing endpoint: {endpoint}")
            self._process_endpoint(client, endpoint, params)
            if endpoint in PRODUCTS_ENDPOINTS:
                products_endpoints_processed = True

        if params.events:
            self.logger.info("Processing events endpoint")
            self._process_endpoint(client, "events", params)

        if params.custom_queries:
            self.logger.info(f"Processing {len(params.custom_queries)} custom bulk queries")
            for custom_query in params.custom_queries:
                self.logger.info(f"Processing custom bulk query: {custom_query.name}")
                self._process_custom_query(client, custom_query)

        self.logger.info("Data extraction completed successfully")

    def _process_endpoint(self, client: ShopifyGraphQLClient, endpoint: str, params: Configuration):
        """
        Process a specific endpoint using DuckDB
        """
        endpoint_methods = {
            "products": self._extract_products_bulk,
            "products_drafts": self._extract_products_bulk,
            "products_archived": self._extract_products_bulk,
            "products_legacy": self._extract_products_legacy,
            "orders": self._extract_orders_bulk,
            "orders_legacy": self._extract_orders_legacy,
            "customers": self._extract_customers_bulk,
            "customers_legacy": self._extract_customers_legacy,
            "inventory": self._extract_inventory_bulk,
            "inventory_legacy": self._extract_inventory_levels,
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

    def _extract_orders_legacy(self, client: ShopifyGraphQLClient, params: Configuration):
        """Extract orders data using DuckDB (legacy one-by-one method)"""
        self.logger.info("Extracting orders data (legacy method)")

        # Collect all data
        all_orders = []
        for batch in client.get_orders(
            date_since=params.loading_options.date_since,
            date_to=params.loading_options.date_to,
            batch_size=params.batch_size,
        ):
            all_orders.extend(batch)

        if all_orders:
            self._process_with_duckdb("orders_legacy", all_orders, params)
            self.logger.info(f"Successfully extracted {len(all_orders)} orders")
        else:
            self.logger.info("No orders found")

    def _extract_orders_bulk(self, client: ShopifyGraphQLClient, params: Configuration):
        """Extract orders data using Shopify bulk operations"""
        self.logger.info("Extracting orders data via bulk operation")

        file_def = self.create_out_file_definition("orders_temp.jsonl")
        temp_jsonl = file_def.full_path

        result = client.get_orders_bulk(
            temp_jsonl,
            include_transactions=params.endpoints.order_transactions,
        )

        if result.item_count > 0:
            self._process_bulk_orders(result)
        else:
            self.logger.info("No orders found")
            Path(result.file_path).unlink(missing_ok=True)

    def _extract_products_legacy(self, client: ShopifyGraphQLClient, params: Configuration):
        """Extract products data using DuckDB (legacy one-by-one method)"""
        self.logger.info("Extracting products data (legacy method)")

        # Collect all data
        all_products = []
        for batch in client.get_products(batch_size=params.batch_size):
            all_products.extend(batch)

        if all_products:
            self._process_with_duckdb("products_legacy", all_products, params)
            self.logger.info(f"Successfully extracted {len(all_products)} products")
        else:
            self.logger.info("No products found")

    def _extract_products_bulk(self, client: ShopifyGraphQLClient, params: Configuration):
        """Extract products using Shopify bulk operation with dynamic status filter"""
        self.logger.info("Extracting products using bulk operation")

        # Build status filter based on selected endpoints (all three are independent toggles)
        statuses = []
        if params.endpoints.products:
            statuses.append("active")
        if params.endpoints.products_drafts:
            statuses.append("draft")
        if params.endpoints.products_archived:
            statuses.append("archived")

        if not statuses:
            self.logger.warning("No product status selected, skipping products extraction")
            return

        status_filter = ",".join(statuses)
        self.logger.info(f"Fetching products with statuses: {status_filter}")

        # Create temp file path and let client save directly to it
        file_def = self.create_out_file_definition("products_temp.jsonl")
        temp_jsonl = file_def.full_path

        result = client.get_products_bulk(
            temp_jsonl,
            status=status_filter,
            include_product_metafields=params.endpoints.product_metafields,
            include_variant_metafields=params.endpoints.variant_metafields,
        )

        if result.item_count > 0:
            self._process_bulk_products(result)
        else:
            self.logger.info("No products found")
            # Clean up empty temp file
            Path(result.file_path).unlink(missing_ok=True)

    def _process_bulk_products(self, bulk_result: BulkOperationResult):
        """Process bulk products data - keep all data together including nested JSON"""
        process_start = time.time()

        self.logger.info(f"Processing {bulk_result.item_count} products from {bulk_result.file_path}")

        table_name = "products"

        try:
            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_json_auto('{bulk_result.file_path}')")

            table = self.create_out_table_definition(f"{table_name}.csv", incremental=True)
            output_file = Path(table.full_path)
            self.conn.execute(f"COPY {table_name} TO '{output_file}' WITH (FORMAT CSV, HEADER, DELIMITER ',')")

            columns_info = self.conn.execute(f"DESCRIBE {table_name}").fetchall()
            self._create_typed_manifest(table_name, columns_info)

            # Get row count for logging
            result_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            row_count = result_count[0] if result_count else 0

            process_time = time.time() - process_start
            self.logger.info(
                f"Products processing complete: {row_count} items in {process_time:.2f}s "
                f"(API wait: {bulk_result.api_wait_time:.2f}s, download: {bulk_result.download_time:.2f}s, "
                f"process: {process_time:.2f}s)"
            )
        finally:
            if self.params.debug:
                debug_file = "bulk_products_download.jsonl"
                shutil.copy2(bulk_result.file_path, debug_file)
                self.logger.info(f"[DEBUG] Saved bulk results to {debug_file}")
            Path(bulk_result.file_path).unlink(missing_ok=True)

    def _process_bulk_orders(self, bulk_result: BulkOperationResult):
        """Process bulk orders data - keep all data together including nested JSON"""
        process_start = time.time()

        self.logger.info(f"Processing {bulk_result.item_count} orders from {bulk_result.file_path}")

        table_name = "orders"

        try:
            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_json_auto('{bulk_result.file_path}')")

            table = self.create_out_table_definition(f"{table_name}.csv", incremental=True)
            output_file = Path(table.full_path)
            self.conn.execute(f"COPY {table_name} TO '{output_file}' WITH (FORMAT CSV, HEADER, DELIMITER ',')")

            columns_info = self.conn.execute(f"DESCRIBE {table_name}").fetchall()
            self._create_typed_manifest(table_name, columns_info)

            # Get row count for logging
            result_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            row_count = result_count[0] if result_count else 0

            process_time = time.time() - process_start
            self.logger.info(
                f"Orders processing complete: {row_count} items in {process_time:.2f}s "
                f"(API wait: {bulk_result.api_wait_time:.2f}s, download: {bulk_result.download_time:.2f}s, "
                f"process: {process_time:.2f}s)"
            )
        finally:
            if self.params.debug:
                debug_file = "bulk_orders_download.jsonl"
                shutil.copy2(bulk_result.file_path, debug_file)
                self.logger.info(f"[DEBUG] Saved bulk results to {debug_file}")
            Path(bulk_result.file_path).unlink(missing_ok=True)

    def _extract_customers_legacy(self, client: ShopifyGraphQLClient, params: Configuration):
        """Extract customers data using DuckDB (legacy one-by-one method)"""
        self.logger.info("Extracting customers data (legacy method)")

        # Collect all data
        all_customers = []
        for batch in client.get_customers(batch_size=params.batch_size):
            all_customers.extend(batch)

        if all_customers:
            self._process_with_duckdb("customers_legacy", all_customers, params)
            self.logger.info(f"Successfully extracted {len(all_customers)} customers")
        else:
            self.logger.info("No customers found")

    def _extract_customers_bulk(self, client: ShopifyGraphQLClient, params: Configuration):
        """Extract customers data using Shopify bulk operations"""
        self.logger.info("Extracting customers data via bulk operation")

        # Create temp file path and let client save directly to it
        file_def = self.create_out_file_definition("customers_temp.jsonl")
        temp_jsonl = file_def.full_path

        result = client.get_customers_bulk(temp_jsonl)

        if result.item_count > 0:
            self._process_bulk_customers(result)
        else:
            self.logger.info("No customers found")
            # Clean up empty temp file
            Path(result.file_path).unlink(missing_ok=True)

    def _process_bulk_customers(self, bulk_result: BulkOperationResult):
        """Process bulk customers data - keep all data together including nested JSON"""
        process_start = time.time()

        self.logger.info(f"Processing {bulk_result.item_count} customers from {bulk_result.file_path}")

        table_name = "customers"

        try:
            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_json_auto('{bulk_result.file_path}')")

            table = self.create_out_table_definition(f"{table_name}.csv", incremental=True)
            output_file = Path(table.full_path)
            self.conn.execute(f"COPY {table_name} TO '{output_file}' WITH (FORMAT CSV, HEADER, DELIMITER ',')")

            columns_info = self.conn.execute(f"DESCRIBE {table_name}").fetchall()
            self._create_typed_manifest(table_name, columns_info)

            # Get row count for logging
            result_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            row_count = result_count[0] if result_count else 0

            process_time = time.time() - process_start
            self.logger.info(
                f"Customers processing complete: {row_count} items in {process_time:.2f}s "
                f"(API wait: {bulk_result.api_wait_time:.2f}s, download: {bulk_result.download_time:.2f}s, "
                f"process: {process_time:.2f}s)"
            )
        finally:
            if self.params.debug:
                debug_file = "bulk_customers_download.jsonl"
                shutil.copy2(bulk_result.file_path, debug_file)
                self.logger.info(f"[DEBUG] Saved bulk results to {debug_file}")
            Path(bulk_result.file_path).unlink(missing_ok=True)

    def _extract_inventory_bulk(self, client: ShopifyGraphQLClient, params: Configuration):
        """Extract inventory using Shopify bulk operations"""
        self.logger.info("Extracting inventory via bulk operation")

        file_def = self.create_out_file_definition("inventory_temp.jsonl")
        temp_jsonl = file_def.full_path

        result = client.get_inventory_bulk(temp_jsonl)

        if result.item_count > 0:
            self._process_bulk_inventory(result)
        else:
            self.logger.info("No inventory found")
            Path(result.file_path).unlink(missing_ok=True)

    def _process_bulk_inventory(self, bulk_result: BulkOperationResult):
        """Process bulk inventory data"""
        process_start = time.time()

        self.logger.info(f"Processing {bulk_result.item_count} inventory items from {bulk_result.file_path}")

        table_name = "inventory"

        try:
            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_json_auto('{bulk_result.file_path}')")

            table = self.create_out_table_definition(f"{table_name}.csv", incremental=True)
            output_file = Path(table.full_path)
            self.conn.execute(f"COPY {table_name} TO '{output_file}' WITH (FORMAT CSV, HEADER, DELIMITER ',')")

            columns_info = self.conn.execute(f"DESCRIBE {table_name}").fetchall()
            self._create_typed_manifest(table_name, columns_info)

            result_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            row_count = result_count[0] if result_count else 0

            process_time = time.time() - process_start
            self.logger.info(
                f"Inventory processing complete: {row_count} items in {process_time:.2f}s "
                f"(API wait: {bulk_result.api_wait_time:.2f}s, download: {bulk_result.download_time:.2f}s, "
                f"process: {process_time:.2f}s)"
            )
        finally:
            if self.params.debug:
                debug_file = "bulk_inventory_download.jsonl"
                shutil.copy2(bulk_result.file_path, debug_file)
                self.logger.info(f"[DEBUG] Saved bulk results to {debug_file}")
            Path(bulk_result.file_path).unlink(missing_ok=True)

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

            # Create normalized tables based on endpoint
            if table_name == "orders":
                self._create_orders_tables(table_name)
            elif table_name == "products":
                self._create_products_tables(table_name)
            elif table_name == "inventory_items":
                self._create_inventory_tables(table_name)
            else:
                # For simple tables, just export as CSV
                self._export_simple_table(f"{table_name}_raw")

        finally:
            # Clean up temporary file
            if not self.configuration.parameters.debug:
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
                o.id as orderId,
                item->>'$.node.id' as lineItemId,
                item->>'$.node.title' as title,
                CAST(item->>'$.node.quantity' AS INTEGER) as quantity,
                item->>'$.node.sku' as sku,
                item->>'$.node.variant.id' as variantId,
                item->>'$.node.variant.title' as variantTitle,
                item->>'$.node.variant.sku' as variantSku,
                item->>'$.node.variant.price' as variantPrice
            FROM {table_name}_raw o,
            UNNEST(o.lineItems.edges) as t(item)
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
                p.id as productId,
                variant->>'$.node.id' as variantId,
                variant->>'$.node.title' as title,
                variant->>'$.node.sku' as sku,
                variant->>'$.node.price' as price,
                variant->>'$.node.compareAtPrice' as compareAtPrice,
                CAST(variant->>'$.node.inventoryQuantity' AS INTEGER) as inventoryQuantity,
                CAST(variant->>'$.node.weight' AS DOUBLE) as weight,
                variant->>'$.node.weightUnit' as weightUnit
            FROM {table_name}_raw p,
            UNNEST(p.variants.edges) as t(variant)
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
                i.id as inventoryItemId,
                level->>'$.node.id' as levelId,
                CAST(level->>'$.node.available' AS INTEGER) as available,
                level->>'$.node.location.id' as locationId,
                level->>'$.node.location.name' as locationName
            FROM {table_name}_raw i,
            UNNEST(i.inventoryLevels.edges) as t(level)
        """)

        # Export both tables
        self._export_table_to_csv("inventory_items", "inventory_items")
        self._export_table_to_csv("inventory_levels", "inventory_levels")

    def _export_simple_table(self, table_name: str):
        """Export simple table to CSV"""
        self._export_table_to_csv(table_name, table_name)

    def _export_table_to_csv(self, output_name: str, table_name: str):
        """Export DuckDB table to CSV with proper types"""

        # Get column information for manifest
        table_meta = self.conn.execute(f"DESCRIBE {table_name}").fetchall()

        # Create manifest with data types
        self._create_typed_manifest(output_name, table_meta)

    def _create_typed_manifest(self, table_name: str, table_meta):
        schema = OrderedDict(
            {
                c[0]: ColumnDefinition(
                    data_types=BaseType(dtype=self.convert_base_types(c[1])),
                    primary_key=False,
                )
                for c in table_meta
            }  # c[0] is the column name, c[1] is the data type, c[3] is the primary key
        )

        out_table = self.create_out_table_definition(
            f"{table_name}.csv",
            schema=schema,
            primary_key=self._get_primary_key(table_name),
            # incremental=self.params.destination.incremental,
            has_header=True,
        )

        try:
            q = f"COPY {table_name} TO '{out_table.full_path}' (HEADER, DELIMITER ',', FORCE_QUOTE *)"
            logging.debug(f"Running query: {q}; ")
            self.conn.execute(q)
            self.write_manifest(out_table)
        except duckdb.ConversionException as e:
            raise UserException(f"Error during query execution: {e}")

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
            "orders_legacy": ["id"],
            "products": ["id"],
            "products_legacy": ["id"],
            "customers": ["id"],
            "customers_legacy": ["id"],
            "inventory_items": ["id"],
            "inventory_levels": ["inventoryItemId", "levelId"],
            "locations": ["id"],
        }
        return primary_keys.get(table_name, [])

    @staticmethod
    def convert_base_types(dtype: str) -> SupportedDataTypes:
        if dtype in [
            "TINYINT",
            "SMALLINT",
            "INTEGER",
            "BIGINT",
            "HUGEINT",
            "UTINYINT",
            "USMALLINT",
            "UINTEGER",
            "UBIGINT",
            "UHUGEINT",
        ]:
            return SupportedDataTypes.INTEGER
        elif dtype in ["REAL", "DECIMAL"]:
            return SupportedDataTypes.NUMERIC
        elif dtype == "DOUBLE":
            return SupportedDataTypes.FLOAT
        elif dtype == "BOOLEAN":
            return SupportedDataTypes.BOOLEAN
        elif dtype in ["TIMESTAMP", "TIMESTAMP WITH TIME ZONE"]:
            return SupportedDataTypes.TIMESTAMP
        elif dtype == "DATE":
            return SupportedDataTypes.DATE
        else:
            return SupportedDataTypes.STRING

    def _process_custom_query(self, client: ShopifyGraphQLClient, custom_query):
        """Process a custom GraphQL bulk query"""
        self.logger.info(f"Executing custom bulk query: {custom_query.name}")

        file_def = self.create_out_file_definition(f"{custom_query.name}_temp.jsonl")
        temp_jsonl = file_def.full_path

        result = client.execute_custom_bulk_query(custom_query.query, temp_jsonl)

        if result.item_count > 0:
            self._process_bulk_custom(result, custom_query.name)
        else:
            self.logger.info(f"Custom bulk query '{custom_query.name}' returned no results")
            Path(result.file_path).unlink(missing_ok=True)

    def _process_bulk_custom(self, bulk_result: BulkOperationResult, table_name: str):
        """Process bulk custom query data"""
        process_start = time.time()

        self.logger.info(f"Processing {bulk_result.item_count} items from {bulk_result.file_path}")

        try:
            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_json_auto('{bulk_result.file_path}')")

            columns_info = self.conn.execute(f"DESCRIBE {table_name}").fetchall()

            select_parts = []
            for col_name, col_type, *_ in columns_info:
                if col_type.upper().startswith("STRUCT") or col_type.upper().endswith("[]"):
                    select_parts.append(f"to_json({col_name}) AS {col_name}")
                else:
                    select_parts.append(col_name)

            select_clause = ", ".join(select_parts)

            table = self.create_out_table_definition(f"{table_name}.csv", incremental=True)
            output_file = Path(table.full_path)
            self.conn.execute(
                f"COPY (SELECT {select_clause} FROM {table_name}) TO '{output_file}' "
                "WITH (FORMAT CSV, HEADER, DELIMITER ',', QUOTE '\"')"
            )

            self._create_typed_manifest(table_name, columns_info)

            result_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            row_count = result_count[0] if result_count else 0

            process_time = time.time() - process_start
            self.logger.info(
                f"Custom query '{table_name}' processing complete: {row_count} items in {process_time:.2f}s "
                f"(API wait: {bulk_result.api_wait_time:.2f}s, download: {bulk_result.download_time:.2f}s, "
                f"process: {process_time:.2f}s)"
            )
        finally:
            if self.params.debug:
                debug_file = f"bulk_{table_name}_download.jsonl"
                shutil.copy2(bulk_result.file_path, debug_file)
                self.logger.info(f"[DEBUG] Saved bulk results to {debug_file}")
            Path(bulk_result.file_path).unlink(missing_ok=True)

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
