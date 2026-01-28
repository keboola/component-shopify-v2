# src/component.py
import json
import logging
import re
import shutil
import tempfile
import time
from collections import OrderedDict, defaultdict
from pathlib import Path
from typing import Any

import dateparser
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
        db_path = "debug.duckdb" if self.configuration.parameters.get("debug") else ":memory:"
        self.conn = duckdb.connect(db_path)
        self.conn.execute("SET temp_directory='/tmp/duckdb_temp'")
        self.conn.execute("SET memory_limit='256MB'")
        self.conn.execute("SET preserve_insertion_order=false")
        self.params = Configuration(**self.configuration.parameters)

        if self.params.debug:
            self.logger.debug(f"DuckDB database saved to: {db_path}")

    def _camel_to_snake(self, name: str) -> str:
        """Convert camelCase to snake_case"""
        name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
        return re.sub("([a-z0-9])([A-Z])", r"\1_\2", name).lower()

    def _scan_jsonl_keys(self, jsonl_path: str) -> dict[str, set[str]]:
        """Scan JSONL file to detect which keys exist for each entity type"""
        entity_keys = defaultdict(set)

        with open(jsonl_path) as f:
            for line in f:
                if obj := json.loads(line):
                    if entity_id := obj.get("id"):
                        if match := re.search(r"gid://shopify/([^/]+)/", entity_id):
                            entity_type = match.group(1)
                            entity_keys[entity_type].update(obj.keys())

        return entity_keys

    def _normalize_table(self, table_name: str) -> str:
        """
        Convert all STRUCT and LIST columns to JSON strings with proper double quotes.
        Rename all columns from camelCase to snake_case.
        """
        columns_info = self.conn.execute(f'DESCRIBE "{table_name}"').fetchall()
        select_parts = []
        needs_conversion = False

        for col_name, col_type, *_ in columns_info:
            col_type_clean = col_type.strip()
            snake_name = self._camel_to_snake(col_name)

            if (
                col_type_clean.startswith("STRUCT(")
                or col_type_clean.endswith("[]")
                or "LIST" in col_type_clean.upper()
            ):
                select_parts.append(f'json("{col_name}") AS "{snake_name}"')
                needs_conversion = True
            else:
                select_parts.append(f'"{col_name}" AS "{snake_name}"')
                if col_name != snake_name:
                    needs_conversion = True

        if not needs_conversion:
            return table_name

        normalized_table = f"{table_name}_json"

        self.conn.execute(f'DROP TABLE IF EXISTS "{normalized_table}"')
        self.conn.execute(f'CREATE TABLE "{normalized_table}" AS SELECT {", ".join(select_parts)} FROM "{table_name}"')

        return normalized_table

    def _decompose_json_columns(self, table_name: str, normalized_table: str):
        """
        Decompose JSON columns into separate child tables with proper relationships.
        Arrays become separate rows, objects become separate tables with 1:1 relationship.
        """
        columns_info = self.conn.execute(f'DESCRIBE "{table_name}"').fetchall()
        primary_key_col = "id"

        for col_name, col_type, *_ in columns_info:
            col_type_str = str(col_type).upper()

            if not ("STRUCT" in col_type_str or "LIST" in col_type_str or col_type_str.endswith("[]")):
                continue

            snake_col_name = self._camel_to_snake(col_name)
            self.logger.info(f"Decomposing column: {col_name} ({col_type}) in {table_name}")

            sample = self.conn.execute(
                f'SELECT "{col_name}" FROM "{table_name}" WHERE "{col_name}" IS NOT NULL LIMIT 1'
            ).fetchone()

            if not sample or not sample[0]:
                self.logger.debug(f"No data found for column {col_name}")
                continue

            sample_value = sample[0]

            if isinstance(sample_value, list):
                self._create_array_child_table(table_name, col_name, snake_col_name, primary_key_col)
            elif isinstance(sample_value, dict):
                self._create_object_child_table(table_name, col_name, snake_col_name, primary_key_col)

    def _create_array_child_table(self, parent_table: str, column_name: str, snake_col_name: str, parent_pk: str):
        """Create a child table for array/list JSON columns"""
        child_table_name = f"{parent_table}_{snake_col_name}"

        try:
            self.conn.execute(f'DROP TABLE IF EXISTS "{child_table_name}"')

            self.conn.execute(f"""
                CREATE TABLE "{child_table_name}" AS
                SELECT
                    "{parent_pk}" as parent_id,
                    ROW_NUMBER() OVER (PARTITION BY "{parent_pk}" ORDER BY (SELECT NULL)) as row_number,
                    UNNEST("{column_name}") as item
                FROM "{parent_table}"
                WHERE "{column_name}" IS NOT NULL AND len("{column_name}") > 0
            """)

            item_columns = self.conn.execute(f'DESCRIBE "{child_table_name}"').fetchall()

            if any("STRUCT" in str(col[1]) for col in item_columns):
                flattened_table = f"{child_table_name}_flat"
                self.conn.execute(f'DROP TABLE IF EXISTS "{flattened_table}"')

                struct_cols = []
                for col in item_columns:
                    if col[0] == "item" and "STRUCT" in str(col[1]):
                        struct_fields = self.conn.execute(
                            f'SELECT * FROM (SELECT item FROM "{child_table_name}" LIMIT 1)'
                        ).fetchone()

                        if struct_fields and struct_fields[0]:
                            for key in struct_fields[0].keys():
                                snake_key = self._camel_to_snake(key)
                                struct_cols.append(f"item['{key}'] as {snake_key}")

                if struct_cols:
                    select_clause = f"parent_id, row_number, {', '.join(struct_cols)}"
                    self.conn.execute(f"""
                        CREATE TABLE "{flattened_table}" AS
                        SELECT {select_clause}
                        FROM "{child_table_name}"
                    """)

                    self.conn.execute(f'DROP TABLE "{child_table_name}"')
                    self.conn.execute(f'ALTER TABLE "{flattened_table}" RENAME TO "{child_table_name}"')

            normalized_child = self._normalize_table(child_table_name)
            self._export_table_with_manifest(child_table_name, normalized_child)

            if not self.params.debug and normalized_child != child_table_name:
                self.conn.execute(f'DROP TABLE IF EXISTS "{child_table_name}"')

            self.logger.info(f"Created child table: {child_table_name}")

        except Exception as e:
            self.logger.warning(f"Failed to decompose array column {column_name}: {str(e)}")

    def _create_object_child_table(self, parent_table: str, column_name: str, snake_col_name: str, parent_pk: str):
        """Create a child table for object JSON columns"""
        child_table_name = f"{parent_table}_{snake_col_name}"

        try:
            sample = self.conn.execute(
                f'SELECT "{column_name}" FROM "{parent_table}" WHERE "{column_name}" IS NOT NULL LIMIT 1'
            ).fetchone()

            if not sample or not sample[0]:
                return

            sample_obj = sample[0]
            if not isinstance(sample_obj, dict):
                return

            field_selects = [f'"{parent_pk}" as parent_id']
            for key in sample_obj.keys():
                snake_key = self._camel_to_snake(key)
                field_selects.append(f"\"{column_name}\"['{key}'] as {snake_key}")

            self.conn.execute(f'DROP TABLE IF EXISTS "{child_table_name}"')
            self.conn.execute(f"""
                CREATE TABLE "{child_table_name}" AS
                SELECT {", ".join(field_selects)}
                FROM "{parent_table}"
                WHERE "{column_name}" IS NOT NULL
            """)

            normalized_child = self._normalize_table(child_table_name)
            self._export_table_with_manifest(child_table_name, normalized_child)

            if not self.params.debug and normalized_child != child_table_name:
                self.conn.execute(f'DROP TABLE IF EXISTS "{child_table_name}"')

            self.logger.info(f"Created child table: {child_table_name}")

        except Exception as e:
            self.logger.warning(f"Failed to decompose object column {column_name}: {str(e)}")

    def _parse_date_to_iso(self, date_str: str | None) -> str | None:
        """
        Parse date string (ISO format or relative like '1 week ago') to ISO format for Shopify API

        Args:
            date_str: Date string in ISO format (YYYY-MM-DD) or relative format ('1 week ago', 'now', etc.)

        Returns:
            ISO formatted date string (YYYY-MM-DD) or None if input is None
        """
        if not date_str:
            return None

        date_str = date_str.strip()

        try:
            parsed_date = dateparser.parse(date_str)

            if parsed_date is None:
                raise UserException(
                    f"Could not parse date '{date_str}'. Please use ISO format (YYYY-MM-DD) or relative format "
                    "like '1 week ago', 'now', etc."
                )

            return parsed_date.strftime(r"%Y-%m-%d")

        except Exception as e:
            raise UserException(f"Invalid date format '{date_str}': {str(e)}")

    def run(self):
        """
        Main execution code
        """
        params = Configuration(**self.configuration.parameters)

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
            "products_unlisted": self._extract_products_bulk,
            "products_legacy": self._extract_products_legacy,
            "orders": self._extract_orders_bulk,
            "orders_legacy": self._extract_orders_legacy,
            "customers": self._extract_customers_bulk,
            "customers_legacy": self._extract_customers_legacy,
            "inventory": self._extract_inventory_bulk,
            "inventory_legacy": self._extract_inventory_levels,
            "locations": self._extract_locations_bulk,
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

        all_orders = []
        for batch in client.get_orders(
            date_since=self._parse_date_to_iso(params.loading_options.date_since),
            date_to=self._parse_date_to_iso(params.loading_options.date_to),
            batch_size=params.batch_size,
        ):
            all_orders.extend(batch)

        if all_orders:
            self._process_with_duckdb("orders_legacy", all_orders, params)
            self.logger.info(f"Successfully extracted {len(all_orders)} orders")
        else:
            self.logger.info("No orders found")

    def _extract_orders_bulk(self, client: ShopifyGraphQLClient, params: Configuration):
        """Extract orders using Shopify bulk operations"""
        self.logger.info("Extracting orders using bulk operations")

        with tempfile.NamedTemporaryFile(mode="w+", suffix=".jsonl", delete=False) as tmp:
            temp_jsonl = tmp.name

        result = client.get_orders_bulk(
            temp_jsonl,
            include_transactions=params.endpoints.order_transactions,
            date_since=self._parse_date_to_iso(params.loading_options.date_since),
            date_to=self._parse_date_to_iso(params.loading_options.date_to),
            fetch_parameter=params.loading_options.fetch_parameter,
        )

        if result.item_count > 0:
            self._process_bulk_orders(result)
        else:
            self.logger.info("No orders found")
            Path(result.file_path).unlink(missing_ok=True)

    def _extract_products_legacy(self, client: ShopifyGraphQLClient, params: Configuration):
        """Extract products data using DuckDB (legacy one-by-one method)"""
        self.logger.info("Extracting products data (legacy method)")

        all_products = []
        for batch in client.get_products(batch_size=params.batch_size):
            all_products.extend(batch)

        if all_products:
            self._process_with_duckdb("products_legacy", all_products, params)
            self.logger.info(f"Successfully extracted {len(all_products)} products")
        else:
            self.logger.info("No products found")

    def _extract_products_bulk(self, client: ShopifyGraphQLClient, params: Configuration):
        """Extract products using Shopify bulk operations"""
        self.logger.info("Extracting products using bulk operations")

        statuses = []
        if params.endpoints.products:
            statuses.append("active")
        if params.endpoints.products_drafts:
            statuses.append("draft")
        if params.endpoints.products_archived:
            statuses.append("archived")
        if params.endpoints.products_unlisted:
            statuses.append("unlisted")

        if not statuses:
            self.logger.warning("No product status selected, skipping products extraction")
            return

        status_filter = ",".join(statuses)
        self.logger.info(f"Fetching products with statuses: {status_filter}")

        with tempfile.NamedTemporaryFile(mode="w+", suffix=".jsonl", delete=False) as tmp:
            temp_jsonl = tmp.name

        result = client.get_products_bulk(
            temp_jsonl,
            status=status_filter,
            include_product_metafields=params.endpoints.product_metafields,
            include_variant_metafields=params.endpoints.variant_metafields,
            date_since=self._parse_date_to_iso(params.loading_options.date_since),
            date_to=self._parse_date_to_iso(params.loading_options.date_to),
            fetch_parameter=params.loading_options.fetch_parameter,
        )

        if result.item_count > 0:
            self._process_bulk_products(result)
        else:
            self.logger.info("No products found")
            Path(result.file_path).unlink(missing_ok=True)

    def _process_bulk_result(self, bulk_result: BulkOperationResult, table_name: str, entity_name: str | None = None):
        """Generic method to process bulk operation results"""
        if entity_name is None:
            entity_name = table_name
        process_start = time.time()

        self.logger.info(f"Processing {bulk_result.item_count} {entity_name} from {bulk_result.file_path}")

        try:
            entity_keys = self._scan_jsonl_keys(bulk_result.file_path)

            self.conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
            self.conn.execute(
                f"CREATE TABLE \"{table_name}\" AS SELECT * FROM read_json_auto('{bulk_result.file_path}')"
            )

            normalized_table = self._normalize_table(table_name)
            self._export_table_with_manifest(table_name, normalized_table, entity_keys)
            self._decompose_json_columns(table_name, normalized_table)

            if not self.params.debug:
                self.conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')

            result_count = self.conn.execute(f"SELECT COUNT(*) FROM {normalized_table}").fetchone()
            row_count = result_count[0] if result_count else 0

            process_time = time.time() - process_start
            self.logger.info(
                f"{entity_name.capitalize()} processing complete: {row_count} items in {process_time:.2f}s "
                f"(API wait: {bulk_result.api_wait_time:.2f}s, download: {bulk_result.download_time:.2f}s, "
                f"process: {process_time:.2f}s)"
            )
        finally:
            if self.params.debug:
                debug_file = f"bulk_{table_name}_download.jsonl"
                shutil.copy2(bulk_result.file_path, debug_file)
            Path(bulk_result.file_path).unlink(missing_ok=True)

    def _process_bulk_products(self, bulk_result: BulkOperationResult):
        self._process_bulk_result(bulk_result, "product")

    def _process_bulk_orders(self, bulk_result: BulkOperationResult):
        self._process_bulk_result(bulk_result, "order")

    def _extract_customers_legacy(self, client: ShopifyGraphQLClient, params: Configuration):
        """Extract customers data using DuckDB (legacy one-by-one method)"""
        self.logger.info("Extracting customers data (legacy method)")

        all_customers = []
        for batch in client.get_customers(batch_size=params.batch_size):
            all_customers.extend(batch)

        if all_customers:
            self._process_with_duckdb("customers_legacy", all_customers, params)
            self.logger.info(f"Successfully extracted {len(all_customers)} customers")
        else:
            self.logger.info("No customers found")

    def _extract_customers_bulk(self, client: ShopifyGraphQLClient, params: Configuration):
        """Extract customers using Shopify bulk operations"""
        self.logger.info("Extracting customers using bulk operations")

        with tempfile.NamedTemporaryFile(mode="w+", suffix=".jsonl", delete=False) as tmp:
            temp_jsonl = tmp.name

        result = client.get_customers_bulk(
            temp_jsonl,
            date_since=self._parse_date_to_iso(params.loading_options.date_since),
            date_to=self._parse_date_to_iso(params.loading_options.date_to),
            fetch_parameter=params.loading_options.fetch_parameter,
        )

        if result.item_count > 0:
            self._process_bulk_customers(result)
        else:
            self.logger.info("No customers found")
            Path(result.file_path).unlink(missing_ok=True)

    def _process_bulk_customers(self, bulk_result: BulkOperationResult):
        self._process_bulk_result(bulk_result, "customer")

    def _extract_inventory_bulk(self, client: ShopifyGraphQLClient, params: Configuration):
        """Extract inventory using Shopify bulk operations"""
        self.logger.info("Extracting inventory using bulk operations")

        with tempfile.NamedTemporaryFile(mode="w+", suffix=".jsonl", delete=False) as tmp:
            temp_jsonl = tmp.name

        result = client.get_inventory_bulk(
            temp_jsonl,
            date_since=self._parse_date_to_iso(params.loading_options.date_since),
            date_to=self._parse_date_to_iso(params.loading_options.date_to),
            fetch_parameter=params.loading_options.fetch_parameter,
        )

        if result.item_count > 0:
            self._process_bulk_inventory(result)
        else:
            self.logger.info("No inventory found")
            Path(result.file_path).unlink(missing_ok=True)

    def _process_bulk_inventory(self, bulk_result: BulkOperationResult):
        self._process_bulk_result(bulk_result, "inventory", "inventory items")

    def _extract_locations_bulk(self, client: ShopifyGraphQLClient, params: Configuration):
        """Extract locations using Shopify bulk operations"""
        self.logger.info("Extracting locations using bulk operations")

        with tempfile.NamedTemporaryFile(mode="w+", suffix=".jsonl", delete=False) as tmp:
            temp_jsonl = tmp.name

        result = client.get_locations_bulk(temp_jsonl)

        if result.item_count > 0:
            self._process_bulk_locations(result)
        else:
            self.logger.info("No locations found")
            Path(result.file_path).unlink(missing_ok=True)

    def _process_bulk_locations(self, bulk_result: BulkOperationResult):
        self._process_bulk_result(bulk_result, "location")

    def _extract_inventory_levels(self, client: ShopifyGraphQLClient, params: Configuration):
        """Extract inventory levels data using DuckDB"""
        self.logger.info("Extracting inventory levels data")

        all_inventory_levels = []
        for batch in client.get_inventory_levels(batch_size=params.batch_size):
            all_inventory_levels.extend(batch)

        if all_inventory_levels:
            self._process_with_duckdb("inventory_levels", all_inventory_levels, params)
            self.logger.info(f"Successfully extracted {len(all_inventory_levels)} inventory levels")
        else:
            self.logger.info("No inventory levels found")

    def _extract_events(self, client: ShopifyGraphQLClient, params: Configuration):
        """Extract events using Shopify bulk operations"""
        self.logger.info("Extracting events using bulk operations")

        with tempfile.NamedTemporaryFile(mode="w+", suffix=".jsonl", delete=False) as tmp:
            temp_jsonl = tmp.name

        result = client.get_events_bulk(
            temp_jsonl,
            date_since=self._parse_date_to_iso(params.loading_options.date_since),
            date_to=self._parse_date_to_iso(params.loading_options.date_to),
        )

        if result.item_count > 0:
            self._process_bulk_events(result)
        else:
            self.logger.info("No events found")
            Path(result.file_path).unlink(missing_ok=True)

    def _process_bulk_events(self, bulk_result: BulkOperationResult):
        self._process_bulk_result(bulk_result, "event")

    def _process_with_duckdb(self, table_name: str, data: list[dict[str, Any]], params: Configuration):
        """
        Process data using DuckDB for type detection and normalization
        """
        if not data:
            return

        file_def = self.create_out_file_definition(f"{table_name}_temp.json")
        temp_json = Path(file_def.full_path)
        with open(temp_json, "w") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        try:
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE {table_name}_raw AS
                SELECT * FROM read_json_auto('{temp_json}')
            """)

            if table_name == "orders":
                self._create_orders_tables(table_name)
            elif table_name == "products":
                self._create_products_tables(table_name)
            elif table_name == "inventory_items":
                self._create_inventory_tables(table_name)
            else:
                self._export_table_with_manifest(f"{table_name}_raw")

        finally:
            if not self.configuration.parameters.debug:
                temp_json.unlink()

    def _create_orders_tables(self, table_name: str):
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

        self._export_table_with_manifest("orders")
        self._export_table_with_manifest("order_line_items")

    def _create_products_tables(self, table_name: str):
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

        self._export_table_with_manifest("products")
        self._export_table_with_manifest("product_variants")

    def _create_inventory_tables(self, table_name: str):
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

        self._export_table_with_manifest("inventory_items")
        self._export_table_with_manifest("inventory_levels")

    def _export_table_with_manifest(
        self, table_name: str, normalized_table: str | None = None, entity_keys: dict[str, set[str]] | None = None
    ):
        if normalized_table is None:
            normalized_table = table_name
        table_meta = self.conn.execute(f'DESCRIBE "{normalized_table}"').fetchall()

        has_id = any(col[0] == "id" for col in table_meta)
        entity_types = []

        if has_id:
            try:
                entity_types_result = self.conn.execute(f"""
                    SELECT DISTINCT regexp_extract(id, 'gid://shopify/([^/]+)/', 1) as entity_type
                    FROM "{normalized_table}"
                    WHERE id IS NOT NULL AND id LIKE 'gid://shopify/%'
                """).fetchall()
                entity_types = [et[0] for et in entity_types_result if et[0]]
            except Exception:
                pass

        if len(entity_types) > 1:
            self.logger.info(f"Splitting {table_name} by entity types: {', '.join(entity_types)}")
            for entity_type in entity_types:
                snake_entity = self._camel_to_snake(entity_type)
                self._export_entity_type(normalized_table, snake_entity, entity_type, table_meta, entity_keys)
        else:
            self._export_single_table(table_name, normalized_table, table_meta)

    def _export_entity_type(
        self,
        normalized_table: str,
        entity_name: str,
        entity_type: str,
        table_meta: list,
        entity_keys: dict[str, set[str]] | None,
    ):
        if entity_keys and entity_type in entity_keys:
            jsonl_keys_snake = {self._camel_to_snake(k) for k in entity_keys[entity_type]}
            valid_columns = [c[0] for c in table_meta if c[0] in jsonl_keys_snake]
        else:
            valid_columns = [c[0] for c in table_meta]

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
            f"{entity_name}.csv",
            schema=schema,
            primary_key=self._get_primary_key(entity_name),
            incremental=bool(self.params.loading_options.incremental_output),
            has_header=True,
        )

        try:
            column_list = ", ".join([f'"{col}"' for col in valid_columns])
            q = f"""
                COPY (
                    SELECT {column_list}
                    FROM "{normalized_table}"
                    WHERE id LIKE 'gid://shopify/{entity_type}/%'
                ) TO '{out_table.full_path}' (HEADER, DELIMITER ',', FORCE_QUOTE *)
            """
            logging.debug(f"Running query: {q}; ")
            self.conn.execute(q)
            self.write_manifest(out_table)
            self.logger.info(f"Exported entity type: {entity_name} ({entity_type})")
        except duckdb.ConversionException as e:
            raise UserException(f"Error during query execution: {e}")

    def _export_single_table(self, table_name: str, normalized_table: str, table_meta: list):
        schema = OrderedDict(
            {
                c[0]: ColumnDefinition(
                    data_types=BaseType(dtype=self.convert_base_types(c[1])),
                    primary_key=False,
                )
                for c in table_meta
            }
        )

        out_table = self.create_out_table_definition(
            f"{table_name}.csv",
            schema=schema,
            primary_key=self._get_primary_key(table_name),
            incremental=bool(self.params.loading_options.incremental_output),
            has_header=True,
        )

        try:
            q = f"COPY \"{normalized_table}\" TO '{out_table.full_path}' (HEADER, DELIMITER ',', FORCE_QUOTE *)"
            logging.debug(f"Running query: {q}; ")
            self.conn.execute(q)
            self.write_manifest(out_table)
        except duckdb.ConversionException as e:
            raise UserException(f"Error during query execution: {e}")

    def _get_primary_key(self, table_name: str) -> list[str]:
        """Define primary keys for different tables"""
        primary_keys = {
            "order": ["id"],
            "order_legacy": ["id"],
            "product": ["id"],
            "product_legacy": ["id"],
            "customer": ["id"],
            "customer_legacy": ["id"],
            "inventory": ["id"],
            "inventory_item": ["id"],
            "inventory_level": ["__parent_id", "id"],
            "location": ["id"],
            "event": ["id"],
        }

        if table_name in primary_keys:
            return primary_keys[table_name]

        if "_" in table_name:
            try:
                columns = [col[0] for col in self.conn.execute(f'DESCRIBE "{table_name}"').fetchall()]
                if "parent_id" in columns and "row_number" in columns:
                    return ["parent_id", "row_number"]
                elif "parent_id" in columns:
                    return ["parent_id"]
                elif "id" in columns:
                    return ["id"]
            except Exception:
                pass

        return []

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

        with tempfile.NamedTemporaryFile(mode="w+", suffix=".jsonl", delete=False) as tmp:
            temp_jsonl = tmp.name

        result = client.execute_custom_bulk_query(custom_query.query, temp_jsonl)

        if result.item_count > 0:
            self._process_bulk_custom(result, custom_query.name)
        else:
            self.logger.info(f"Custom bulk query '{custom_query.name}' returned no results")
            Path(result.file_path).unlink(missing_ok=True)

    def _process_bulk_custom(self, bulk_result: BulkOperationResult, table_name: str):
        self._process_bulk_result(bulk_result, table_name, f"custom query '{table_name}'")

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
