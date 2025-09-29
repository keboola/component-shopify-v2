# src/component.py
import duckdb
import json
import logging
from typing import Any
from pathlib import Path

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

from configuration import Configuration
from shopify_cli.client import ShopifyGraphQLClient


class Component(ComponentBase):
    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(__name__)
        # Inicializace DuckDB
        self.conn = duckdb.connect()

    def run(self):
        """
        Main execution code
        """
        params = Configuration(**self.configuration.parameters)

        # Initialize Shopify client
        client = ShopifyGraphQLClient(
            store_name=params.store_name, 
            api_token=params.api_token, 
            api_version=params.api_version
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
            date_from=params.date_from, 
            date_to=params.date_to, 
            batch_size=params.batch_size
        ):
            all_orders.extend(batch)

        if all_orders:
            self._process_with_duckdb("orders", all_orders, params)
            self.logger.info(f"Successfully extracted {len(all_orders)} orders")
        else:
            self.logger.info("No orders found")

    def _process_with_duckdb(self, table_name: str, data: list[dict[str, Any]], params: Configuration):
        """
        Process data using DuckDB for type detection and normalization
        """
        if not data:
            return

        # Create temporary JSON file
        temp_json = self.data_out_tables / f"{table_name}_temp.json"
        with open(temp_json, 'w', encoding='utf-8') as f:
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
                self._export_simple_table(table_name, schema_info)

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
        output_file = self.data_out_tables / f"{output_name}.csv"
        
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
            "metadata": [
                {
                    "key": "KBC.createdBy.component.id",
                    "value": "ex-shopify-v2"
                }
            ],
            "column_metadata": []
        }

        # Add column type metadata
        for col_name, col_type, null, key, default, extra in columns_info:
            # Map DuckDB types to Keboola base types
            keboola_type = self._map_duckdb_to_keboola_type(col_type)
            manifest["column_metadata"].append({
                "key": f"KBC.datatype.basetype.{col_name}",
                "value": keboola_type
            })

        # Write manifest
        manifest_file = output_file.with_suffix('.csv.manifest')
        with open(manifest_file, 'w', encoding='utf-8') as f:
            json.dump(manifest, f, indent=2, ensure_ascii=False)

    def _map_duckdb_to_keboola_type(self, duckdb_type: str) -> str:
        """Map DuckDB types to Keboola base types"""
        type_mapping = {
            'VARCHAR': 'STRING',
            'BIGINT': 'INTEGER',
            'INTEGER': 'INTEGER',
            'DOUBLE': 'FLOAT',
            'DECIMAL': 'NUMERIC',
            'BOOLEAN': 'BOOLEAN',
            'DATE': 'DATE',
            'TIMESTAMP': 'TIMESTAMP',
            'TIMESTAMPTZ': 'TIMESTAMP',
        }
        
        # Handle complex types
        if duckdb_type.startswith('VARCHAR'):
            return 'STRING'
        elif duckdb_type.startswith('DECIMAL'):
            return 'NUMERIC'
        elif duckdb_type.startswith('DOUBLE'):
            return 'FLOAT'
        
        return type_mapping.get(duckdb_type, 'STRING')

    def _get_primary_key(self, table_name: str) -> list[str]:
        """Define primary keys for different tables"""
        primary_keys = {
            'orders': ['id'],
            'order_line_items': ['orderId', 'lineItemId'],
            'products': ['id'],
            'product_variants': ['productId', 'variantId'],
            'inventory_items': ['id'],
            'inventory_levels': ['inventoryItemId', 'levelId'],
            'customers': ['id'],
            'locations': ['id'],
        }
        return primary_keys.get(table_name, ['id'])

    # ... ostatní extract metody zůstávají stejné, jen volají _process_with_duckdb