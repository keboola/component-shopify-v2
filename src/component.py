# src/component.py
import csv
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
from keboola.vcr.sanitizers import QueryParamSanitizer

from configuration import PRODUCTS_ENDPOINTS, Configuration
from shopify_cli.auth import ShopifyTokenManager
from shopify_cli.client import BulkOperationResult, ShopifyGraphQLClient

# ---------------------------------------------------------------------------
# VCR sanitizers — auto-discovered by keboola.datadirtest and platform debug jobs.
# Redacts short-lived GCS signed URL credentials (GoogleAccessId, Signature, Expires)
# that Shopify returns as bulk operation download links.
# ---------------------------------------------------------------------------

VCR_SANITIZERS = [
    QueryParamSanitizer(
        parameters=["GoogleAccessId", "Expires", "Signature"],
        replacement="REDACTED",
    )
]

# Columns intentionally excluded from generic JSON decomposition into child tables.
# `originalUnitPriceSet` is line-item-level data, but decomposition runs on the mixed
# top-level table and would emit a misnamed `order_original_unit_price_set` child table.
# It remains available as the serialized `original_unit_price_set` JSON column on line_item.
# Proper per-entity child tables are introduced by PR 3 of L1-141; suppressing this here
# avoids shipping a name that PR 3 would have to rename.
DECOMPOSITION_SKIP_COLUMNS = {"originalUnitPriceSet"}

# Order.customerJourneySummary is flattened explicitly onto the `order` table instead of
# going through the generic decomposition path. Generic decomposition would emit a 1:1
# `order_customer_journey_summary` child table, and a plain skip would leave a single
# serialized JSON blob; the customer's dbt mapping (SUPPORT-12550) depends on flat, __-separated columns
# on the `order` table (e.g. landing_site <- customer_journey_summary__first_visit__landing_page).
#
# Attribution is asynchronous: `ready` may be false with null visit fields, and
# customerJourneySummary itself may be null (Shopify docs). All columns below are therefore
# always produced (NULL where absent), with no data-dependent column presence.
CUSTOMER_JOURNEY_SOURCE_COLUMN = "customerJourneySummary"

# Order.lineItems.nodes.product is a plain nullable object field that stays inline on the
# LineItem rows of the mixed bulk `order` stream (bulk splits rows per connection only). Left
# as-is, the dict `product` column would reach the generic decomposition path and be emitted as
# a misnamed `order_product` child table keyed by a LineItem GID (the same trap suppressed for
# originalUnitPriceSet). Instead we extract product.id into a flat `product_id` column on the
# `line_item` entity and drop the struct before decomposition. product may be null (the line
# item's product was deleted), so `product_id` is always produced (NULL where absent), with no
# data-dependent column presence.
LINE_ITEM_PRODUCT_SOURCE_COLUMN = "product"
LINE_ITEM_PRODUCT_ID_COLUMN = "product_id"
LINE_ITEM_ENTITY_TYPE = "LineItem"

# Line-item-level plain lists (taxLines, discountAllocations) live inline on the LineItem
# rows of the mixed bulk `order` stream. The generic decomposition path runs on the whole
# stream and would emit these as `order_*` child tables keyed by a LineItem GID. Instead we
# decompose them entity-aware: each list is scoped to its owning GID entity and emitted under
# an entity-derived prefix (line_item_*), with money flattened into __-separated columns
# (matching the SUPPORT-12550 column spec) rather than left as a serialized shop_money JSON blob.
#
# Each entry maps the source column (camelCase, as read from the bulk JSONL) to the owning
# entity type and the ordered output columns. Each output column is (name, json_path), where
# json_path is evaluated against a single unnested list element.
ENTITY_CHILD_LIST_TABLES: dict[str, dict[str, Any]] = {
    "taxLines": {
        "entity_type": "LineItem",
        "columns": [
            ("title", "$.title"),
            ("rate", "$.rate"),
            ("rate_percentage", "$.ratePercentage"),
            ("price_set__shop_money__amount", "$.priceSet.shopMoney.amount"),
            ("price_set__shop_money__currency_code", "$.priceSet.shopMoney.currencyCode"),
            ("channel_liable", "$.channelLiable"),
            ("source", "$.source"),
        ],
    },
    "discountAllocations": {
        "entity_type": "LineItem",
        "columns": [
            ("amount_set__shop_money__amount", "$.allocatedAmountSet.shopMoney.amount"),
            ("amount_set__shop_money__currency_code", "$.allocatedAmountSet.shopMoney.currencyCode"),
            ("discount_application_index", "$.discountApplication.index"),
        ],
    },
}


def _build_customer_journey_columns() -> list[tuple[str, str]]:
    """Return ordered (output_column_name, json_path) pairs for the flattened columns.

    json_path is relative to the customerJourneySummary object and uses the GraphQL
    (camelCase) field names, matching the keys DuckDB reads from the bulk JSONL.
    """
    prefix = "customer_journey_summary"
    top_fields = [
        ("ready", "ready"),
        ("customer_order_index", "customerOrderIndex"),
        ("days_to_conversion", "daysToConversion"),
    ]
    visit_fields = [
        ("id", "id"),
        ("occurred_at", "occurredAt"),
        ("landing_page", "landingPage"),
        ("referrer_url", "referrerUrl"),
        ("source", "source"),
        ("source_description", "sourceDescription"),
        ("source_type", "sourceType"),
        ("referral_code", "referralCode"),
        ("utm_parameters__source", "utmParameters.source"),
        ("utm_parameters__medium", "utmParameters.medium"),
        ("utm_parameters__campaign", "utmParameters.campaign"),
        ("utm_parameters__term", "utmParameters.term"),
        ("utm_parameters__content", "utmParameters.content"),
    ]
    visits = [("first_visit", "firstVisit"), ("last_visit", "lastVisit")]

    columns = [(f"{prefix}__{name}", f"$.{path}") for name, path in top_fields]
    for visit_col, visit_path in visits:
        for name, path in visit_fields:
            columns.append((f"{prefix}__{visit_col}__{name}", f"$.{visit_path}.{path}"))
    return columns


CUSTOMER_JOURNEY_SUMMARY_COLUMNS = _build_customer_journey_columns()


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

    def _flatten_customer_journey_summary(self, table_name: str, entity_keys: dict[str, set[str]] | None) -> None:
        """Flatten Order.customerJourneySummary into fixed __-separated columns on the order table.

        Extracts the customerJourneySummary struct into the columns defined by
        CUSTOMER_JOURNEY_SUMMARY_COLUMNS and drops the original struct column so the generic
        decomposition never emits an `order_customer_journey_summary` child table. Values are
        read via JSON path extraction, so every column is produced even when the struct, a
        visit, or individual fields are absent (asynchronous attribution).
        """
        columns = [c[0] for c in self.conn.execute(f'DESCRIBE "{table_name}"').fetchall()]
        has_source = CUSTOMER_JOURNEY_SOURCE_COLUMN in columns

        projections = [f'* EXCLUDE ("{CUSTOMER_JOURNEY_SOURCE_COLUMN}")'] if has_source else ["*"]
        for out_name, json_path in CUSTOMER_JOURNEY_SUMMARY_COLUMNS:
            if has_source:
                projections.append(
                    f'json_extract_string(to_json("{CUSTOMER_JOURNEY_SOURCE_COLUMN}"), \'{json_path}\') AS "{out_name}"'
                )
            else:
                projections.append(f'CAST(NULL AS VARCHAR) AS "{out_name}"')

        self.conn.execute(
            f'CREATE OR REPLACE TABLE "{table_name}" AS SELECT {", ".join(projections)} FROM "{table_name}"'
        )

        # Keep the flattened columns (and drop the raw struct key) in the Order entity's key set
        # so the entity-split export retains them.
        if entity_keys and "Order" in entity_keys:
            order_keys = entity_keys["Order"]
            order_keys.discard(CUSTOMER_JOURNEY_SOURCE_COLUMN)
            order_keys.update(name for name, _ in CUSTOMER_JOURNEY_SUMMARY_COLUMNS)

    def _flatten_line_item_product(self, table_name: str, entity_keys: dict[str, set[str]] | None) -> None:
        """Extract lineItems.product.id into a flat product_id column and drop the struct.

        product is a plain nullable object inline on the LineItem rows of the mixed bulk stream.
        Generic decomposition would otherwise emit a misnamed order_product child table keyed by
        a LineItem GID, so the id is flattened onto line_item here and the struct is dropped
        before decomposition. product may be null (deleted product), so product_id is always
        produced (NULL where absent) via JSON path extraction.
        """
        columns = [c[0] for c in self.conn.execute(f'DESCRIBE "{table_name}"').fetchall()]
        has_source = LINE_ITEM_PRODUCT_SOURCE_COLUMN in columns

        projections = [f'* EXCLUDE ("{LINE_ITEM_PRODUCT_SOURCE_COLUMN}")'] if has_source else ["*"]
        if has_source:
            projections.append(
                f"json_extract_string(to_json(\"{LINE_ITEM_PRODUCT_SOURCE_COLUMN}\"), '$.id') "
                f'AS "{LINE_ITEM_PRODUCT_ID_COLUMN}"'
            )
        else:
            projections.append(f'CAST(NULL AS VARCHAR) AS "{LINE_ITEM_PRODUCT_ID_COLUMN}"')

        self.conn.execute(
            f'CREATE OR REPLACE TABLE "{table_name}" AS SELECT {", ".join(projections)} FROM "{table_name}"'
        )

        # Keep the flattened column (and drop the raw struct key) in the LineItem entity's key
        # set so the entity-split export retains product_id.
        if entity_keys and LINE_ITEM_ENTITY_TYPE in entity_keys:
            line_item_keys = entity_keys[LINE_ITEM_ENTITY_TYPE]
            line_item_keys.discard(LINE_ITEM_PRODUCT_SOURCE_COLUMN)
            line_item_keys.add(LINE_ITEM_PRODUCT_ID_COLUMN)

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

            if col_name in DECOMPOSITION_SKIP_COLUMNS or col_name in ENTITY_CHILD_LIST_TABLES:
                self.logger.info(f"Skipping generic decomposition for column: {col_name} in {table_name}")
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

    def _decompose_entity_child_lists(self, table_name: str) -> None:
        """Decompose line-item-level plain lists into entity-prefixed child tables.

        The bulk `order` stream is a mixed table where LineItem rows carry plain list columns
        (taxLines, discountAllocations) inline. Generic decomposition runs on the whole stream
        and would emit these as `order_*` tables keyed by a LineItem GID. Instead, each list is
        scoped to its owning GID entity (id LIKE 'gid://shopify/<Entity>/%') and emitted under an
        entity-derived prefix (line_item_*), with money flattened into __-separated columns.
        """
        existing_columns = {c[0] for c in self.conn.execute(f'DESCRIBE "{table_name}"').fetchall()}

        for source_column, spec in ENTITY_CHILD_LIST_TABLES.items():
            if source_column not in existing_columns:
                continue
            entity_type = spec["entity_type"]
            child_table = f"{self._camel_to_snake(entity_type)}_{self._camel_to_snake(source_column)}"
            self._create_entity_child_list_table(table_name, source_column, entity_type, child_table, spec["columns"])

    def _create_entity_child_list_table(
        self,
        parent_table: str,
        source_column: str,
        entity_type: str,
        child_table: str,
        column_spec: list[tuple[str, str]],
    ) -> None:
        """Build one entity-scoped child table from a plain list column with flattened columns.

        row_number is the deterministic 1-based index of each element within its parent's list:
        UNNEST of a parallel range zips positionally with UNNEST of the list, so the index always
        matches the element's array position (unlike ROW_NUMBER() OVER (ORDER BY (SELECT NULL))).
        """
        row_source = f"""
            SELECT
                "id" AS parent_id,
                UNNEST(range(1, len("{source_column}") + 1)) AS row_number,
                UNNEST("{source_column}") AS item
            FROM "{parent_table}"
            WHERE id LIKE 'gid://shopify/{entity_type}/%'
              AND "{source_column}" IS NOT NULL
              AND len("{source_column}") > 0
        """

        try:
            count_result = self.conn.execute(f"SELECT COUNT(*) FROM ({row_source})").fetchone()
            if not count_result or count_result[0] == 0:
                self.logger.debug(f"No rows for entity child table {child_table}; skipping")
                return

            projections = ["parent_id", "row_number"]
            for out_name, json_path in column_spec:
                projections.append(f"json_extract_string(to_json(item), '{json_path}') AS \"{out_name}\"")

            self.conn.execute(f'DROP TABLE IF EXISTS "{child_table}"')
            self.conn.execute(f'CREATE TABLE "{child_table}" AS SELECT {", ".join(projections)} FROM ({row_source})')

            self._export_table_with_manifest(child_table)

            if not self.params.debug:
                self.conn.execute(f'DROP TABLE IF EXISTS "{child_table}"')

            self.logger.info(f"Created entity child table: {child_table}")
        except Exception as e:
            self.logger.warning(f"Failed to decompose entity child list {source_column}: {str(e)}")

    def _create_array_child_table(self, parent_table: str, column_name: str, snake_col_name: str, parent_pk: str):
        """Create a child table for array/list JSON columns"""
        child_table_name = f"{parent_table}_{snake_col_name}"

        try:
            self.conn.execute(f'DROP TABLE IF EXISTS "{child_table_name}"')

            # row_number is the deterministic 1-based index of each element within its parent's
            # list: UNNEST of a parallel range zips positionally with UNNEST of the list, so every
            # element keeps a distinct position. ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) is
            # evaluated before UNNEST expands the list, which collapsed every element to row 1 and
            # (with PK (parent_id, row_number) + incremental load) silently deduped all but one.
            self.conn.execute(f"""
                CREATE TABLE "{child_table_name}" AS
                SELECT
                    "{parent_pk}" as parent_id,
                    UNNEST(range(1, len("{column_name}") + 1)) as row_number,
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

    def _parse_loading_option_dates(self, date_since: str | None, date_to: str | None) -> tuple[str | None, str | None]:
        """Parse date_since and date_to into Shopify search bounds.

        The window is rounded outward, never inward, and the two bounds are deliberately asymmetric:

        - Upper bound (date_to) keeps full ISO-8601 UTC timestamp precision so that ``date_to="now"``
          (or any relative value) resolves to the actual run moment, e.g. ``updated_at:<'2026-07-10T13:56:13Z'``.
          Truncating it to bare ``YYYY-MM-DD`` snaps it back to midnight of the run day and silently drops
          every record updated earlier that same day.
        - Lower bound (date_since) is intentionally floored to midnight of its day. A timestamp-precise
          lower bound would open gaps between consecutive incremental runs using relative date_since values:
          yesterday's run covers up to its own start time, while today's ">= 1 day ago" would begin later in
          the day, leaving the intervening records uncovered. Flooring to midnight keeps the windows overlapping.

        Dates are resolved in UTC (``TIMEZONE="UTC"``): the previous ``parse_datetime_interval`` truncated
        both bounds to bare ``YYYY-MM-DD`` (the source of the same-day-exclusion bug) and, being unable to
        pass a timezone through, resolved values in the container's local timezone. Resolving in UTC makes
        relative values ("now", "7 years ago") real UTC instants and anchors explicit calendar dates
        ("2026-03-19") to UTC midnight without a timezone shift, so the trailing ``Z`` is always accurate
        regardless of the runtime timezone.
        """
        if not date_since and not date_to:
            return None, None
        settings = {"TIMEZONE": "UTC", "RETURN_AS_TIMEZONE_AWARE": True}
        start = dateparser.parse(date_since or "1970-01-01", settings=settings)
        end = dateparser.parse(date_to or "now", settings=settings)
        if start is None or end is None:
            bad = date_since if start is None else date_to
            raise UserException(
                f"Could not parse date '{bad}'. Please use ISO format (YYYY-MM-DD) or relative format "
                "like '1 week ago', 'now', etc."
            )
        if end < start:
            raise UserException(f"date_since ('{date_since}') cannot be after date_to ('{date_to}').")
        floored_start = start.strftime("%Y-%m-%d") if date_since else None
        timestamp_end = end.strftime("%Y-%m-%dT%H:%M:%SZ") if date_to else None
        return floored_start, timestamp_end

    def _resolve_access_token(self, params: Configuration) -> str:
        """Resolve the access token based on auth mode.

        For legacy static tokens, returns the token directly.
        For client credentials (Dev Dashboard apps), exchanges credentials for a short-lived token,
        using cached token from state if still valid.

        Returns:
            Valid Shopify Admin API access token.
        """
        if not params.uses_client_credentials:
            self.logger.info("Using static Admin API access token (legacy auth)")
            return params.api_token

        self.logger.info("Using client credentials authentication (Dev Dashboard app)")
        state = self.get_state_file() or {}

        token_manager = ShopifyTokenManager(
            store_name=params.store_name,
            client_id=params.client_id,
            client_secret=params.client_secret,
        )
        access_token = token_manager.get_access_token(state)

        # Persist token to state for reuse within its validity window
        token_state = token_manager.get_token_state()
        state.update(token_state)
        self.write_state_file(state)
        self.logger.info("Access token saved to state file")

        return access_token

    def run(self):
        """
        Main execution code
        """
        params = Configuration(**self.configuration.parameters)

        api_token = self._resolve_access_token(params)

        client = ShopifyGraphQLClient(
            store_name=params.store_name,
            api_token=api_token,
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
                self._process_custom_query(client, custom_query, params)

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
            "collections": self._extract_collections_bulk,
            "locations": self._extract_locations_bulk,
            "events": self._extract_events,
            "order_refunds": self._extract_order_refunds,
            "order_shipping_discounts": self._extract_order_shipping_discounts,
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

        date_since, date_to = self._parse_loading_option_dates(
            params.loading_options.date_since, params.loading_options.date_to
        )
        all_orders = []
        for batch in client.get_orders(
            date_since=date_since,
            date_to=date_to,
            batch_size=params.batch_size,
        ):
            all_orders.extend(batch)

        if all_orders:
            self._process_with_duckdb("orders_legacy", all_orders, params)
            self.logger.info(f"Successfully extracted {len(all_orders)} orders")
        else:
            self.logger.info("No orders found")

    def _extract_order_refunds(self, client: ShopifyGraphQLClient, params: Configuration):
        """Extract order refunds using paginated GraphQL into 5 flat output tables."""
        self.logger.info("Extracting order refunds (paginated GraphQL)")

        date_since, date_to = self._parse_loading_option_dates(
            params.loading_options.date_since, params.loading_options.date_to
        )

        refunds: list[dict[str, Any]] = []
        refund_line_items: list[dict[str, Any]] = []
        refund_order_adjustments: list[dict[str, Any]] = []
        refund_shipping_lines: list[dict[str, Any]] = []
        refund_transactions: list[dict[str, Any]] = []

        for batch in client.get_order_refunds(
            date_since=date_since,
            date_to=date_to,
            batch_size=params.batch_size,
            fetch_parameter=params.loading_options.fetch_parameter,
        ):
            for order in batch:
                order_id = order["id"]
                for r in order.get("refunds", []):
                    refund_id = r["id"]
                    refunds.append(self._flatten_refund(r, order_id))

                    for edge in r.get("refundLineItems", {}).get("edges", []):
                        node = edge["node"]
                        refund_line_items.append(self._flatten_refund_line_item(node, refund_id, order_id))

                    for edge in r.get("orderAdjustments", {}).get("edges", []):
                        node = edge["node"]
                        refund_order_adjustments.append(
                            self._flatten_refund_order_adjustment(node, refund_id, order_id)
                        )

                    for edge in r.get("refundShippingLines", {}).get("edges", []):
                        node = edge["node"]
                        refund_shipping_lines.append(self._flatten_refund_shipping_line(node, refund_id, order_id))

                    for edge in r.get("transactions", {}).get("edges", []):
                        node = edge["node"]
                        refund_transactions.append(self._flatten_refund_transaction(node, refund_id, order_id))

        tables = {
            "refund": refunds,
            "refund_line_item": refund_line_items,
            "refund_order_adjustment": refund_order_adjustments,
            "refund_shipping_line": refund_shipping_lines,
            "refund_transaction": refund_transactions,
        }

        total = 0
        for table_name, rows in tables.items():
            if rows:
                self._write_refund_table(table_name, rows)
                total += len(rows)
                self.logger.info(f"Wrote {len(rows)} rows to {table_name}")
            else:
                self.logger.info(f"No data for {table_name}")

        self.logger.info(f"Refunds extraction complete: {len(refunds)} refunds, {total} total rows across 5 tables")

    @staticmethod
    def _extract_money(money_set: dict[str, Any] | None, prefix: str) -> dict[str, str | None]:
        """Extract shopMoney and presentmentMoney from a MoneyBag field."""
        if not money_set:
            return {
                f"{prefix}_shop_amount": None,
                f"{prefix}_shop_currency": None,
                f"{prefix}_presentment_amount": None,
                f"{prefix}_presentment_currency": None,
            }
        shop = money_set.get("shopMoney") or {}
        pres = money_set.get("presentmentMoney") or {}
        return {
            f"{prefix}_shop_amount": shop.get("amount"),
            f"{prefix}_shop_currency": shop.get("currencyCode"),
            f"{prefix}_presentment_amount": pres.get("amount"),
            f"{prefix}_presentment_currency": pres.get("currencyCode"),
        }

    def _flatten_refund(self, r: dict[str, Any], order_id: str) -> dict[str, Any]:
        row: dict[str, Any] = {
            "id": r["id"],
            "order_id": order_id,
            "created_at": r.get("createdAt"),
            "updated_at": r.get("updatedAt"),
            "note": r.get("note"),
        }
        row.update(self._extract_money(r.get("totalRefundedSet"), "total_refunded"))
        return row

    @staticmethod
    def _flatten_refund_line_item(node: dict[str, Any], refund_id: str, order_id: str) -> dict[str, Any]:
        return {
            "id": node.get("id"),
            "refund_id": refund_id,
            "order_id": order_id,
            "line_item_id": (node.get("lineItem") or {}).get("id"),
            "quantity": node.get("quantity"),
            "restock_type": node.get("restockType"),
            "subtotal_shop_amount": (node.get("subtotalSet") or {}).get("shopMoney", {}).get("amount"),
            "subtotal_shop_currency": (node.get("subtotalSet") or {}).get("shopMoney", {}).get("currencyCode"),
            "subtotal_presentment_amount": (node.get("subtotalSet") or {}).get("presentmentMoney", {}).get("amount"),
            "subtotal_presentment_currency": (node.get("subtotalSet") or {})
            .get("presentmentMoney", {})
            .get("currencyCode"),
            "total_tax_shop_amount": (node.get("totalTaxSet") or {}).get("shopMoney", {}).get("amount"),
            "total_tax_shop_currency": (node.get("totalTaxSet") or {}).get("shopMoney", {}).get("currencyCode"),
            "total_tax_presentment_amount": (node.get("totalTaxSet") or {}).get("presentmentMoney", {}).get("amount"),
            "total_tax_presentment_currency": (node.get("totalTaxSet") or {})
            .get("presentmentMoney", {})
            .get("currencyCode"),
            "price_shop_amount": (node.get("priceSet") or {}).get("shopMoney", {}).get("amount"),
            "price_shop_currency": (node.get("priceSet") or {}).get("shopMoney", {}).get("currencyCode"),
            "price_presentment_amount": (node.get("priceSet") or {}).get("presentmentMoney", {}).get("amount"),
            "price_presentment_currency": (node.get("priceSet") or {}).get("presentmentMoney", {}).get("currencyCode"),
            "location_id": (node.get("location") or {}).get("id"),
        }

    @staticmethod
    def _flatten_refund_order_adjustment(node: dict[str, Any], refund_id: str, order_id: str) -> dict[str, Any]:
        return {
            "id": node.get("id"),
            "refund_id": refund_id,
            "order_id": order_id,
            "reason": node.get("reason"),
            "amount_shop_amount": (node.get("amountSet") or {}).get("shopMoney", {}).get("amount"),
            "amount_shop_currency": (node.get("amountSet") or {}).get("shopMoney", {}).get("currencyCode"),
            "amount_presentment_amount": (node.get("amountSet") or {}).get("presentmentMoney", {}).get("amount"),
            "amount_presentment_currency": (node.get("amountSet") or {})
            .get("presentmentMoney", {})
            .get("currencyCode"),
            "tax_amount_shop_amount": (node.get("taxAmountSet") or {}).get("shopMoney", {}).get("amount"),
            "tax_amount_shop_currency": (node.get("taxAmountSet") or {}).get("shopMoney", {}).get("currencyCode"),
            "tax_amount_presentment_amount": (node.get("taxAmountSet") or {}).get("presentmentMoney", {}).get("amount"),
            "tax_amount_presentment_currency": (node.get("taxAmountSet") or {})
            .get("presentmentMoney", {})
            .get("currencyCode"),
        }

    @staticmethod
    def _flatten_refund_shipping_line(node: dict[str, Any], refund_id: str, order_id: str) -> dict[str, Any]:
        return {
            "refund_id": refund_id,
            "order_id": order_id,
            "shipping_line_id": (node.get("shippingLine") or {}).get("id"),
            "subtotal_shop_amount": (node.get("subtotalAmountSet") or {}).get("shopMoney", {}).get("amount"),
            "subtotal_shop_currency": (node.get("subtotalAmountSet") or {}).get("shopMoney", {}).get("currencyCode"),
            "subtotal_presentment_amount": (node.get("subtotalAmountSet") or {})
            .get("presentmentMoney", {})
            .get("amount"),
            "subtotal_presentment_currency": (node.get("subtotalAmountSet") or {})
            .get("presentmentMoney", {})
            .get("currencyCode"),
            "tax_amount_shop_amount": (node.get("taxAmountSet") or {}).get("shopMoney", {}).get("amount"),
            "tax_amount_shop_currency": (node.get("taxAmountSet") or {}).get("shopMoney", {}).get("currencyCode"),
            "tax_amount_presentment_amount": (node.get("taxAmountSet") or {}).get("presentmentMoney", {}).get("amount"),
            "tax_amount_presentment_currency": (node.get("taxAmountSet") or {})
            .get("presentmentMoney", {})
            .get("currencyCode"),
        }

    @staticmethod
    def _flatten_refund_transaction(node: dict[str, Any], refund_id: str, order_id: str) -> dict[str, Any]:
        return {
            "id": node.get("id"),
            "refund_id": refund_id,
            "order_id": order_id,
            "kind": node.get("kind"),
            "status": node.get("status"),
            "test": node.get("test"),
            "amount_shop_amount": (node.get("amountSet") or {}).get("shopMoney", {}).get("amount"),
            "amount_shop_currency": (node.get("amountSet") or {}).get("shopMoney", {}).get("currencyCode"),
            "amount_presentment_amount": (node.get("amountSet") or {}).get("presentmentMoney", {}).get("amount"),
            "amount_presentment_currency": (node.get("amountSet") or {})
            .get("presentmentMoney", {})
            .get("currencyCode"),
            "gateway": node.get("gateway"),
            "formatted_gateway": node.get("formattedGateway"),
            "created_at": node.get("createdAt"),
            "processed_at": node.get("processedAt"),
            "error_code": node.get("errorCode"),
            "authorization_code": node.get("authorizationCode"),
            "authorization_expires_at": node.get("authorizationExpiresAt"),
        }

    def _write_refund_table(self, table_name: str, rows: list[dict[str, Any]]) -> None:
        """Write a flat list of dicts as a CSV output table with manifest."""
        columns = list(rows[0].keys())
        schema = OrderedDict(
            {
                col: ColumnDefinition(
                    data_types=BaseType(dtype=SupportedDataTypes.STRING),
                    primary_key=False,
                )
                for col in columns
            }
        )

        out_table = self.create_out_table_definition(
            f"{table_name}.csv",
            schema=schema,
            primary_key=self._get_primary_key(table_name),
            incremental=bool(self.params.loading_options.incremental_output),
            has_header=True,
        )

        with open(out_table.full_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=columns, quoting=csv.QUOTE_ALL)
            writer.writeheader()
            writer.writerows(rows)

        self.write_manifest(out_table)

    def _extract_orders_bulk(self, client: ShopifyGraphQLClient, params: Configuration):
        """Extract orders using Shopify bulk operations"""
        self.logger.info("Extracting orders using bulk operations")

        with tempfile.NamedTemporaryFile(mode="w+", suffix=".jsonl", delete=False) as tmp:
            temp_jsonl = tmp.name

        date_since, date_to = self._parse_loading_option_dates(
            params.loading_options.date_since, params.loading_options.date_to
        )
        result = client.get_orders_bulk(
            temp_jsonl,
            include_transactions=params.endpoints.order_transactions,
            date_since=date_since,
            date_to=date_to,
            fetch_parameter=params.loading_options.fetch_parameter,
        )

        if result.item_count > 0:
            self._process_bulk_orders(result)
        else:
            self.logger.info("No orders found")
            Path(result.file_path).unlink(missing_ok=True)

    def _extract_order_shipping_discounts(self, client: ShopifyGraphQLClient, params: Configuration):
        """Extract order shipping lines and code-type discount applications via paginated GraphQL.

        Both connections are fetched in a single paginated query per order page and written as
        two flat output tables: ``order_shipping_lines`` and ``order_discount_codes``.
        """
        self.logger.info("Extracting order shipping lines and discount codes (paginated GraphQL)")

        date_since, date_to = self._parse_loading_option_dates(
            params.loading_options.date_since, params.loading_options.date_to
        )

        shipping_lines: list[dict[str, Any]] = []
        discount_codes: list[dict[str, Any]] = []

        for batch in client.get_order_shipping_discounts(
            date_since=date_since,
            date_to=date_to,
            batch_size=params.batch_size,
            fetch_parameter=params.loading_options.fetch_parameter,
        ):
            for order in batch:
                order_id = order["id"]

                for row_number, edge in enumerate(order.get("shippingLines", {}).get("edges", [])):
                    shipping_lines.append(self._flatten_shipping_line(edge["node"], order_id, row_number))

                row_number = 0
                for edge in order.get("discountApplications", {}).get("edges", []):
                    node = edge["node"]
                    if node.get("__typename") != "DiscountCodeApplication":
                        continue
                    discount_codes.append(self._flatten_discount_code(node, order_id, row_number))
                    row_number += 1

        tables = {
            "order_shipping_lines": shipping_lines,
            "order_discount_codes": discount_codes,
        }
        for table_name, rows in tables.items():
            if rows:
                self._write_flat_table(table_name, rows)
                self.logger.info(f"Wrote {len(rows)} rows to {table_name}")
            else:
                self.logger.info(f"No data for {table_name}")

    @staticmethod
    def _flatten_shipping_line(node: dict[str, Any], order_id: str, row_number: int) -> dict[str, Any]:
        original_price = (node.get("originalPriceSet") or {}).get("shopMoney") or {}
        discounted_price = (node.get("discountedPriceSet") or {}).get("shopMoney") or {}
        return {
            "parent_id": order_id,
            "id": node.get("id"),
            "row_number": row_number,
            "title": node.get("title"),
            "code": node.get("code"),
            "source": node.get("source"),
            "price_set__shop_money__amount": original_price.get("amount"),
            "price_set__shop_money__currency_code": original_price.get("currencyCode"),
            "discounted_price_set__shop_money__amount": discounted_price.get("amount"),
            "discounted_price_set__shop_money__currency_code": discounted_price.get("currencyCode"),
            "tax_lines": json.dumps(node.get("taxLines") or []),
        }

    @staticmethod
    def _flatten_discount_code(node: dict[str, Any], order_id: str, row_number: int) -> dict[str, Any]:
        value = node.get("value") or {}
        return {
            "parent_id": order_id,
            "row_number": row_number,
            "code": node.get("code"),
            "discount_application_index": node.get("index"),
            "value_type": value.get("__typename"),
            "value_amount": value.get("amount"),
            "value_currency_code": value.get("currencyCode"),
            "value_percentage": value.get("percentage"),
        }

    def _write_flat_table(self, table_name: str, rows: list[dict[str, Any]]) -> None:
        """Write a flat list of dicts as a CSV output table with manifest.

        Columns are STRING except ``row_number``, which is typed INTEGER (it is a deterministic
        array index).
        """
        columns = list(rows[0].keys())
        integer_columns = {"row_number"}
        schema = OrderedDict(
            {
                col: ColumnDefinition(
                    data_types=BaseType(
                        dtype=SupportedDataTypes.INTEGER if col in integer_columns else SupportedDataTypes.STRING
                    ),
                    primary_key=False,
                )
                for col in columns
            }
        )

        out_table = self.create_out_table_definition(
            f"{table_name}.csv",
            schema=schema,
            primary_key=self._get_primary_key(table_name),
            incremental=bool(self.params.loading_options.incremental_output),
            has_header=True,
        )

        with open(out_table.full_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=columns, quoting=csv.QUOTE_ALL)
            writer.writeheader()
            writer.writerows(rows)

        self.write_manifest(out_table)

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

        date_since, date_to = self._parse_loading_option_dates(
            params.loading_options.date_since, params.loading_options.date_to
        )
        result = client.get_products_bulk(
            temp_jsonl,
            status=status_filter,
            include_product_metafields=params.endpoints.product_metafields,
            include_variant_metafields=params.endpoints.variant_metafields,
            date_since=date_since,
            date_to=date_to,
            fetch_parameter=params.loading_options.fetch_parameter,
        )

        if result.item_count > 0:
            self._process_bulk_products(result)
        else:
            self.logger.info("No products found")
            Path(result.file_path).unlink(missing_ok=True)

    def _process_bulk_result(
        self,
        bulk_result: BulkOperationResult,
        table_name: str,
        entity_name: str | None = None,
        entity_name_overrides: dict[str, str] | None = None,
    ):
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

            if table_name == "order":
                self._flatten_customer_journey_summary(table_name, entity_keys)
                self._flatten_line_item_product(table_name, entity_keys)

            normalized_table = self._normalize_table(table_name)
            self._export_table_with_manifest(table_name, normalized_table, entity_keys, entity_name_overrides)
            self._decompose_entity_child_lists(table_name)
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

        date_since, date_to = self._parse_loading_option_dates(
            params.loading_options.date_since, params.loading_options.date_to
        )
        result = client.get_customers_bulk(
            temp_jsonl,
            date_since=date_since,
            date_to=date_to,
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

        date_since, date_to = self._parse_loading_option_dates(
            params.loading_options.date_since, params.loading_options.date_to
        )
        result = client.get_inventory_bulk(
            temp_jsonl,
            date_since=date_since,
            date_to=date_to,
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

    def _extract_collections_bulk(self, client: ShopifyGraphQLClient, params: Configuration):
        """Extract collections using Shopify bulk operations"""
        self.logger.info("Extracting collections using bulk operations")

        with tempfile.NamedTemporaryFile(mode="w+", suffix=".jsonl", delete=False) as tmp:
            temp_jsonl = tmp.name

        date_since, date_to = self._parse_loading_option_dates(
            params.loading_options.date_since, params.loading_options.date_to
        )
        result = client.get_collections_bulk(
            temp_jsonl,
            include_metafields=params.endpoints.collection_metafields,
            date_since=date_since,
            date_to=date_to,
            fetch_parameter=params.loading_options.fetch_parameter,
        )

        if result.item_count > 0:
            self._process_bulk_collections(result, include_metafields=params.endpoints.collection_metafields)
        else:
            self.logger.info("No collections found")
            Path(result.file_path).unlink(missing_ok=True)

    def _process_bulk_collections(self, bulk_result: BulkOperationResult, include_metafields: bool = False):
        # The collections bulk decomposes into child entities that share generic GID entity
        # types with the products endpoint. "Product" rows here are the product-GID -> collection-GID
        # mapping (not the full product schema), and "Metafield" rows are collection-owned. Rename
        # both to distinct "collection_product"/"collection_metafield" tables so a config with both
        # products and collections enabled doesn't collide on the generic "product"/"metafield" tables.
        entity_name_overrides = {"Product": "collection_product"}
        if include_metafields:
            entity_name_overrides["Metafield"] = "collection_metafield"
        self._process_bulk_result(bulk_result, "collection", entity_name_overrides=entity_name_overrides)

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

        date_since, date_to = self._parse_loading_option_dates(
            params.loading_options.date_since, params.loading_options.date_to
        )
        result = client.get_events_bulk(
            temp_jsonl,
            date_since=date_since,
            date_to=date_to,
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
        self,
        table_name: str,
        normalized_table: str | None = None,
        entity_keys: dict[str, set[str]] | None = None,
        entity_name_overrides: dict[str, str] | None = None,
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
                if entity_name_overrides and entity_type in entity_name_overrides:
                    snake_entity = entity_name_overrides[entity_type]
                else:
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

        # table_meta carries the full normalized parent schema. Each entity/child
        # table (e.g. product) only contains its own columns, so filter the manifest
        # schema to valid_columns — otherwise every child manifest declares the
        # parent's entire column set.
        valid_set = set(valid_columns)
        schema: OrderedDict[str, ColumnDefinition] = OrderedDict()
        for column in table_meta:
            name = column[0]
            if name not in valid_set:
                continue
            output_name = "parent_id" if name == "__parent_id" else name
            schema[output_name] = ColumnDefinition(
                data_types=BaseType(dtype=self.convert_base_types(column[1])),
                primary_key=False,
            )

        out_table = self.create_out_table_definition(
            f"{entity_name}.csv",
            schema=schema,
            primary_key=self._get_primary_key(entity_name),
            incremental=bool(self.params.loading_options.incremental_output),
            has_header=True,
        )

        try:
            renamed_columns = []
            for col in valid_columns:
                if col == "__parent_id":
                    renamed_columns.append('"__parent_id" AS "parent_id"')
                else:
                    renamed_columns.append(f'"{col}"')
            column_list = ", ".join(renamed_columns)

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
            "line_item": ["id"],
            "product": ["id"],
            "product_legacy": ["id"],
            "customer": ["id"],
            "customer_legacy": ["id"],
            "inventory": ["id"],
            "inventory_item": ["id"],
            "inventory_level": ["parent_id", "id"],
            "collection": ["id"],
            "collection_product": ["parent_id", "id"],
            "collection_metafield": ["id"],
            "location": ["id"],
            "event": ["id"],
            # Refund tables (from paginated refunds extraction)
            "refund": ["id"],
            "refund_line_item": ["id"],
            "refund_order_adjustment": ["id"],
            "refund_shipping_line": ["refund_id", "shipping_line_id"],
            "refund_transaction": ["id"],
            # Paginated shipping/discount extraction (deterministic array-index row_number)
            "order_shipping_lines": ["parent_id", "row_number"],
            "order_discount_codes": ["parent_id", "row_number"],
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

    def _process_custom_query(self, client: ShopifyGraphQLClient, custom_query, params: Configuration):
        """Process a custom GraphQL bulk query"""
        self.logger.info(f"Executing custom bulk query: {custom_query.name}")

        query = custom_query.query

        date_since, date_to = self._parse_loading_option_dates(
            params.loading_options.date_since, params.loading_options.date_to
        )
        query = query.replace("{{ period_start_date }}", date_since or "")
        query = query.replace("{{ period_end_date }}", date_to or "")
        query = query.replace("{{ fetch_parameter }}", params.loading_options.fetch_parameter)

        with tempfile.NamedTemporaryFile(mode="w+", suffix=".jsonl", delete=False) as tmp:
            temp_jsonl = tmp.name

        result = client.execute_custom_bulk_query(query, temp_jsonl)

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
