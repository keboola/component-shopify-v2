import json
import logging
import time
from collections.abc import Iterator
from dataclasses import dataclass
from functools import wraps
from typing import Any
from urllib.request import urlopen

import shopify
from keboola.component.exceptions import UserException

from .query_loader import QueryLoader

TOTAL_ITEMS_LIMIT: int | None = None  # None for production, count for testing


@dataclass
class BulkOperationResult:
    """Result from a bulk operation"""

    file_path: str
    item_count: int
    api_wait_time: float  # Time spent waiting for Shopify to process
    download_time: float  # Time spent downloading the JSONL file


def log_bulk_performance(entity_name: str):
    """Decorator to log performance metrics for bulk operations"""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger = logging.getLogger(__name__)

            result = func(*args, **kwargs)

            if result:
                total_time = result.api_wait_time + result.download_time
                items_per_second = result.item_count / total_time if total_time > 0 else 0
                logger.info(
                    f"{entity_name.capitalize()} bulk operation: "
                    f"API wait {result.api_wait_time:.2f}s, "
                    f"download {result.download_time:.2f}s, "
                    f"total {total_time:.2f}s "
                    f"({result.item_count} items, {items_per_second:.2f} items/s)"
                )

            return result

        return wrapper

    return decorator


class ShopifyGraphQLClient:
    """
    Shopify GraphQL API client for data extraction
    """

    def __init__(self, store_name: str, api_token: str, api_version: str = "2024-01", debug: bool = False):
        """
        Initialize Shopify GraphQL client

        Args:
            store_name: Shopify store name (without .myshopify.com)
            api_token: Shopify Admin API access token
            api_version: Shopify API version
            debug: Enable debug mode (saves JSONL files)
        """
        self.store_name = store_name
        self.api_token = api_token
        self.api_version = api_version
        self.debug = debug
        self.logger = logging.getLogger(__name__)

        # Initialize query loader
        self.query_loader = QueryLoader()

        # Setup Shopify session
        self._setup_session()

    def _setup_session(self):
        """Setup Shopify session for API calls"""
        try:
            shop_url = f"https://{self.store_name}.myshopify.com"
            session = shopify.Session(shop_url, self.api_version, self.api_token)
            shopify.ShopifyResource.activate_session(session)
            self.logger.info(f"Successfully connected to Shopify store: {self.store_name}")
        except Exception as e:
            raise UserException(f"Failed to connect to Shopify store: {str(e)}")

    def execute_query(
        self, query: str, variables: dict[str, Any] | None = None, max_retries: int = 5
    ) -> dict[str, Any]:
        """
        Execute GraphQL query with retry logic for throttling

        Args:
            query: GraphQL query string
            variables: Query variables
            max_retries: Maximum number of retries for throttled requests

        Returns:
            Query response data
        """
        retry_count = 0
        base_wait = 1  # Start with 1 second

        while retry_count <= max_retries:
            try:
                client = shopify.GraphQL()
                result_str = client.execute(query, variables=variables)

                result = json.loads(result_str)

                if "errors" in result:
                    error_messages = [error.get("message", "Unknown error") for error in result["errors"]]

                    # Check if it's a throttling error
                    if any("throttled" in msg.lower() for msg in error_messages):
                        if retry_count < max_retries:
                            wait_time = base_wait * (2**retry_count)  # Exponential backoff
                            self.logger.warning(
                                f"API throttled. Waiting {wait_time}s before retry {retry_count + 1}/{max_retries}"
                            )
                            time.sleep(wait_time)
                            retry_count += 1
                            continue

                    raise UserException(f"GraphQL query failed: {'; '.join(error_messages)}")

                return result.get("data", {})
            except Exception as e:
                if isinstance(e, UserException):
                    raise
                raise UserException(f"Failed to execute GraphQL query: {str(e)}")

        # If all retries exhausted
        raise UserException(f"GraphQL query failed after {max_retries} retries due to throttling")

    def _paginate(
        self, query: str, data_key: str, batch_size: int, max_items: int | None = TOTAL_ITEMS_LIMIT
    ) -> Iterator[list[dict[str, Any]]]:
        """
        Generic pagination helper for GraphQL queries

        Args:
            query: GraphQL query string
            data_key: Key in response data containing the edges
            batch_size: Number of items per batch
            max_items: Maximum total items to fetch (for testing)

        Yields:
            List of item dictionaries
        """
        cursor = None
        total_fetched = 0

        while True:
            variables = {"first": batch_size}
            if cursor:
                variables["after"] = cursor

            data = self.execute_query(query, variables)
            collection_data = data.get(data_key, {})
            edges = collection_data.get("edges", [])

            if not edges:
                break

            # Extract nodes from edges
            items = [edge["node"] for edge in edges]

            # Apply max_items limit if specified
            if max_items is not None:
                remaining = max_items - total_fetched
                if len(items) > remaining:
                    items = items[:remaining]
                    self.logger.info(f"Sliced batch to {len(items)} items to respect limit")
                total_fetched += len(items)
                self.logger.info(f"Fetched {len(items)} items (total: {total_fetched}/{max_items})")

            yield items

            # Check if we've reached the limit
            if max_items is not None and total_fetched >= max_items:
                self.logger.info(f"Reached max_items limit of {max_items}, stopping pagination")
                break

            page_info = collection_data.get("pageInfo", {})
            if not page_info.get("hasNextPage", False):
                break

            cursor = page_info.get("endCursor")

    def get_orders(
        self,
        date_since: str | None = None,
        date_to: str | None = None,
        batch_size: int = 50,
    ) -> Iterator[list[dict[str, Any]]]:
        """
        Get orders with pagination

        Args:
            date_since: Start date for filtering (YYYY-MM-DD)
            date_to: End date for filtering (YYYY-MM-DD)
            batch_size: Number of orders per batch

        Yields:
            List of order dictionaries
        """
        query = self.query_loader.load_query("GetOrders")

        # Build date filter query
        date_filter = ""
        if date_since or date_to:
            date_conditions = []
            if date_since:
                date_conditions.append(f"created_at:>={date_since}")
            if date_to:
                date_conditions.append(f"created_at:<={date_to}")
            date_filter = f'query: "{" ".join(date_conditions)}"'

        # Modify query to include date filter if needed
        if date_filter:
            query = query.replace(
                "query GetOrders($first: Int!, $after: String, $query: String)",
                "query GetOrders($first: Int!, $after: String)",
            )
            query = query.replace(
                "orders(first: $first, after: $after, query: $query)",
                f"orders(first: $first, after: $after, {date_filter})",
            )
        else:
            query = query.replace(
                "query GetOrders($first: Int!, $after: String, $query: String)",
                "query GetOrders($first: Int!, $after: String)",
            )
            query = query.replace(
                "orders(first: $first, after: $after, query: $query)",
                "orders(first: $first, after: $after)",
            )

        yield from self._paginate(query, "orders", batch_size)

    def get_products(self, batch_size: int = 50) -> Iterator[list[dict[str, Any]]]:
        """
        Get products with pagination

        Args:
            batch_size: Number of products per batch

        Yields:
            List of product dictionaries
        """
        query = self.query_loader.load_query("GetProducts")
        yield from self._paginate(query, "products", batch_size)

    def get_customers(self, batch_size: int = 50) -> Iterator[list[dict[str, Any]]]:
        """
        Get customers with pagination

        Args:
            batch_size: Number of customers per batch

        Yields:
            List of customer dictionaries
        """
        query = self.query_loader.load_query("GetCustomers")
        yield from self._paginate(query, "customers", batch_size)

    def get_inventory_items(self, batch_size: int = 50) -> Iterator[list[dict[str, Any]]]:
        """
        Get inventory items with pagination

        Args:
            batch_size: Number of inventory items per batch

        Yields:
            List of inventory item dictionaries
        """
        query = self.query_loader.load_query("GetInventoryItems")
        yield from self._paginate(query, "inventoryItems", batch_size)

    def get_locations(self) -> list[dict[str, Any]]:
        """
        Get all locations

        Returns:
            List of location dictionaries
        """
        # Load query from external file
        query = self.query_loader.load_query("GetLocations")

        data = self.execute_query(query)
        locations_data = data.get("locations", {})
        edges = locations_data.get("edges", [])

        return [edge["node"] for edge in edges]

    def get_product_drafts(self, batch_size: int = 50) -> Iterator[list[dict[str, Any]]]:
        """
        Get product drafts with pagination

        Args:
            batch_size: Number of product drafts per batch

        Yields:
            List of product draft dictionaries
        """
        query = self.query_loader.load_query("GetProducts")
        yield from self._paginate(query, "productDrafts", batch_size)

    def get_product_metafields(self, batch_size: int = 50) -> Iterator[list[dict[str, Any]]]:
        """
        Get product metafields with pagination

        Args:
            batch_size: Number of metafields per batch

        Yields:
            List of metafield dictionaries
        """
        query = self.query_loader.load_query("GetProductMetafields")
        yield from self._paginate(query, "metafields", batch_size)

    def get_variant_metafields(self, batch_size: int = 50) -> Iterator[list[dict[str, Any]]]:
        """
        Get variant metafields with pagination

        Args:
            batch_size: Number of metafields per batch

        Yields:
            List of metafield dictionaries
        """
        query = self.query_loader.load_query("GetVariantMetafields")
        yield from self._paginate(query, "metafields", batch_size)

    def get_inventory_levels(self, batch_size: int = 50) -> Iterator[list[dict[str, Any]]]:
        """
        Get inventory levels with pagination

        Args:
            batch_size: Number of inventory levels per batch

        Yields:
            List of inventory level dictionaries
        """
        query = self.query_loader.load_query("GetInventoryLevels")
        yield from self._paginate(query, "inventoryLevels", batch_size)

    def get_products_archived(self, batch_size: int = 50) -> Iterator[list[dict[str, Any]]]:
        """
        Get archived products with pagination

        Args:
            batch_size: Number of products per batch

        Yields:
            List of archived product dictionaries
        """
        query = self.query_loader.load_query("GetProducts")
        # Modify query to filter for archived products
        query = query.replace(
            "products(first: $first, after: $after)", 'products(first: $first, after: $after, query: "status:archived")'
        )
        yield from self._paginate(query, "products", batch_size)

    def get_events(
        self, batch_size: int = 50, event_types: list[str] | None = None, subject_types: list[str] | None = None
    ) -> Iterator[list[dict[str, Any]]]:
        """
        Get events with pagination

        Args:
            batch_size: Number of events per batch
            event_types: List of event types to filter by
            subject_types: List of subject types to filter by

        Yields:
            List of event dictionaries
        """
        query = self.query_loader.load_query("GetEvents")

        # Build query filter
        query_filters = []
        if event_types:
            query_filters.append(f"verb:{','.join(event_types)}")
        if subject_types:
            query_filters.append(f"subject_type:{','.join(subject_types)}")

        if query_filters:
            query = query.replace(
                "events(first: $first, after: $after, query: $query)",
                f'events(first: $first, after: $after, query: "{",".join(query_filters)}")',
            )
        else:
            query = query.replace(
                "events(first: $first, after: $after, query: $query)", "events(first: $first, after: $after)"
            )

        yield from self._paginate(query, "events", batch_size)

    @log_bulk_performance("products")
    def get_products_bulk(
        self,
        temp_file_path: str,
        status: str | None = None,
        include_product_metafields: bool = False,
        include_variant_metafields: bool = False,
    ) -> BulkOperationResult:
        """
        Get all products using Shopify's bulk operations

        Args:
            status: Product status filter - can be single value
                or comma-separated (e.g., "ACTIVE", "ACTIVE,DRAFT,ARCHIVED")
                If None, all products regardless of status will be fetched
            temp_file_path: Path where JSONL results will be saved
            include_product_metafields: Whether to include product metafields in the response
            include_variant_metafields: Whether to include variant metafields in the response

        Returns:
            BulkOperationResult with file path and timing info (item_count will be 0 if no results)
        """
        api_wait_start = time.time()

        query_filter = f"status:{status}" if status else ""
        log_status = f" with status={status}" if status else ""

        metafields_log = []
        if include_product_metafields:
            metafields_log.append("product metafields")
        if include_variant_metafields:
            metafields_log.append("variant metafields")
        log_metafields = f" (including {' & '.join(metafields_log)})" if metafields_log else ""

        self.logger.info(f"Starting bulk operation for products{log_status}{log_metafields}")

        # Start bulk operation - load mutation directly
        mutation_file = self.query_loader.queries_dir / "BulkProducts.graphql"
        with open(mutation_file, "r", encoding="utf-8") as f:
            mutation = f.read()

        # Inject product metafields if requested
        if include_product_metafields:
            metafields_fragment_file = self.query_loader.queries_dir / "fragments" / "ProductMetafields.graphql"
            with open(metafields_fragment_file, "r", encoding="utf-8") as f:
                product_metafields_fragment = f.read()
            mutation = mutation.replace("__METAFIELDS_PLACEHOLDER__", product_metafields_fragment)
        else:
            mutation = mutation.replace("__METAFIELDS_PLACEHOLDER__", "")

        # Inject variant metafields if requested
        if include_variant_metafields:
            var_metafields_frag_file = self.query_loader.queries_dir / "fragments" / "VariantMetafields.graphql"
            with open(var_metafields_frag_file, "r", encoding="utf-8") as f:
                var_metafields_fragment = f.read()
            mutation = mutation.replace("__VARIANT_METAFIELDS_PLACEHOLDER__", var_metafields_fragment)
        else:
            mutation = mutation.replace("__VARIANT_METAFIELDS_PLACEHOLDER__", "")

        # Inject the query filter into the GraphQL query string
        if query_filter:
            mutation = mutation.replace("products {", f'products(query: "{query_filter}") {{')

        result = self.execute_query(mutation)

        bulk_op = result.get("bulkOperationRunQuery", {}).get("bulkOperation", {})
        user_errors = result.get("bulkOperationRunQuery", {}).get("userErrors", [])

        if user_errors:
            raise UserException(f"Bulk operation failed: {user_errors}")

        operation_id = bulk_op.get("id")
        self.logger.info(f"Bulk operation started: {operation_id}")

        # Poll for completion
        status_file = self.query_loader.queries_dir / "BulkOperationStatus.graphql"
        with open(status_file, "r", encoding="utf-8") as f:
            status_query = f.read()

        poll_start = time.time()
        while True:
            elapsed = time.time() - poll_start
            sleep_interval = 5 if elapsed < 60 else 15
            time.sleep(sleep_interval)

            status_result = self.execute_query(status_query)
            current_op = status_result.get("currentBulkOperation", {})

            status = current_op.get("status")
            self.logger.info(f"Bulk operation status: {status}")

            if status == "COMPLETED":
                url = current_op.get("url")
                object_count = current_op.get("objectCount", 0)
                api_wait_time = time.time() - api_wait_start

                if not url:
                    self.logger.info("Bulk operation completed with no results (empty dataset)")
                    with open(temp_file_path, "w", encoding="utf-8") as f:
                        pass
                    return BulkOperationResult(
                        file_path=temp_file_path,
                        item_count=0,
                        api_wait_time=api_wait_time,
                        download_time=0.0,
                    )

                self.logger.info(f"Downloading results from: {url}")
                return self._download_bulk_results(url, int(object_count), "products", temp_file_path, api_wait_time)

            elif status in ["FAILED", "CANCELED"]:
                error = current_op.get("errorCode", "Unknown error")
                raise UserException(f"Bulk operation {status.lower()}: {error}")

    @log_bulk_performance("orders")
    def get_orders_bulk(self, temp_file_path: str, include_transactions: bool = False) -> BulkOperationResult:
        """
        Get all orders using Shopify's bulk operations

        Args:
            temp_file_path: Path where JSONL results will be saved
            include_transactions: Whether to include order transactions in the response

        Returns:
            BulkOperationResult with file path and timing info (item_count will be 0 if no results)
        """
        api_wait_start = time.time()

        transactions_log = " (including transactions)" if include_transactions else ""
        self.logger.info(f"Starting bulk operation for orders{transactions_log}")

        mutation_file = self.query_loader.queries_dir / "BulkOrders.graphql"
        with open(mutation_file, "r", encoding="utf-8") as f:
            mutation = f.read()

        if include_transactions:
            transactions_fragment_file = self.query_loader.queries_dir / "fragments" / "OrderTransactions.graphql"
            with open(transactions_fragment_file, "r", encoding="utf-8") as f:
                transactions_fragment = f.read()
            mutation = mutation.replace("__TRANSACTIONS_PLACEHOLDER__", transactions_fragment)
        else:
            mutation = mutation.replace("__TRANSACTIONS_PLACEHOLDER__", "")

        result = self.execute_query(mutation)

        bulk_op = result.get("bulkOperationRunQuery", {}).get("bulkOperation", {})
        user_errors = result.get("bulkOperationRunQuery", {}).get("userErrors", [])

        if user_errors:
            raise UserException(f"Bulk operation failed: {user_errors}")

        operation_id = bulk_op.get("id")
        self.logger.info(f"Bulk operation started: {operation_id}")

        # Poll for completion
        status_file = self.query_loader.queries_dir / "BulkOperationStatus.graphql"
        with open(status_file, "r", encoding="utf-8") as f:
            status_query = f.read()

        poll_start = time.time()
        while True:
            elapsed = time.time() - poll_start
            sleep_interval = 5 if elapsed < 60 else 15
            time.sleep(sleep_interval)

            status_result = self.execute_query(status_query)
            current_op = status_result.get("currentBulkOperation", {})

            status = current_op.get("status")
            self.logger.info(f"Bulk operation status: {status}")

            if status == "COMPLETED":
                url = current_op.get("url")
                object_count = current_op.get("objectCount", 0)
                api_wait_time = time.time() - api_wait_start

                if not url:
                    self.logger.info("Bulk operation completed with no results (empty dataset)")
                    with open(temp_file_path, "w", encoding="utf-8") as f:
                        pass
                    return BulkOperationResult(
                        file_path=temp_file_path,
                        item_count=0,
                        api_wait_time=api_wait_time,
                        download_time=0.0,
                    )

                self.logger.info(f"Downloading results from: {url}")
                return self._download_bulk_results(url, int(object_count), "orders", temp_file_path, api_wait_time)

            elif status in ["FAILED", "CANCELED"]:
                error = current_op.get("errorCode", "Unknown error")
                raise UserException(f"Bulk operation {status.lower()}: {error}")

    @log_bulk_performance("customers")
    def get_customers_bulk(self, temp_file_path: str) -> BulkOperationResult:
        """
        Get all customers using Shopify's bulk operations

        Args:
            temp_file_path: Path where JSONL results will be saved

        Returns:
            BulkOperationResult with file path and timing info (item_count will be 0 if no results)
        """
        api_wait_start = time.time()
        self.logger.info("Starting bulk operation for customers")

        # Start bulk operation - load mutation directly
        mutation_file = self.query_loader.queries_dir / "BulkCustomers.graphql"
        with open(mutation_file, "r", encoding="utf-8") as f:
            mutation = f.read()

        result = self.execute_query(mutation)

        bulk_op = result.get("bulkOperationRunQuery", {}).get("bulkOperation", {})
        user_errors = result.get("bulkOperationRunQuery", {}).get("userErrors", [])

        if user_errors:
            raise UserException(f"Bulk operation failed: {user_errors}")

        operation_id = bulk_op.get("id")
        self.logger.info(f"Bulk operation started: {operation_id}")

        # Poll for completion
        status_file = self.query_loader.queries_dir / "BulkOperationStatus.graphql"
        with open(status_file, "r", encoding="utf-8") as f:
            status_query = f.read()

        poll_start = time.time()
        while True:
            elapsed = time.time() - poll_start
            sleep_interval = 5 if elapsed < 60 else 15
            time.sleep(sleep_interval)

            status_result = self.execute_query(status_query)
            current_op = status_result.get("currentBulkOperation", {})

            status = current_op.get("status")
            self.logger.info(f"Bulk operation status: {status}")

            if status == "COMPLETED":
                url = current_op.get("url")
                object_count = current_op.get("objectCount", 0)
                api_wait_time = time.time() - api_wait_start

                if not url:
                    self.logger.info("Bulk operation completed with no results (empty dataset)")
                    with open(temp_file_path, "w", encoding="utf-8") as f:
                        pass
                    return BulkOperationResult(
                        file_path=temp_file_path,
                        item_count=0,
                        api_wait_time=api_wait_time,
                        download_time=0.0,
                    )

                self.logger.info(f"Downloading results from: {url}")
                return self._download_bulk_results(url, int(object_count), "customers", temp_file_path, api_wait_time)

            elif status in ["FAILED", "CANCELED"]:
                error = current_op.get("errorCode", "Unknown error")
                raise UserException(f"Bulk operation {status.lower()}: {error}")

    @log_bulk_performance("inventory")
    def get_inventory_bulk(self, temp_file_path: str) -> BulkOperationResult:
        """
        Get all inventory items and levels using Shopify's bulk operations

        Args:
            temp_file_path: Path where JSONL results will be saved

        Returns:
            BulkOperationResult with file path and timing info
        """
        api_wait_start = time.time()
        self.logger.info("Starting bulk operation for inventory")

        mutation_file = self.query_loader.queries_dir / "BulkInventory.graphql"
        with open(mutation_file, "r", encoding="utf-8") as f:
            mutation = f.read()

        result = self.execute_query(mutation)

        bulk_op = result.get("bulkOperationRunQuery", {}).get("bulkOperation", {})
        user_errors = result.get("bulkOperationRunQuery", {}).get("userErrors", [])

        if user_errors:
            raise UserException(f"Bulk operation failed: {user_errors}")

        operation_id = bulk_op.get("id")
        self.logger.info(f"Bulk operation started: {operation_id}")

        status_file = self.query_loader.queries_dir / "BulkOperationStatus.graphql"
        with open(status_file, "r", encoding="utf-8") as f:
            status_query = f.read()

        poll_start = time.time()
        while True:
            elapsed = time.time() - poll_start
            sleep_interval = 5 if elapsed < 60 else 15
            time.sleep(sleep_interval)

            status_result = self.execute_query(status_query)
            current_op = status_result.get("currentBulkOperation", {})

            status = current_op.get("status")
            self.logger.info(f"Bulk operation status: {status}")

            if status == "COMPLETED":
                url = current_op.get("url")
                object_count = current_op.get("objectCount", 0)
                api_wait_time = time.time() - api_wait_start

                if not url:
                    self.logger.info("Bulk operation completed with no results (empty dataset)")
                    with open(temp_file_path, "w", encoding="utf-8") as f:
                        pass
                    return BulkOperationResult(
                        file_path=temp_file_path,
                        item_count=0,
                        api_wait_time=api_wait_time,
                        download_time=0.0,
                    )

                self.logger.info(f"Downloading results from: {url}")
                return self._download_bulk_results(url, int(object_count), "inventory", temp_file_path, api_wait_time)

            elif status in ["FAILED", "CANCELED"]:
                error = current_op.get("errorCode", "Unknown error")
                raise UserException(f"Bulk operation {status.lower()}: {error}")

    def _download_bulk_results(
        self, url: str, item_count: int, entity_type: str, temp_file_path: str, api_wait_time: float
    ) -> BulkOperationResult:
        """
        Download JSONL results from bulk operation and save to file

        Args:
            url: URL to download JSONL from
            item_count: The number of items (for logging/debug)
            entity_type: Type of entity (for logging/debug)
            temp_file_path: Path where JSONL will be saved
            api_wait_time: Time spent waiting for bulk operation to complete

        Returns:
            BulkOperationResult with file path, item count, and timing info
        """
        download_start = time.time()

        with urlopen(url) as response:
            with open(temp_file_path, "wb") as f:
                chunk_size = 8192
                while True:
                    chunk = response.read(chunk_size)
                    if not chunk:
                        break
                    f.write(chunk)

        download_time = time.time() - download_start

        self.logger.info(f"Downloaded {item_count} items from bulk operation, saved to {temp_file_path}")

        return BulkOperationResult(
            file_path=temp_file_path,
            item_count=item_count,
            api_wait_time=api_wait_time,
            download_time=download_time,
        )

    @log_bulk_performance("custom")
    def execute_custom_bulk_query(self, query: str, temp_file_path: str) -> BulkOperationResult:
        """
        Execute a custom GraphQL query using bulk operations

        Args:
            query: GraphQL bulk operation mutation string
            temp_file_path: Path where JSONL results will be saved

        Returns:
            BulkOperationResult with file path and timing info
        """
        api_wait_start = time.time()
        self.logger.info("Starting custom bulk operation")

        result = self.execute_query(query)

        bulk_op = result.get("bulkOperationRunQuery", {}).get("bulkOperation", {})
        user_errors = result.get("bulkOperationRunQuery", {}).get("userErrors", [])

        if user_errors:
            raise UserException(f"Bulk operation failed: {user_errors}")

        operation_id = bulk_op.get("id")
        self.logger.info(f"Bulk operation started: {operation_id}")

        # Poll for completion
        status_file = self.query_loader.queries_dir / "BulkOperationStatus.graphql"
        with open(status_file, "r", encoding="utf-8") as f:
            status_query = f.read()

        poll_start = time.time()
        while True:
            elapsed = time.time() - poll_start
            sleep_interval = 5 if elapsed < 60 else 15
            time.sleep(sleep_interval)

            status_result = self.execute_query(status_query)
            current_op = status_result.get("currentBulkOperation", {})

            status = current_op.get("status")
            self.logger.info(f"Bulk operation status: {status}")

            if status == "COMPLETED":
                url = current_op.get("url")
                object_count = current_op.get("objectCount", 0)
                api_wait_time = time.time() - api_wait_start

                if not url:
                    self.logger.info("Bulk operation completed with no results (empty dataset)")
                    with open(temp_file_path, "w", encoding="utf-8") as f:
                        pass
                    return BulkOperationResult(
                        file_path=temp_file_path,
                        item_count=0,
                        api_wait_time=api_wait_time,
                        download_time=0.0,
                    )

                self.logger.info(f"Downloading results from: {url}")
                return self._download_bulk_results(url, int(object_count), "custom", temp_file_path, api_wait_time)

            elif status in ["FAILED", "CANCELED"]:
                error = current_op.get("errorCode", "Unknown error")
                raise UserException(f"Bulk operation {status.lower()}: {error}")
