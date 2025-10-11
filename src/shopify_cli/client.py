import json
import logging
import time
from collections.abc import Iterator
from typing import Any

import shopify
from keboola.component.exceptions import UserException

from .query_loader import QueryLoader


class ShopifyGraphQLClient:
    """
    Shopify GraphQL API client for data extraction
    """

    def __init__(self, store_name: str, api_token: str, api_version: str = "2024-01"):
        """
        Initialize Shopify GraphQL client

        Args:
            store_name: Shopify store name (without .myshopify.com)
            api_token: Shopify Admin API access token
            api_version: Shopify API version
        """
        self.store_name = store_name
        self.api_token = api_token
        self.api_version = api_version
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
                logging.info(result)

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
        self, query: str, data_key: str, batch_size: int, max_items: int | None = 2_000
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
        date_from: str | None = None,
        date_to: str | None = None,
        batch_size: int = 50,
    ) -> Iterator[list[dict[str, Any]]]:
        """
        Get orders with pagination

        Args:
            date_from: Start date for filtering (YYYY-MM-DD)
            date_to: End date for filtering (YYYY-MM-DD)
            batch_size: Number of orders per batch

        Yields:
            List of order dictionaries
        """
        query = self.query_loader.load_query("GetOrders")

        # Build date filter query
        date_filter = ""
        if date_from or date_to:
            date_conditions = []
            if date_from:
                date_conditions.append(f"created_at:>={date_from}")
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

        logging.info(query)
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

    def get_transactions(self, batch_size: int = 50) -> Iterator[list[dict[str, Any]]]:
        """
        Get transactions with pagination

        Args:
            batch_size: Number of transactions per batch

        Yields:
            List of transaction dictionaries
        """
        query = self.query_loader.load_query("GetTransactions")
        yield from self._paginate(query, "transactions", batch_size)

    def get_payment_transactions(self, batch_size: int = 50) -> Iterator[list[dict[str, Any]]]:
        """
        Get payment transactions (balance transactions) with pagination

        Args:
            batch_size: Number of payment transactions per batch

        Yields:
            List of payment transaction dictionaries
        """
        query = self.query_loader.load_query("GetPaymentTransactions")
        yield from self._paginate(query, "balanceTransactions", batch_size)

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
