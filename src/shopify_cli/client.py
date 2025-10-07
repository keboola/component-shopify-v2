import logging
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

    def execute_query(self, query: str, variables: dict[str, Any] | None) -> dict[str, Any]:
        """
        Execute GraphQL query

        Args:
            query: GraphQL query string
            variables: Query variables

        Returns:
            Query response data
        """
        try:
            client = shopify.GraphQL()
            result = client.execute(query, variables=variables)

            if "errors" in result:
                error_messages = [error.get("message", "Unknown error") for error in result["errors"]]
                raise UserException(f"GraphQL query failed: {'; '.join(error_messages)}")

            return result.get("data", {})
        except Exception as e:
            if isinstance(e, UserException):
                raise
            raise UserException(f"Failed to execute GraphQL query: {str(e)}")

    def get_orders(
        self, date_from: str | None = None, date_to: str | None = None, batch_size: int = 50
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
        # Load query from external file
        query = self.query_loader.load_query("GetOrders")

        # Build date filter query
        date_filter = ""
        if date_from or date_to:
            date_conditions = []
            if date_from:
                date_conditions.append(f"created_at:>={date_from}")
            if date_to:
                date_conditions.append(f"created_at:<={date_to}")
            date_filter = f'query: "{", ".join(date_conditions)}"'

        # Modify query to include date filter if needed
        if date_filter:
            # Replace the query parameter in the loaded query
            query = query.replace(
                "query GetOrders($first: Int!, $after: String, $query: String)",
                "query GetOrders($first: Int!, $after: String, $query: String)",
            )
            query = query.replace(
                "orders(first: $first, after: $after, query: $query)",
                f"orders(first: $first, after: $after, {date_filter})",
            )
        else:
            # Remove query parameter if no date filter
            query = query.replace(
                "query GetOrders($first: Int!, $after: String, $query: String)",
                "query GetOrders($first: Int!, $after: String)",
            )
            query = query.replace(
                "orders(first: $first, after: $after, query: $query)", "orders(first: $first, after: $after)"
            )

        cursor = None
        while True:
            variables = {"first": batch_size}
            if cursor:
                variables["after"] = cursor

            data = self.execute_query(query, variables)
            orders_data = data.get("orders", {})
            edges = orders_data.get("edges", [])

            if not edges:
                break

            # Extract orders from edges
            orders = [edge["node"] for edge in edges]
            yield orders

            # Check if there are more pages
            page_info = orders_data.get("pageInfo", {})
            if not page_info.get("hasNextPage", False):
                break

            cursor = page_info.get("endCursor")

    def get_products(self, batch_size: int = 50) -> Iterator[list[dict[str, Any]]]:
        """
        Get products with pagination

        Args:
            batch_size: Number of products per batch

        Yields:
            List of product dictionaries
        """
        # Load query from external file
        query = self.query_loader.load_query("GetProducts")

        cursor = None
        while True:
            variables = {"first": batch_size}
            if cursor:
                variables["after"] = cursor

            data = self.execute_query(query, variables)
            products_data = data.get("products", {})
            edges = products_data.get("edges", [])

            if not edges:
                break

            # Extract products from edges
            products = [edge["node"] for edge in edges]
            yield products

            # Check if there are more pages
            page_info = products_data.get("pageInfo", {})
            if not page_info.get("hasNextPage", False):
                break

            cursor = page_info.get("endCursor")

    def get_customers(self, batch_size: int = 50) -> Iterator[list[dict[str, Any]]]:
        """
        Get customers with pagination

        Args:
            batch_size: Number of customers per batch

        Yields:
            List of customer dictionaries
        """
        # Load query from external file
        query = self.query_loader.load_query("GetCustomers")

        cursor = None
        while True:
            variables = {"first": batch_size}
            if cursor:
                variables["after"] = cursor

            data = self.execute_query(query, variables)
            customers_data = data.get("customers", {})
            edges = customers_data.get("edges", [])

            if not edges:
                break

            # Extract customers from edges
            customers = [edge["node"] for edge in edges]
            yield customers

            # Check if there are more pages
            page_info = customers_data.get("pageInfo", {})
            if not page_info.get("hasNextPage", False):
                break

            cursor = page_info.get("endCursor")

    def get_inventory_items(self, batch_size: int = 50) -> Iterator[list[dict[str, Any]]]:
        """
        Get inventory items with pagination

        Args:
            batch_size: Number of inventory items per batch

        Yields:
            List of inventory item dictionaries
        """
        # Load query from external file
        query = self.query_loader.load_query("GetInventoryItems")

        cursor = None
        while True:
            variables = {"first": batch_size}
            if cursor:
                variables["after"] = cursor

            data = self.execute_query(query, variables)
            inventory_data = data.get("inventoryItems", {})
            edges = inventory_data.get("edges", [])

            if not edges:
                break

            # Extract inventory items from edges
            inventory_items = [edge["node"] for edge in edges]
            yield inventory_items

            # Check if there are more pages
            page_info = inventory_data.get("pageInfo", {})
            if not page_info.get("hasNextPage", False):
                break

            cursor = page_info.get("endCursor")

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
        # Load query from external file
        query = self.query_loader.load_query("GetProductDrafts")

        cursor = None
        while True:
            variables = {"first": batch_size}
            if cursor:
                variables["after"] = cursor

            data = self.execute_query(query, variables)
            drafts_data = data.get("productDrafts", {})
            edges = drafts_data.get("edges", [])

            if not edges:
                break

            # Extract product drafts from edges
            drafts = [edge["node"] for edge in edges]
            yield drafts

            # Check if there are more pages
            page_info = drafts_data.get("pageInfo", {})
            if not page_info.get("hasNextPage", False):
                break

            cursor = page_info.get("endCursor")

    def get_product_metafields(self, batch_size: int = 50) -> Iterator[list[dict[str, Any]]]:
        """
        Get product metafields with pagination

        Args:
            batch_size: Number of metafields per batch

        Yields:
            List of metafield dictionaries
        """
        # Load query from external file
        query = self.query_loader.load_query("GetProductMetafields")

        cursor = None
        while True:
            variables = {"first": batch_size}
            if cursor:
                variables["after"] = cursor

            data = self.execute_query(query, variables)
            metafields_data = data.get("metafields", {})
            edges = metafields_data.get("edges", [])

            if not edges:
                break

            # Extract metafields from edges
            metafields = [edge["node"] for edge in edges]
            yield metafields

            # Check if there are more pages
            page_info = metafields_data.get("pageInfo", {})
            if not page_info.get("hasNextPage", False):
                break

            cursor = page_info.get("endCursor")

    def get_variant_metafields(self, batch_size: int = 50) -> Iterator[list[dict[str, Any]]]:
        """
        Get variant metafields with pagination

        Args:
            batch_size: Number of metafields per batch

        Yields:
            List of metafield dictionaries
        """
        # Load query from external file
        query = self.query_loader.load_query("GetVariantMetafields")

        cursor = None
        while True:
            variables = {"first": batch_size}
            if cursor:
                variables["after"] = cursor

            data = self.execute_query(query, variables)
            metafields_data = data.get("metafields", {})
            edges = metafields_data.get("edges", [])

            if not edges:
                break

            # Extract metafields from edges
            metafields = [edge["node"] for edge in edges]
            yield metafields

            # Check if there are more pages
            page_info = metafields_data.get("pageInfo", {})
            if not page_info.get("hasNextPage", False):
                break

            cursor = page_info.get("endCursor")

    def get_inventory_levels(self, batch_size: int = 50) -> Iterator[list[dict[str, Any]]]:
        """
        Get inventory levels with pagination

        Args:
            batch_size: Number of inventory levels per batch

        Yields:
            List of inventory level dictionaries
        """
        # Load query from external file
        query = self.query_loader.load_query("GetInventoryLevels")

        cursor = None
        while True:
            variables = {"first": batch_size}
            if cursor:
                variables["after"] = cursor

            data = self.execute_query(query, variables)
            inventory_data = data.get("inventoryLevels", {})
            edges = inventory_data.get("edges", [])

            if not edges:
                break

            # Extract inventory levels from edges
            inventory_levels = [edge["node"] for edge in edges]
            yield inventory_levels

            # Check if there are more pages
            page_info = inventory_data.get("pageInfo", {})
            if not page_info.get("hasNextPage", False):
                break

            cursor = page_info.get("endCursor")

    def get_products_archived(self, batch_size: int = 50) -> Iterator[list[dict[str, Any]]]:
        """
        Get archived products with pagination

        Args:
            batch_size: Number of products per batch

        Yields:
            List of archived product dictionaries
        """
        # Use the same query as products but with status filter
        query = self.query_loader.load_query("GetProducts")

        # Modify query to filter for archived products
        query = query.replace(
            "products(first: $first, after: $after)", 'products(first: $first, after: $after, query: "status:archived")'
        )

        cursor = None
        while True:
            variables = {"first": batch_size}
            if cursor:
                variables["after"] = cursor

            data = self.execute_query(query, variables)
            products_data = data.get("products", {})
            edges = products_data.get("edges", [])

            if not edges:
                break

            # Extract products from edges
            products = [edge["node"] for edge in edges]
            yield products

            # Check if there are more pages
            page_info = products_data.get("pageInfo", {})
            if not page_info.get("hasNextPage", False):
                break

            cursor = page_info.get("endCursor")

    def get_transactions(self, batch_size: int = 50) -> Iterator[list[dict[str, Any]]]:
        """
        Get transactions with pagination

        Args:
            batch_size: Number of transactions per batch

        Yields:
            List of transaction dictionaries
        """
        # Load query from external file
        query = self.query_loader.load_query("GetTransactions")

        cursor = None
        while True:
            variables = {"first": batch_size}
            if cursor:
                variables["after"] = cursor

            data = self.execute_query(query, variables)
            transactions_data = data.get("transactions", {})
            edges = transactions_data.get("edges", [])

            if not edges:
                break

            # Extract transactions from edges
            transactions = [edge["node"] for edge in edges]
            yield transactions

            # Check if there are more pages
            page_info = transactions_data.get("pageInfo", {})
            if not page_info.get("hasNextPage", False):
                break

            cursor = page_info.get("endCursor")

    def get_payment_transactions(self, batch_size: int = 50) -> Iterator[list[dict[str, Any]]]:
        """
        Get payment transactions (balance transactions) with pagination

        Args:
            batch_size: Number of payment transactions per batch

        Yields:
            List of payment transaction dictionaries
        """
        # Load query from external file
        query = self.query_loader.load_query("GetPaymentTransactions")

        cursor = None
        while True:
            variables = {"first": batch_size}
            if cursor:
                variables["after"] = cursor

            data = self.execute_query(query, variables)
            transactions_data = data.get("balanceTransactions", {})
            edges = transactions_data.get("edges", [])

            if not edges:
                break

            # Extract payment transactions from edges
            transactions = [edge["node"] for edge in edges]
            yield transactions

            # Check if there are more pages
            page_info = transactions_data.get("pageInfo", {})
            if not page_info.get("hasNextPage", False):
                break

            cursor = page_info.get("endCursor")

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
        # Load query from external file
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

        cursor = None
        while True:
            variables = {"first": batch_size}
            if cursor:
                variables["after"] = cursor

            data = self.execute_query(query, variables)
            events_data = data.get("events", {})
            edges = events_data.get("edges", [])

            if not edges:
                break

            # Extract events from edges
            events = [edge["node"] for edge in edges]
            yield events

            # Check if there are more pages
            page_info = events_data.get("pageInfo", {})
            if not page_info.get("hasNextPage", False):
                break

            cursor = page_info.get("endCursor")
