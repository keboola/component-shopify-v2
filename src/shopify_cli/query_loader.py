from pathlib import Path

MAP_QUERY_NAME_TO_FILE = {
    "orders": ("orders/orders.graphql", "GetOrders"),
    "customers": ("customers/customers.graphql", "GetCustomers"),
    "products": ("products/products.graphql", "GetProducts"),
    "inventory_items": ("inventory/inventory_items.graphql", "GetInventoryItems"),
    "locations": ("locations/locations.graphql", "GetLocations"),
    # .....
}


class QueryLoader:
    """
    Loads GraphQL queries from external .graphql files
    """

    def __init__(self, queries_dir: str | None = None):
        """
        Initialize query loader

        Args:
            queries_dir: Path to queries directory. If None, uses default location.
        """
        if queries_dir is None:
            current_dir = Path(__file__).parent
            self.queries_dir = current_dir / "queries"
        else:
            self.queries_dir = Path(queries_dir)
        self._queries_cache: dict[str, str] = {}

    def load_query(self, query_name: str) -> str:
        """
        Load a GraphQL query from file

        Args:
            query_name: Name of the query (without .graphql extension)

        Returns:
            GraphQL query string

        Raises:
            FileNotFoundError: If query file doesn't exist
            ValueError: If query not found in file
        """
        # Check cache first
        if query_name in self._queries_cache:
            return self._queries_cache[query_name]

        # Load from file
        query_file = self.queries_dir / f"{query_name}.graphql"

        if not query_file.exists():
            raise FileNotFoundError(f"Query file not found: {query_file}")

        with open(query_file) as f:
            content = f.read()

        # Extract the specific query from the file
        query = self._extract_query(content, query_name)

        # Cache the result
        self._queries_cache[query_name] = query

        return query

    def _extract_query(self, content: str, query_name: str) -> str:
        """
        Extract a specific query from GraphQL file content

        Args:
            content: Full file content
            query_name: Name of the query to extract

        Returns:
            Extracted query string

        Raises:
            ValueError: If query not found in content
        """
        lines = content.split("\n")
        query_lines = []
        in_query = False
        brace_count = 0

        for line in lines:
            if line.strip().startswith("#"):
                continue

            # Check if this line starts the query we want
            if f"query {query_name}" in line:
                in_query = True
                query_lines.append(line)
                # Count opening braces in this line
                brace_count += line.count("{")
                continue

            if in_query:
                query_lines.append(line)
                brace_count += line.count("{")
                brace_count -= line.count("}")

                # If we've closed all braces, we're done
                if brace_count == 0:
                    break

        if not query_lines:
            raise ValueError(f"Query '{query_name}' not found in file")

        return "\n".join(query_lines).strip()

    def get_available_queries(self) -> list:
        """
        Get list of available query files

        Returns:
            List of query names (without .graphql extension)
        """
        if not self.queries_dir.exists():
            return []

        query_files = list(self.queries_dir.glob("*.graphql"))
        return [f.stem for f in query_files]
