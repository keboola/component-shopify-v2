# Shopify GraphQL Extractor v2

A Keboola component for extracting data from Shopify using GraphQL API. This is the second version of the Shopify extractor, built specifically to use GraphQL instead of the deprecated REST API.

## Description

This component extracts data from Shopify stores using the modern GraphQL Admin API with bulk operations for efficient data extraction. It features a DuckDB-powered data processing engine that automatically normalizes complex nested JSON data into relational tables with proper data types. The component supports comprehensive data extraction across multiple Shopify endpoints with advanced date filtering and custom query capabilities.

## Features

| **Feature**             | **Description**                               |
|-------------------------|-----------------------------------------------|
| GraphQL API             | Uses modern Shopify GraphQL Admin API v2025-10 |
| Bulk Operations         | Efficient bulk data extraction for large datasets |
| DuckDB Processing       | Advanced data processing with automatic type detection |
| Data Normalization      | Converts nested JSON into normalized relational tables |
| Multiple Endpoints      | 10+ supported endpoints including orders, products, customers, inventory |
| Date Range Filtering    | Filter data by date ranges across all bulk operations |
| Flexible Date Formats   | Supports ISO dates (YYYY-MM-DD) and relative formats ("1 week ago", "now") |
| Custom Bulk Queries     | Execute custom GraphQL bulk operations |
| Type Detection          | Automatic data type detection and conversion  |
| Relational Output       | Normalized tables with proper relationships   |
| Error Handling          | Comprehensive error handling and logging      |

## Prerequisites

- Shopify store with Admin API access
- Admin API access token with appropriate permissions
- Python 3.13
- DuckDB (automatically installed)

## Supported Endpoints

The component supports the following Shopify GraphQL endpoints using **bulk operations** for efficient data extraction:

### Core Endpoints (Bulk Operations)

- **products** - Extract active products with variants and metafields
- **products_drafts** - Extract draft products
- **products_archived** - Extract archived products
- **products_unlisted** - Extract unlisted products
- **orders** - Extract order data with line items, customer info, and addresses
- **customers** - Extract customer data with addresses and marketing preferences
- **inventory** - Extract inventory levels across locations
- **locations** - Extract store location information
- **events** - Extract system events and activity logs

### Endpoint Options

- **product_metafields** - Include product-level metafields in products extraction
- **variant_metafields** - Include product variant metafields in products extraction
- **order_transactions** - Include transactions in orders extraction

### Custom Queries

The component also supports custom GraphQL bulk operations (mutations), allowing you to execute any custom bulk query against the Shopify API.

## Configuration

### Required Parameters

- **#api_token** - Your Shopify Admin API access token
- **store_name** - Your Shopify store name (without .myshopify.com)

### Optional Parameters

- **api_version** - Shopify API version (default: "2025-10")
- **endpoints** - Object with boolean flags for each endpoint to enable:
  - **products** - Extract active products (default: false)
  - **products_drafts** - Extract draft products (default: false)
  - **products_archived** - Extract archived products (default: false)
  - **products_unlisted** - Extract unlisted products (default: false)
  - **product_metafields** - Include product metafields (default: false)
  - **variant_metafields** - Include variant metafields (default: false)
  - **orders** - Extract orders (default: false)
  - **order_transactions** - Include order transactions (default: false)
  - **customers** - Extract customers (default: false)
  - **inventory** - Extract inventory (default: false)
  - **locations** - Extract locations (default: false)
- **loading_options** - Date filtering and loading behavior:
  - **date_since** - Start date for extraction (ISO format YYYY-MM-DD or relative like "1 week ago", "2 months ago")
  - **date_to** - End date for extraction (ISO format YYYY-MM-DD or relative like "now", "yesterday")
  - **fetch_parameter** - Field to filter by: "updated_at" or "created_at" (default: "updated_at")
  - **incremental_output** - Load type: 0=Full Load, 1=Incremental Update (default: 1)
- **events** - Array of event configurations for events endpoint (default: [])
- **custom_queries** - Array of custom bulk query configurations:
  - **name** - Query name (used for output table name)
  - **query** - GraphQL bulk operation mutation string
- **debug** - Enable debug logging and save raw JSONL files (default: false)

### Example Configuration

```json
{
  "parameters": {
    "#api_token": "your_shopify_admin_api_token_here",
    "store_name": "your-shop-name",
    "api_version": "2025-10",
    "endpoints": {
      "orders": true,
      "order_transactions": true,
      "products": true,
      "products_drafts": true,
      "product_metafields": true,
      "variant_metafields": true,
      "customers": true,
      "inventory": true,
      "locations": true
    },
    "loading_options": {
      "date_since": "1 month ago",
      "date_to": "now",
      "fetch_parameter": "updated_at",
      "incremental_output": 1
    },
    "events": [],
    "custom_queries": [
      {
        "name": "my_custom_query",
        "query": "mutation { bulkOperationRunQuery(query: \"\"\"{ products(query: \\\"status:active\\\") { edges { node { id title } } } }\"\"\") { bulkOperation { id status } userErrors { field message } } }"
      }
    ],
    "debug": false
  }
}
```

## Output

The component uses DuckDB to automatically process bulk operation results into CSV tables with proper data types. Each endpoint generates CSV files with all data preserved, including nested JSON structures:

### Output Tables

#### Bulk Operations (Primary Method)

- **orders.csv** - Orders with all nested data (line items, customer info, transactions as JSON)
- **products.csv** - Products with all nested data (variants, metafields, images as JSON)
- **customers.csv** - Customer data with addresses and preferences (nested as JSON)
- **inventory.csv** - Inventory levels across locations
- **locations.csv** - Store location information
- **events.csv** - System event logs
- **{custom_query_name}.csv** - Custom query results


### Data Types and Manifests

All CSV files include Keboola manifest files (`.csv.manifest`) with:
- Proper column data types (detected by DuckDB)
- Primary key definitions for relational integrity
- Component metadata for data lineage

Data types are automatically detected and mapped:
- Strings: `VARCHAR` → `STRING`
- Numbers: `BIGINT`, `DOUBLE` → `INTEGER`, `FLOAT`
- Dates: `TIMESTAMP` → `TIMESTAMP`
- Booleans: `BOOLEAN` → `BOOLEAN`

## Architecture

The component leverages several key technologies:

- **Shopify GraphQL API**: Uses the modern Admin API for efficient data retrieval
- **DuckDB**: In-memory analytical database for data processing and type detection
- **Pydantic**: Configuration validation and type safety
- **Keboola Component Framework**: Integration with Keboola platform

### Data Processing Pipeline

1. **Configuration Validation**: Pydantic models validate input parameters
2. **Date Parsing**: Convert ISO or relative date formats to API-compatible format
3. **Bulk Operation Initiation**: Shopify client initiates GraphQL bulk operations with filters
4. **Operation Polling**: Monitor bulk operation status until completion
5. **JSONL Download**: Download bulk operation results to temporary files
6. **DuckDB Processing**: Load JSONL data into DuckDB for processing
7. **Type Detection**: DuckDB automatically detects and assigns proper data types
8. **CSV Export**: Tables are exported as CSV with typed manifest files
9. **Cleanup**: Temporary files are removed from system temp directory

## Dependencies

The component requires the following Python packages:

- `keboola-component>=1.6.13` - Keboola platform integration
- `pydantic>=2.11.9` - Configuration validation
- `duckdb>=1.4.0` - Data processing engine
- `requests>=2.31.0` - HTTP client
- `dateparser>=1.2.0` - Flexible date parsing (ISO and relative formats)

Development dependencies:
- `flake8>=7.3.0` - Code linting

Development
-----------

To customize the local data folder path, replace the `CUSTOM_FOLDER` placeholder with your desired path in the `docker-compose.yml` file:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    volumes:
      - ./:/code
      - ./CUSTOM_FOLDER:/data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Clone this repository, initialize the workspace, and run the component using the following
commands:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
git clone https://github.com/keboola/component-shopify-v2 ex_shopify_v2
cd ex_shopify_v2
docker-compose build
docker-compose run --rm dev
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Run the test suite and perform lint checks using this command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
docker-compose run --rm test
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Integration
===========

For details about deployment and integration with Keboola, refer to the
[deployment section of the developer
documentation](https://developers.keboola.com/extend/component/deployment/).
