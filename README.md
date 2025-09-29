# Shopify GraphQL Extractor v2

A Keboola component for extracting data from Shopify using GraphQL API. This is the second version of the Shopify extractor, built specifically to use GraphQL instead of the deprecated REST API.

## Description

This component extracts data from Shopify stores using the modern GraphQL Admin API. It features a DuckDB-powered data processing engine that automatically normalizes complex nested JSON data into relational tables with proper data types. The component supports comprehensive data extraction across multiple Shopify endpoints with advanced pagination and filtering capabilities.

## Features

| **Feature**             | **Description**                               |
|-------------------------|-----------------------------------------------|
| GraphQL API             | Uses modern Shopify GraphQL Admin API         |
| DuckDB Processing       | Advanced data processing with automatic type detection |
| Data Normalization      | Converts nested JSON into normalized relational tables |
| Multiple Endpoints      | 12+ supported endpoints including orders, products, customers, inventory |
| Date Range Filtering    | Filter data by date ranges (orders endpoint)  |
| Pagination Support      | Handles large datasets with automatic pagination |
| Batch Processing        | Configurable batch sizes for optimal performance |
| Type Detection          | Automatic data type detection and conversion  |
| Relational Output       | Normalized tables with proper relationships   |
| Error Handling          | Comprehensive error handling and logging      |

## Prerequisites

- Shopify store with Admin API access
- Admin API access token with appropriate permissions
- Python 3.13+
- DuckDB (automatically installed)

## Supported Endpoints

The component supports the following Shopify GraphQL endpoints:

### Core Endpoints
- **orders** - Extract order data with line items, customer info, and addresses (supports date filtering)
- **products** - Extract product data with variants, images, and metafields
- **customers** - Extract customer data with addresses and marketing preferences
- **inventory_items** - Extract inventory item data with location-specific quantities
- **locations** - Extract store location information

### Advanced Endpoints
- **products_drafts** - Extract product drafts and unpublished products
- **product_metafields** - Extract product-level metafields
- **variant_metafields** - Extract product variant metafields
- **inventory** - Extract inventory levels across locations
- **products_archived** - Extract archived/deleted products
- **transactions** - Extract financial transactions
- **payments_transactions** - Extract payment and balance transactions
- **events** - Extract system events and activity logs

## Configuration

### Required Parameters

- **#api_token** - Your Shopify Admin API access token
- **store_name** - Your Shopify store name (without .myshopify.com)

### Optional Parameters

- **api_version** - Shopify API version (default: "2024-01")
- **endpoints** - List of endpoints to extract (default: ["orders", "products"])
  - Valid endpoints: orders, products, customers, inventory_items, locations, products_drafts, product_metafields, variant_metafields, inventory, products_archived, transactions, payments_transactions, events
- **date_from** - Start date for data extraction (YYYY-MM-DD format)
- **date_to** - End date for data extraction (YYYY-MM-DD format)
- **batch_size** - Number of records per batch (1-250, default: 50)
- **debug** - Enable debug logging (default: false)

### Example Configuration

```json
{
  "parameters": {
    "#api_token": "your_shopify_admin_api_token_here",
    "store_name": "your-shop-name",
    "api_version": "2024-01",
    "endpoints": ["orders", "products", "customers", "inventory_items"],
    "date_from": "2024-01-01",
    "date_to": "2024-12-31",
    "batch_size": 50,
    "debug": false
  }
}
```

## Output

The component uses DuckDB to automatically normalize complex JSON data into relational tables with proper data types. Each endpoint generates one or more CSV files:

### Normalized Tables

#### Orders Endpoint
- **orders.csv** - Main order information (id, totals, customer data, timestamps)
- **order_line_items.csv** - Order line items with product/variant details

#### Products Endpoint
- **products.csv** - Main product information (id, title, description, vendor, etc.)
- **product_variants.csv** - Product variants with pricing and inventory

#### Inventory Items Endpoint
- **inventory_items.csv** - Inventory item details with product relationships
- **inventory_levels.csv** - Location-specific inventory quantities

#### Simple Tables
- **customers.csv** - Customer data with addresses and preferences
- **locations.csv** - Store location information
- **products_drafts.csv** - Draft product data
- **product_metafields.csv** - Product metafields
- **variant_metafields.csv** - Variant metafields
- **inventory.csv** - Inventory level data
- **products_archived.csv** - Archived product data
- **transactions.csv** - Financial transaction data
- **payments_transactions.csv** - Payment transaction data
- **events.csv** - System event logs

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
2. **GraphQL Query Execution**: Shopify client executes paginated GraphQL queries
3. **Data Collection**: Raw JSON data is collected in batches
4. **DuckDB Processing**: Data is loaded into DuckDB for normalization
5. **Table Generation**: Complex structures are normalized into relational tables
6. **Type Detection**: DuckDB automatically detects and assigns proper data types
7. **CSV Export**: Normalized tables are exported as CSV with manifest files

## Dependencies

The component requires the following Python packages:

- `keboola-component>=1.6.13` - Keboola platform integration
- `pydantic>=2.11.9` - Configuration validation
- `shopifyapi>=12.7.0` - Shopify API client
- `duckdb>=1.4.0` - Data processing engine
- `requests>=2.31.0` - HTTP client
- `gql>=4.0.0` - GraphQL client utilities

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
git clone https://github.com/keboola/cookiecutter-python-component ex_shopify_v2
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
