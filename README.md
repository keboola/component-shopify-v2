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
| Multiple Endpoints      | 11+ supported endpoints including orders, products, customers, collections, inventory |
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
- **collections** - Extract product collections (custom and smart collections with products)
- **locations** - Extract store location information
- **events** - Extract system events and activity logs

### Paginated Endpoints

- **order_refunds** - Extract order refund data (refund line items, order adjustments, shipping refunds, transactions) using paginated GraphQL. Standalone endpoint — it paginates its own orders query and does not require the **orders** toggle to be enabled. Each refund amount includes both shop and presentment currency.

### Endpoint Options

- **product_metafields** - Include product-level metafields in products extraction
- **variant_metafields** - Include product variant metafields in products extraction
- **collection_metafields** - Include collection-level metafields in collections extraction
- **order_transactions** - Include transactions in orders extraction
- **order_shipping_discounts** - Extract order shipping lines and discount codes into the `order_shipping_lines` and `order_discount_codes` tables (paginated GraphQL, see [Order shipping lines & discount codes](#order-shipping-lines--discount-codes))

> **Note:** Disabling `order_transactions` after a previous run had it enabled will fail the
> Storage load against the existing `order` table — Keboola refuses imports that omit columns
> present on the destination table (the missing-column rule). Drop the `transactions` column (or
> the whole table) in Storage before disabling the option, or keep the toggle stable.

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
  - **order_refunds** - Extract order refunds (default: false, standalone endpoint)
  - **order_shipping_discounts** - Extract order shipping lines and discount codes (default: false)
  - **customers** - Extract customers (default: false)
  - **inventory** - Extract inventory (default: false)
  - **collections** - Extract collections (default: false)
  - **collection_metafields** - Include collection metafields (default: false)
  - **locations** - Extract locations (default: false)
- **loading_options** - Date filtering and loading behavior:
  - **date_since** - Start date for extraction (ISO format YYYY-MM-DD or relative like "1 week ago", "2 months ago"). Floored to midnight of its day.
  - **date_to** - End date for extraction (ISO format YYYY-MM-DD or relative like "now", "yesterday"). Resolved to a full ISO-8601 UTC timestamp of the actual run moment, so `date_to: "now"` includes records updated earlier the same day. Leave unset to emit no upper bound. **Note:** prior to this fix, `date_to` was truncated to midnight of the run day, silently excluding records updated on the run day whenever `date_to` was set.
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
      "order_refunds": true,
      "order_shipping_discounts": true,
      "products": true,
      "products_drafts": true,
      "product_metafields": true,
      "variant_metafields": true,
      "customers": true,
      "inventory": true,
      "collections": true,
      "collection_metafields": true,
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
- **collection.csv** - Collections with columns: `id`, `title`, `handle`, `description_html`, `sort_order`, `template_suffix`, `updated_at`, `products_count` (JSON), `rule_set` (JSON)
- **collection_rule_set.csv** - Rule sets for smart collections with columns: `parent_id`, `applied_disjunctively`, `rules` (JSON string — smart-collection rules are kept as a single JSON column, not exploded into separate rows)
- **collection_products_count.csv** - Product counts per collection with columns: `parent_id`, `count`
- **collection_product.csv** - Collection↔product mapping (the REST `collects` equivalent) with columns: `id` (product GID), `parent_id` (collection GID). Kept in a distinct table (not the generic `product` table emitted by the products endpoint, which has the full product schema) so the two do not collide when both endpoints are enabled.
- **collection_metafield.csv** - Collection metafields (when `collection_metafields` is enabled) with columns: `id`, `parent_id` (collection GID), `namespace`, `key`, `value`, `type`, `description`, `created_at`, `updated_at`. Kept in a distinct table (not the generic `metafield` table used by product metafields) so collection metafields are not merged with product metafields.
- **locations.csv** - Store location information
- **events.csv** - System event logs
- **{custom_query_name}.csv** - Custom query results

#### Paginated Extraction (Order Refunds)

The `order_refunds` endpoint uses paginated GraphQL (not bulk operations) and writes 5 flat tables directly:

- **refund.csv** - Refund header with total refunded amounts (shop + presentment currency)
- **refund_line_item.csv** - Individual refunded line items with subtotal, tax, and price (shop + presentment)
- **refund_order_adjustment.csv** - Order-level adjustments (e.g. `REFUND_DISCREPANCY`) with amount and tax
- **refund_shipping_line.csv** - Refunded shipping lines with subtotal and tax amounts
- **refund_transaction.csv** - Payment transactions (kind, status, gateway, amounts)

All child tables carry `refund_id` and `order_id` foreign keys. Primary keys use stable Shopify GIDs.

#### Orders output mapping (v1 fields)

The orders endpoint produces the parent `order.csv` and `line_item.csv` tables plus a set of
automatically decomposed child tables. Nested money values are preserved as **serialized JSON**,
not flattened into `__`-separated scalar columns — downstream transformations (e.g. dbt) must
extract them with JSON functions (`amount`, `currencyCode` live inside the JSON).

New columns on **`order.csv`**:

| Column | Source (GraphQL) | Format |
| --- | --- | --- |
| `display_financial_status` | `displayFinancialStatus` | string |
| `display_fulfillment_status` | `displayFulfillmentStatus` | string |
| `test` | `test` | boolean |
| `payment_gateway_names` | `paymentGatewayNames` | serialized JSON array (also decomposed, see below) |
| `current_total_additional_fees_set` | `currentTotalAdditionalFeesSet` | serialized JSON `{"shopMoney":{"amount","currencyCode"}}` |
| `current_total_duties_set` | `currentTotalDutiesSet` | serialized JSON `{"shopMoney":{"amount","currencyCode"}}` |
| `total_tip_received_set` | `totalTipReceivedSet` | serialized JSON `{"shopMoney":{"amount","currencyCode"}}` |

New columns on **`line_item.csv`**:

| Column | Source (GraphQL) | Format |
| --- | --- | --- |
| `current_quantity` | `currentQuantity` | integer |
| `original_unit_price_set` | `originalUnitPriceSet` | serialized JSON `{"shopMoney":{"amount","currencyCode"}}` |
| `product_id` | `lineItems.product.id` | Product GID (e.g. `gid://shopify/Product/123`); **can be empty** when the line item's product was deleted |
| `gift_card` | `isGiftCard` (aliased to `giftCard`) | boolean; `true` when the line item is the purchase of a gift card |
| `tax_lines` | `taxLines` | serialized JSON array (also decomposed into `line_item_tax_lines.csv`) |
| `discount_allocations` | `discountAllocations` | serialized JSON array (also decomposed into `line_item_discount_allocations.csv`) |

> Note: `product_id` is the flattened `lineItems.product.id`. `Order.lineItems.nodes.product`
> is a plain nullable object that stays inline on the LineItem rows, so its `id` is extracted
> onto `line_item.csv` and the object is dropped before decomposition — otherwise generic
> decomposition would emit a misnamed `order_product` child table keyed by a LineItem GID. The
> column is always present; it is empty for line items whose product has been deleted.
>
> Note: `gift_card` matters for revenue computation — per Shopify's revenue-recognition
> semantics, gift card purchases are typically **excluded from revenue** at the time of sale and
> recognized later on redemption, so downstream revenue metrics should filter on this flag.

Child tables created for the new fields:

| Table | Columns | Relationship |
| --- | --- | --- |
| `order_payment_gateway_names.csv` | `parent_id`, `row_number`, `item` | one row per payment gateway name (`parent_id` → `order.id`) |
| `order_current_total_additional_fees_set.csv` | `parent_id`, `shop_money` | 1:1 with `order` (`shop_money` is serialized JSON) |
| `order_current_total_duties_set.csv` | `parent_id`, `shop_money` | 1:1 with `order` |
| `order_total_tip_received_set.csv` | `parent_id`, `shop_money` | 1:1 with `order` |

> Note: `original_unit_price_set` is intentionally **not** decomposed into a child table. It is
> line-item-level data, but generic decomposition runs on the mixed top-level stream and would
> emit a misnamed `order_original_unit_price_set` table. The value stays available as the
> serialized JSON column on `line_item.csv`.

#### Line-item child tables (per-entity decomposition)

Some line-item-level plain lists are additionally decomposed into their own **entity-prefixed**
child tables, with nested money **flattened into `__`-separated scalar columns**. Generic
decomposition runs on the mixed top-level `order` stream and would misname these `order_*` while
keying them by a LineItem GID; instead the rows are scoped by GID entity type
(`id LIKE 'gid://shopify/LineItem/%'`) and emitted under the `line_item_*` prefix.

`row_number` is the deterministic 1-based index of each element **within its own line item**
(per-parent, order-preserving) — it is the array position, not a global sequence.

Naming mapping (customer spec → output): `parent_id` = the customer's `line_item_id`,
`row_number` = the customer's `row_nr`.

**`line_item_tax_lines.csv`** — PK (`parent_id`, `row_number`); one row per `lineItems.taxLines` element:

| Column | Source (GraphQL) |
| --- | --- |
| `parent_id` | `lineItems.id` (LineItem GID) |
| `row_number` | array index of the tax line within the line item (1-based) |
| `title` | `taxLines.title` |
| `rate` | `taxLines.rate` |
| `rate_percentage` | `taxLines.ratePercentage` |
| `price_set__shop_money__amount` | `taxLines.priceSet.shopMoney.amount` |
| `price_set__shop_money__currency_code` | `taxLines.priceSet.shopMoney.currencyCode` |
| `channel_liable` | `taxLines.channelLiable` |
| `source` | `taxLines.source` |

**`line_item_discount_allocations.csv`** — PK (`parent_id`, `row_number`); one row per `lineItems.discountAllocations` element:

| Column | Source (GraphQL) |
| --- | --- |
| `parent_id` | `lineItems.id` (LineItem GID) |
| `row_number` | array index of the discount allocation within the line item (1-based) |
| `amount_set__shop_money__amount` | `discountAllocations.allocatedAmountSet.shopMoney.amount` |
| `amount_set__shop_money__currency_code` | `discountAllocations.allocatedAmountSet.shopMoney.currencyCode` |
| `discount_application_index` | `discountAllocations.discountApplication.index` |

A child table is emitted only when at least one line item carries a non-empty list; line items
with an empty list contribute no rows.

#### Customer journey / marketing attribution (`customerJourneySummary`)

`Order.customerJourneySummary` is used for marketing attribution (deriving order source
categories such as Google Brand/Non-brand, Meta, TikTok, Email, Influencer, Direct). Unlike the
money sets above, it is **flattened directly onto `order.csv` as `__`-separated scalar columns**
(not decomposed into an `order_customer_journey_summary` child table and not left as a serialized
JSON blob). This matches the downstream dbt mapping, e.g.
`landing_site ← customer_journey_summary__first_visit__landing_page` and
`referring_site ← customer_journey_summary__first_visit__referrer_url`.

29 columns are added to `order.csv`:

| Column | Source (GraphQL) |
| --- | --- |
| `customer_journey_summary__ready` | `customerJourneySummary.ready` |
| `customer_journey_summary__customer_order_index` | `customerJourneySummary.customerOrderIndex` |
| `customer_journey_summary__days_to_conversion` | `customerJourneySummary.daysToConversion` |
| `customer_journey_summary__first_visit__id` | `customerJourneySummary.firstVisit.id` |
| `customer_journey_summary__first_visit__occurred_at` | `customerJourneySummary.firstVisit.occurredAt` |
| `customer_journey_summary__first_visit__landing_page` | `customerJourneySummary.firstVisit.landingPage` |
| `customer_journey_summary__first_visit__referrer_url` | `customerJourneySummary.firstVisit.referrerUrl` |
| `customer_journey_summary__first_visit__source` | `customerJourneySummary.firstVisit.source` |
| `customer_journey_summary__first_visit__source_description` | `customerJourneySummary.firstVisit.sourceDescription` |
| `customer_journey_summary__first_visit__source_type` | `customerJourneySummary.firstVisit.sourceType` |
| `customer_journey_summary__first_visit__referral_code` | `customerJourneySummary.firstVisit.referralCode` |
| `customer_journey_summary__first_visit__utm_parameters__source` | `customerJourneySummary.firstVisit.utmParameters.source` |
| `customer_journey_summary__first_visit__utm_parameters__medium` | `customerJourneySummary.firstVisit.utmParameters.medium` |
| `customer_journey_summary__first_visit__utm_parameters__campaign` | `customerJourneySummary.firstVisit.utmParameters.campaign` |
| `customer_journey_summary__first_visit__utm_parameters__term` | `customerJourneySummary.firstVisit.utmParameters.term` |
| `customer_journey_summary__first_visit__utm_parameters__content` | `customerJourneySummary.firstVisit.utmParameters.content` |
| `customer_journey_summary__last_visit__id` | `customerJourneySummary.lastVisit.id` |
| `customer_journey_summary__last_visit__occurred_at` | `customerJourneySummary.lastVisit.occurredAt` |
| `customer_journey_summary__last_visit__landing_page` | `customerJourneySummary.lastVisit.landingPage` |
| `customer_journey_summary__last_visit__referrer_url` | `customerJourneySummary.lastVisit.referrerUrl` |
| `customer_journey_summary__last_visit__source` | `customerJourneySummary.lastVisit.source` |
| `customer_journey_summary__last_visit__source_description` | `customerJourneySummary.lastVisit.sourceDescription` |
| `customer_journey_summary__last_visit__source_type` | `customerJourneySummary.lastVisit.sourceType` |
| `customer_journey_summary__last_visit__referral_code` | `customerJourneySummary.lastVisit.referralCode` |
| `customer_journey_summary__last_visit__utm_parameters__source` | `customerJourneySummary.lastVisit.utmParameters.source` |
| `customer_journey_summary__last_visit__utm_parameters__medium` | `customerJourneySummary.lastVisit.utmParameters.medium` |
| `customer_journey_summary__last_visit__utm_parameters__campaign` | `customerJourneySummary.lastVisit.utmParameters.campaign` |
| `customer_journey_summary__last_visit__utm_parameters__term` | `customerJourneySummary.lastVisit.utmParameters.term` |
| `customer_journey_summary__last_visit__utm_parameters__content` | `customerJourneySummary.lastVisit.utmParameters.content` |

> **Asynchronous attribution caveat:** `customer_journey_summary__ready` reflects Shopify's
> `ready` flag — "whether the attributed sessions for the order have been created yet". Attribution
> is computed asynchronously, so recently placed orders can arrive with `ready = false` and **null
> visit fields**, and `customerJourneySummary` itself can be **null** entirely. In all of these
> cases every one of the 29 columns is still emitted (with empty/null values) — column presence on
> `order.csv` never depends on the data. Values are extracted as-is on each run; the component does
> **not** poll or retry waiting for attribution to become ready.

#### Order address child tables (`order_shipping_address`, `order_billing_address`)

`Order.shippingAddress` and `Order.billingAddress` are decomposed 1:1 into the
`order_shipping_address.csv` and `order_billing_address.csv` child tables (`parent_id` →
`order.id`). In addition to the existing address columns, both tables carry a `country_code`
column:

| Column | Source (GraphQL) | Format |
| --- | --- | --- |
| `country` | `shippingAddress`/`billingAddress`.`country` | full country display name (e.g. `Czechia`, `Netherlands`) |
| `country_code` | `shippingAddress`/`billingAddress`.`countryCodeV2` (aliased to `countryCode`) | ISO 3166-1 alpha-2 two-letter code (e.g. `NL`, `AT`, `DE`) |

`country_code` is the ISO two-letter code (the legacy v1 `order.shipping_address__country_code`),
while `country` remains the human-readable display name — they are distinct columns. The value is
nullable: guest or partial addresses can arrive with `country_code` empty.

#### Order shipping lines & discount codes

Enabled with the `order_shipping_discounts` endpoint flag. Unlike the other order data, these two
tables are produced by a **paginated GraphQL pass** (not by bulk operations). This is required
because `Order.shippingLines` and `Order.discountApplications` are connections whose node types do
NOT implement the Shopify `Node` interface (verified on API version `2025-10`): `ShippingLine` has a
nullable `id` and implements no interfaces, and `DiscountApplication` has no `id` field at all.
Shopify bulk operations require "Connections must implement the Node interface", so bulk cannot be
used for them. Both connections are fetched together in a single paginated query per order page, and
the order-selection/date filter mirrors the bulk orders extraction so the same order set is covered.

**Column naming note (mapping to the requested v1-style names):** money values are flattened into
`__`-separated columns (e.g. `price_set__shop_money__amount`), not serialized JSON blobs. Column
names follow the component's conventions rather than the originally requested names — the mapping is:

| Component column | Requested name |
| -- | -- |
| `parent_id` | `order_id` |
| `row_number` | `row_nr` |

`row_number` is the deterministic 0-based index of the record in the order's API response array
(for `order_discount_codes` it is the index among the emitted code-type applications), so it is
stable across runs and does not rely on nondeterministic SQL row numbering.

**`order_shipping_lines.csv`** — one row per shipping line per order (PK: `parent_id` + `row_number`):

| Column | GraphQL source |
| -- | -- |
| `parent_id` | `order.id` |
| `id` | `shippingLines.nodes.id` (nullable) |
| `row_number` | array index of `shippingLines.nodes` |
| `title` | `shippingLines.nodes.title` |
| `code` | `shippingLines.nodes.code` |
| `source` | `shippingLines.nodes.source` |
| `price_set__shop_money__amount` | `shippingLines.nodes.originalPriceSet.shopMoney.amount` |
| `price_set__shop_money__currency_code` | `shippingLines.nodes.originalPriceSet.shopMoney.currencyCode` |
| `discounted_price_set__shop_money__amount` | `shippingLines.nodes.discountedPriceSet.shopMoney.amount` |
| `discounted_price_set__shop_money__currency_code` | `shippingLines.nodes.discountedPriceSet.shopMoney.currencyCode` |
| `tax_lines` | `shippingLines.nodes.taxLines`, serialized as a JSON string (a plain column, NOT a child table) |

**`order_discount_codes.csv`** — one row per code-type discount application per order
(PK: `parent_id` + `row_number`). Only `DiscountCodeApplication` nodes produce rows; automatic,
manual, and script discount applications are skipped.

| Column | GraphQL source |
| -- | -- |
| `parent_id` | `order.id` |
| `row_number` | index among the emitted code-type discount applications |
| `code` | `discountApplications.nodes ... on DiscountCodeApplication { code }` |
| `discount_application_index` | `discountApplications.nodes.index` (position in the full discount-applications list) |
| `value_type` | `discountApplications.nodes.value.__typename` (`MoneyV2` or `PricingPercentageValue`) |
| `value_amount` | `value ... on MoneyV2 { amount }` |
| `value_currency_code` | `value ... on MoneyV2 { currencyCode }` |
| `value_percentage` | `value ... on PricingPercentageValue { percentage }` |

> **⚠️ `value_percentage` sign convention:** Shopify's docs describe `PricingPercentageValue.percentage`
> as a value in the range **-100 to 0** (negative = discount, `-100` = free), but real-store validation
> shows the live API returning **positive** values (e.g. `25.0` for a 25%-off code). The value is
> extracted **as-is** with no sign manipulation, so it may arrive as either sign depending on the store/API.
> Consumers should treat the magnitude as the discount percentage — e.g. use `abs(value_percentage)` — to
> stay correct regardless of sign.

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
