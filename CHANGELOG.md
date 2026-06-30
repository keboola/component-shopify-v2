# Changelog

## [Unreleased]

### Added
- `order_refunds` endpoint: extracts refund data via paginated GraphQL (not bulk) into 5 flat tables (`refund`, `refund_line_item`, `refund_order_adjustment`, `refund_shipping_line`, `refund_transaction`). All money fields include both shop and presentment currency. Primary keys use stable Shopify GIDs. Requires the `orders` toggle to be enabled in the UI.
