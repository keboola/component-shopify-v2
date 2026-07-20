# Changelog

## 0.3.2 - 2026-07-20

### Fixed

- **DuckDB out-of-memory on large order syncs.** The extractor previously ran DuckDB in-memory (`:memory:`) with a hardcoded 256 MB limit, so base tables could not spill to disk and large syncs (wide-window backfills and ordinary 60-day production windows on high-volume stores) aborted with `Out of Memory Error: could not allocate block ... (244.1 MiB/244.1 MiB used)` during the base-table load. DuckDB now uses a file-backed database under `/tmp/shopify_duckdb/` (unique per run; debug mode writes `/tmp/shopify_duckdb/debug.duckdb` instead of a relative `debug.duckdb` under the read-only `/code` overlay), letting base tables spill to disk. `/tmp` is excluded from the 10 GB overlay budget. The memory limit is raised to an explicit 320 MB (headroom for Python + untracked DuckDB allocations inside the 512 MB container), and the effective limit is logged at startup; the DuckDB file size is logged at end of run. `temp_directory` and `preserve_insertion_order` are unchanged. Storage-layer change only - no output schema or endpoint changes (SUPPORT-12550).
- **DuckDB thread count pinned to 2 (`SET threads=2`).** DuckDB otherwise defaults its thread count to the visible core count, so its transient per-thread allocations (e.g. `read_json_auto` parse buffers, which cannot spill) scaled with container CPU while `memory_limit` stayed fixed - a "medium" 4-CPU backend OOM'd (`305.1 MiB/305.1 MiB used`) on a 54k-order-item workload that passed in 7.5 s on a 2-CPU container. The constant is not derived from CPU count, so memory behavior no longer varies with backend size. The effective thread count is also logged at startup.
- **Inventory endpoint now always extracts the full current snapshot, ignoring `date_since`/`date_to`.** The inventory bulk query windowed `inventoryItems` by `updated_at`, but `InventoryItem.updatedAt` only moves on catalog-record edits, not on stock changes - so any narrow window silently produced a stale snapshot or (with 0 matching items) no inventory tables at all, while jobs still succeeded. Inventory is now un-windowed like `locations`: it always extracts the full current stock snapshot regardless of the configured date range. Behavior change - the five inventory tables (`inventory_item`, `inventory_level`, `inventory_location`, `inventory_quantities`, `inventory_variant`) now reflect the entire catalog every run. For very large catalogs (100k+ SKUs) this makes the inventory bulk operation heavier (SUPPORT-12550).

## 0.3.1 - 2026-07-20

### Fixed

- Bulk-download log lines no longer emit the Shopify-returned GCS pre-signed URL in full. The `Downloading results from: ...` INFO log now logs only the base URL (path before `?`), stripping the `Signature`, `GoogleAccessId`, and `Expires` query params. Previously these short-lived credentials were written verbatim into job logs (and forwarded to log aggregation), letting anyone with log-read access download the raw export until the URL expired. The download itself is unaffected - it still uses the full signed URL. Applies to all bulk endpoints (products, orders, customers, collections, inventory, custom).

## 0.3.0 - 2026-07-20

### Added

- `order_refunds` endpoint: extracts refund data via paginated GraphQL (not bulk) into 5 flat tables (`refund`, `refund_line_item`, `refund_order_adjustment`, `refund_shipping_line`, `refund_transaction`). All money fields include both shop and presentment currency. Primary keys use stable Shopify GIDs. Standalone endpoint (does not require the `orders` toggle).
- `collection_metafields` endpoint option: when enabled alongside `collections`, per-collection metafields are extracted into a distinct `collection_metafield` table (keyed by `parent_id` = collection GID), mirroring the product-metafields pattern.

### Fixed

- Collections endpoint now emits its collection↔product mapping as `collection_product` (was the generic `product`), so it no longer collides with the full-schema `product` table from the products endpoint when both are enabled.
- Entity-split table manifests now declare only their own columns (previously inherited the full parent schema). Metadata only; CSV output unchanged.
- **`date_to` no longer silently excludes records updated on the run day.** The loading-option date bounds
  were truncated to bare `YYYY-MM-DD`, so the upper bound became midnight of the run day
  (`updated_at:<'2026-07-10'`). Any record updated earlier the same day was dropped from the extraction,
  even with `date_to: "now"`. The upper bound is now emitted as a full ISO-8601 UTC timestamp of the actual
  run moment (`updated_at:<'2026-07-10T13:56:13Z'`), so same-day records are included. `date_since` remains
  floored to midnight of its day (window rounding is outward, never inward). Unset `date_to` still emits no
  upper bound. Applies uniformly to all endpoints (SUPPORT-12550).
- Date bounds are now resolved in UTC, so the emitted timestamp is a real UTC instant regardless of the
  runtime container's timezone (previously the naive local time was labelled `Z`, which was only correct
  while the container ran UTC).

### Changed

- **`orders_legacy`: the `date_to` upper bound is now exclusive (`<`) instead of inclusive (`<=`).**
  Configs with an explicit calendar-date `date_to` (e.g. `2026-07-10`) now exclude the boundary day,
  matching the documented "Period end date [excluding]" contract and all bulk endpoints. The date bounds
  are also quoted in the query, required because timestamp values contain colons (Shopify search's field
  separator).
