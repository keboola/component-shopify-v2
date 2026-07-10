# Changelog

## Unreleased

### Added

- `collection_metafields` endpoint option: when enabled alongside `collections`, per-collection metafields are extracted into a distinct `collection_metafield` table (keyed by `parent_id` = collection GID), mirroring the product-metafields pattern.

### Fixed

- **`date_to` no longer silently excludes records updated on the run day.** The loading-option date bounds
  were truncated to bare `YYYY-MM-DD`, so the upper bound became midnight of the run day
  (`updated_at:<'2026-07-10'`). Any record updated earlier the same day was dropped from the extraction,
  even with `date_to: "now"`. The upper bound is now emitted as a full ISO-8601 UTC timestamp of the actual
  run moment (`updated_at:<'2026-07-10T13:56:13Z'`), so same-day records are included. `date_since` remains
  floored to midnight of its day (window rounding is outward, never inward). Unset `date_to` still emits no
  upper bound. Applies uniformly to all endpoints (SUPPORT-12550).
- Collections endpoint now emits its collection↔product mapping as `collection_product` (was the generic `product`), so it no longer collides with the full-schema `product` table from the products endpoint when both are enabled.
- Entity-split table manifests now declare only their own columns (previously inherited the full parent schema). Metadata only; CSV output unchanged.
