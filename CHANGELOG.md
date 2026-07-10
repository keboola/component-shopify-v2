# Changelog

## Unreleased

### Added

- `collection_metafields` endpoint option: when enabled alongside `collections`, per-collection metafields are extracted into a distinct `collection_metafield` table (keyed by `parent_id` = collection GID), mirroring the product-metafields pattern.
- Temporary `collections_diagnostic` config flag (dev-branch only): runs cheap read-only NON-bulk probes (`collectionsCount`, paginated `collections` probes for candidate date-window filters, and direct `collection(id:)` lookups selecting `id/title/handle/updatedAt`) and logs count + Aurora-ID presence per probe. Writes nothing to Storage; to be removed once the retrieval fix is known.

### Fixed

- Collections endpoint now emits its collection↔product mapping as `collection_product` (was the generic `product`), so it no longer collides with the full-schema `product` table from the products endpoint when both are enabled.
- Entity-split table manifests now declare only their own columns (previously inherited the full parent schema). Metadata only; CSV output unchanged.
