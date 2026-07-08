# Changelog

## Unreleased

### Added

- `collection_metafields` endpoint option: when enabled alongside `collections`, per-collection metafields are extracted into a distinct `collection_metafield` table (keyed by `parent_id` = collection GID), mirroring the product-metafields pattern.

### Fixed

- Entity-split table manifests now declare only their own columns (previously inherited the full parent schema). Metadata only; CSV output unchanged.
