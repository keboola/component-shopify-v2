# Changelog

## Unreleased

### Fixed

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
