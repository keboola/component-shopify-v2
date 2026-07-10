# Changelog

## Unreleased

### Fixed

- **`date_to` no longer silently excludes records updated on the run day.** The loading-option date bounds
  were truncated to bare `YYYY-MM-DD`, so the upper bound became midnight of the run day
  (`updated_at:<'2026-07-10'`). Any record updated earlier the same day was dropped from the extraction,
  even with `date_to: "now"`. The upper bound is now emitted as a full ISO-8601 UTC timestamp of the actual
  run moment (`updated_at:<'2026-07-10T13:56:13Z'`), so same-day records are included. `date_since` remains
  floored to midnight of its day (window rounding is outward, never inward). Unset `date_to` still emits no
  upper bound. Applies uniformly to all endpoints (SUPPORT-12550).
