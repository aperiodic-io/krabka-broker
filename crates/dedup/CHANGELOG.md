# Changelog

Krabka versions and tags the whole workspace as one unit, so this crate has no
release of its own. The [root changelog](../../CHANGELOG.md) records every
release that contains `krabka-dedup`.

## [Unreleased]

### Changed

- The index of a partition is 64 hash tables, not one. When a table grows or
  shrinks, it moves only its own entries. At two million identities, the
  slowest single `classify` falls from about 90 ms to a few milliseconds. Each
  table holds at most `max(2 * ceil(max_entries / 64), 4096)` entries, so
  identities chosen to share a table cannot bring the whole-index move back.
- A table that falls to a quarter of its capacity shrinks to twice its length,
  so the heap follows the live entries down after a burst expires.

### Added

- `DedupWindowFilter::SEGMENTS`, `heap_bytes` and `largest_relocation`.
