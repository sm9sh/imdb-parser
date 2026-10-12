# Wikidata Countries Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox syntax for tracking. The user chose inline execution with "роби".

**Goal:** Add Wikidata country enrichment and exclude India during IMDb import.
**Architecture:** A bounded HTTP client and transactional SQLite cache live in lib/countries.php. CLI prefetches country IDs before title writes; importer uses local SQL cache for filtering.
**Tech Stack:** PHP 8.2, Doctrine DBAL 2.13.9, PDO SQLite, cURL; no added dependencies.
**Spec:** docs/superpowers/specs/2026-10-12-wikidata-countries-design.md

## Global Constraints
- Country data only from P345/P495/P297; preserve IMDb metadata and ratings.
- Sequential batches of 100 IDs, refresh after 30 days, response limit 2 MiB, at most 2 retries.
- India Q668 or ISO IN causes exclusion; unknown countries are retained and counted.
- Explicit migration with verified backup; no deletion of existing titles.
- HTTP outside title transactions; local HTTP fixture only in tests; stage named files and do not push.

## Review Focus
- Missing response IDs must fail rather than create a false unknown cache entry.
- HTTP failure before -t -p must preserve existing titles.
- No ISO code and conflicting ISO codes must preserve country QID.
- Country-filtered rows must not change transaction boundaries.
- Cached excluded titles must not receive ratings updates.

## Task 1: Country HTTP and cache
Files: create lib/countries.php, tests/CountryTest.php, tests/fixtures/wikidata.php.
Interfaces: countrySettings(array), assertCountrySchema(conn), migrateCountrySchema(conn,path), fetchCountryBatch(ids,settings), syncCountryIds(conn,ids,settings).

- [x] Write failing tests with a local fixture: tt1 IN; tt2 US/AU; tt3 historical QID/no code; tt4 unknown. Assert exact cache rows and fresh-cache request count.
- [x] Run .tools/php-8.2/php.exe tests/CountryTest.php; expect missing functions.
- [x] Implement migration, strict SPARQL response validation, sequential HTTP retries, and atomic replacement: DELETE FROM title_country WHERE tconst=?; insert deduplicated QIDs; upsert checked_at in the same transaction.
- [x] Exercise corrupt/partial/foreign-ID responses, oversized body, timeout, 429 retry, 404, expiry and trigger-induced rollback.
- [x] Run the targeted file and tests/run.php; commit only the task files.

## Task 2: CLI integration and import filter
Files: modify run.php, lib/importer.php, config.php.example, tests/bootstrap.php; create tests/CountryCliTest.php.
Interfaces: completedTitle(row,year), syncCountryDataset(conn,path,year,settings), syncStoredCountries(conn,settings,limit), countryExclusionSql(codes).

- [ ] Write failing CLI tests for --migrate-countries, --countries/--country-limit, help, missing migration, India/co-production/unknown import and repeated ratings.
- [ ] Run .tools/php-8.2/php.exe tests/CountryCliTest.php; expect unknown flags or missing filter.
- [ ] Add standalone migration/country actions and validate settings before side effects.
- [ ] Prefetch eligible basics IDs in a streaming pass before clearing or writing title.
- [ ] At basics flush, query excluded tconst for at most 100 pending rows; remove excluded rows before beginning/advancing the transaction. Ratings add a parameterized NOT EXISTS country predicate.
- [ ] Test -t -p failure preservation, invalid config, cached unknowns, rollback with skipped countries and legacy MySQL rejection.
- [ ] Run both country files and tests/run.php; commit named task files.

## Task 3: Verification, documentation and activation
Files: update README.md, PLAN.md and this plan/spec status.
- [ ] Document commands, cache/unknown semantics, country SQL JOIN example and first-import network cost.
- [ ] Lint changed PHP files and run full suite; inspect the final diff.
- [ ] Run explicit migration on working SQLite, verify backup and retained title data.
- [ ] Run --countries --country-limit=100 and an explicit small live lookup for Indian and co-production examples. Record counts and source results without claiming full coverage.
- [ ] Enable country mode in ignored local config after successful migration/live check.
- [ ] Obtain an independent review of the final feature diff; resolve material findings with regression tests.
- [ ] Commit verified changes; report measured sample coverage, remaining full backfill and backup path.

## Execution Record
Task 1: country module RED (missing functions) -> GREEN, 9 country scenarios passed; baseline plus task suite 50/50 passed in 12 files. Task 2 CLI tests are currently RED by design. Full 612k-title country backfill remains a separate long-running operation.
