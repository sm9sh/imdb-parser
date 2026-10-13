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

- [x] Write failing CLI tests for --migrate-countries, --countries/--country-limit, help, missing migration, India/co-production/unknown import and repeated ratings.
- [x] Run .tools/php-8.2/php.exe tests/CountryCliTest.php; expect unknown flags or missing filter.
- [x] Add standalone migration/country actions and validate settings before side effects.
- [x] Prefetch eligible basics IDs in a streaming pass before clearing or writing title.
- [x] At basics flush, query excluded tconst for at most 100 pending rows; remove excluded rows before beginning/advancing the transaction. Ratings add a parameterized NOT EXISTS country predicate.
- [x] Test -t -p failure preservation, invalid config, cached unknowns, rollback with skipped countries and legacy MySQL rejection.
- [x] Run both country files and tests/run.php; commit named task files.

## Task 3: Verification, documentation and activation
Files: update README.md, PLAN.md and this plan/spec status.
- [x] Document commands, cache/unknown semantics, country SQL JOIN example and first-import network cost.
- [x] Lint changed PHP files and run full suite; inspect the final diff.
- [x] Run explicit migration on working SQLite, verify backup and retained title data.
- [x] Run --countries --country-limit=100 and an explicit small live lookup for Indian and co-production examples. Record counts and source results without claiming full coverage.
- [x] Enable country mode in ignored local config after successful migration/live check.
- [x] Obtain an independent review of the final feature diff; resolve material findings with regression tests.
- [x] Commit verified changes; report measured sample coverage, remaining full backfill and backup path.

## Execution Record
Task 1: country module RED (missing functions) -> GREEN, 9 country scenarios passed; baseline plus task suite 50/50 passed in 12 files. Task 2: 10 CLI scenarios RED (unknown flags, missing filter/preflight) -> GREEN. Official Wikidata SomeValue blank-node/Skolem forms exposed a parser failure; added regression RED -> GREEN without guessing country codes. Full suite 61/61 scenarios in 13 files passed; all changed PHP files linted; diff whitespace clean. Full 612k-title country backfill remains a separate long-running operation.

Task 3: migration backup db/imdb-before-countries-20261012-140621-e5dc9d1a.sqlite; before/backup/after full-title SHA-256 matched, title_count=612090, rated=311421, quick_check=ok. Pilot100 returned45 known/55unknown; 4 explicit public examples confirmed India and US/AU. Cache104 checked/49known/55unknown,50rows;611986 stored titles remain. Local country mode enabled. Repeated public sample used fresh cache with checked=0. Evidence db/wikidata-countries-20261012/.

Final review: fresh gpt-6-astra reviewed c3b0107..2eaea94; five focus checks passed, independently ran20/20 country/CLI scenarios; no Critical or Minor findings. One Important/P2: Retry-After above60 seconds truncated, causing early retry. Regression with HTTP429 delta300 and HTTP503 date+300 failed before fix, passed after fix; Final: fixed long Retry-After — numeric/date CLI regression RED -> GREEN; full suite 62/62 in 13 files, exit0, lint and whitespace clean.
Final: Ruling: stop when server Retry-After exceeds60 seconds instead of waiting indefinitely — honors server cooldown within the bounded client policy — cost: manual rerun after the reported interval.
Final: Ruling: full612k backfill remains a separate operation — approved spec explicitly separates it from readiness — cost:611986 stored titles have no lookup yet.
Final: Ruling: accept activation and preservation from the executor's live measurements — reviewer deliberately did not access working config/database; fingerprints and local state were independently checked against the backup — cost: live HTTP/filter examples cover only104 IDs.
Final: Ruling: do not claim complete Wikidata coverage — unknown rows are retained by the approved policy, and fixture/live samples cannot prove all community data — cost: Indian titles without known P495 can pass the filter.

Task 3: complete; all readiness checks passed. Final full suite log: db/wikidata-countries-20261012/suite-final.log. Existing branch codex/complete-cli retained; user-authorized verified commits only, no merge/push/PR.
