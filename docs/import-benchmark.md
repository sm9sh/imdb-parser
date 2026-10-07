# Import benchmark

Date: 2026-10-07. Windows; PHP 8.2.34, SQLite 3.53.4, Doctrine DBAL 2.13.9. Tests use disposable local databases with the same indexes and transaction portion 2000.

Importer revisions: `ff8014c` for the controlled and first full runs; `aa5bfd6` for the repeat run. Later changes affect the test runner and documentation.

## Controlled 100,000-row comparison

Generated UTF-8 basics TSV: 6,666,777 bytes. Both processes used the same deterministic data, machine, indexes and journal defaults. No full import ran during the reported comparison. Times include parsing and data writes; statement counts exclude schema, transaction-control and verification SQL.

| Algorithm | Elapsed | Rows/sec | Data SQL statements | PHP peak memory |
| --- | ---: | ---: | ---: | ---: |
| Baseline from `6b0e2a4`: UPDATE then INSERT | 19.739 s | 5,066 | 200,000 | 2 MiB |
| Bounded prepared upsert | 13.166 s | 7,596 | 1,000 | 4 MiB |

Elapsed time fell 33.3%; throughput rose 49.9%. Use these results as a machine-specific comparison, not a universal SLA. Reproduce with `php tests/benchmark.php baseline 100000` and `php tests/benchmark.php optimized 100000`.

Basics batches hold at most 100 rows / 256 KiB and 900 bound parameters. A single larger row is processed alone without truncation. Transaction boundaries can make smaller SQL batches. Prepared statements are reused for an equal row count. Ratings use one prepared UPDATE per input row. PHP memory depends on the largest row and bounded batch; PHP's memory counter excludes SQLite's own page cache and other native allocations.

## Full local dataset verification

The existing local gzip files were extracted with checksum/trailer validation; no claim is made that these snapshots are current.

| Dataset | Gzip bytes | TSV bytes | Data rows | Maximum line bytes |
| --- | ---: | ---: | ---: | ---: |
| title.basics | 152,355,886 | 743,959,667 | 8,701,401 | 880 |
| title.ratings | 6,065,940 | 20,939,773 | 1,215,671 | 22 |

Local gzip SHA-256 values:

- basics: `14350ce7927b5d437102e745cf70700501abe50d47ce333b8a07e7d8929ecaa1`
- ratings: `86534901f34ee321a8077be208e4b5b82defa2602137a25e9099c7d844783c91`

An independent streaming scan found no incorrect column counts. Source basics NULL counts: startYear 1,123,731; endYear 8,613,724; runtimeMinutes 6,344,149; genres 398,375. First title: `tt0000001`, Carmencita, year 1894. First rating: 5.7 / 1,858 votes.

Both full imports completed with exit 0 in the same isolated SQLite database. The working `db/imdb.sqlite` is outside this benchmark. Transactions contain up to 2,000 data rows; SQLite journal and durability defaults remain unchanged.

| Run / dataset | Processed / committed | Unmatched | Data SQL statements | Elapsed | Rows/sec |
| --- | ---: | ---: | ---: | ---: | ---: |
| First / basics | 8,701,401 / 8,701,401 | 0 | 87,015 | 1,653.124 s | 5,263.6 |
| First / ratings | 1,215,671 / 1,215,671 | 0 | 1,215,671 | 1,123.306 s | 1,082.2 |
| Repeat / basics | 8,701,401 / 8,701,401 | 0 | 87,015 | 2,038.370 s | 4,268.8 |
| Repeat / ratings | 1,215,671 / 1,215,671 | 0 | 1,215,671 | 1,509.710 s | 805.2 |

First total: 2,776.961 s (46 min 17 s). Repeat total: 3,548.345 s (59 min 8 s). PHP peak memory: 4 MiB in each run. These are wall-clock times on this Windows workstation with other activity, not an isolated performance SLA. Unlike the controlled comparison, first/repeat timings cover different insert/update workloads and must not be used as a baseline speedup claim.

Independent checks after each run matched all source row and NULL counts. The database retained exactly 8,701,401 titles; no duplicates appeared. Both rating fields are present in exactly 1,215,671 titles; no partial rating pairs exist. The Carmencita values above match. SQLite `PRAGMA quick_check` returned `ok` after both runs; database size stayed 1,571,409,920 bytes.

After the repeat, a separate streaming verifier compared every field against source TSV using bounded, parameterized lookup by `tconst`: 9 fields for all 8,701,401 basics rows and 3 fields for all 1,215,671 ratings rows. All values matched, including literal text and normalized numeric/NULL values. Verification took 137.075 s. It does not reuse importer helpers and does not depend on source ordering; these TSV snapshots do not follow SQLite BINARY order for identifiers of different lengths.
