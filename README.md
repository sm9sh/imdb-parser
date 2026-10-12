# imdb-parser

Import and update IMDb `title.basics` and `title.ratings` in a local SQLite database.

## Requirements and installation

Use PHP 8.2+ with cURL, zlib and PDO SQLite, SQLite 3.24.0+ and Composer. Verified on PHP 8.2.34, SQLite 3.53.4 and Doctrine DBAL 2.13.9. SQLite upsert requires [SQLite 3.24.0](https://sqlite.org/lang_upsert.html).

```sh
git clone https://github.com/sm9sh/imdb-parser
cd imdb-parser
composer install
cp config.php.example config.php
php run.php --help
```

On Windows, use `Copy-Item config.php.example config.php`. Review the config before import. No command creates a config automatically. The local `composer.lock` is ignored; a fresh install resolves the constraints in `composer.json`. Review dependency updates. DBAL 2 uses the abandoned `doctrine/cache` package; upgrading DBAL is a separate task.

This checkout provides PHP 8.2.34 in `.tools/php-8.2/` and Composer in `.tools/composer.phar`. These tools are ignored. For a fresh Windows checkout, install [PHP 8.2](https://www.php.net/downloads.php?os=windows&version=8.2) and Composer, or put them on PATH. `run.bat` selects the local runtime when available.

```powershell
.\run.bat --help
.\.tools\php-8.2\php.exe .tools\composer.phar install
.\.tools\php-8.2\php.exe tests\run.php
```

Use PHP 8.2 on PATH for generic `php` and `composer` commands. Set the PhpStorm CLI interpreter to `.tools/php-8.2/php.exe` and language level to 8.2.

## Configuration and commands

`DATABASE_URL` defaults to `db/imdb.sqlite`. SQLite creates the directory, file, `title` table and indexes only for database actions. `DOWNLOAD_DIR` may contain spaces and need not end with a slash. Relative download paths resolve from the project directory.

`TRANSACTION_PORTION` is a positive integer (default 2000 data rows). `CONNECT_TIMEOUT` and `TRANSFER_TIMEOUT` are positive seconds (10 and 3600). `DOWNLOAD_RETRIES` is 0–5 (default 2 retries after the first attempt). `DATASET_BASE_URL` defaults to IMDb; it supports an HTTP(S) mirror without credentials.

| Command | Action |
| --- | --- |
| `php run.php --help` | Show help without config, database or file changes. No arguments do the same. |
| `php run.php -d` | Download and validate both gzip files; no database access. |
| `php run.php -u` | Extract both local gzip files; no database access. |
| `php run.php -p` | Validate both TSV headers, then import basics and update ratings. |
| `php run.php -a` | Download, extract and import; keep existing titles. |
| `php run.php -t` | Explicitly clear the title table. This deletes local title data. |

Flags may be combined, for example `-u -p`. `-a` never implies `-t`. File/header checks precede clearing when `-p` is present. Each file is replaced separately: an error on the second file can leave the first successfully replaced.

The project lock prevents two CLI processes in the same checkout from changing shared files. It does not coordinate different hosts or checkouts. Close other SQLite writers before import or backup.

## Title selection

The CLI defaults to `IMPORT_COMPLETED_ONLY => true`, including configs without this key. The current year is read once per run. Import only:

- `movie` with a known `startYear` no later than the current year.
- `tvSeries` and `tvMiniSeries` with a known `endYear` no later than the current year.

Other types, unknown required years and future years are skipped. Ratings update only matching selected titles. IMDb provides release/start and series end years; these rules approximate completion and cannot confirm exact release or finale dates. See the [dataset specification](https://data.imdb.com/non-commercial-datasets/).

Both archives still download in full; filtering happens during import. Set `IMPORT_COMPLETED_ONLY => false` explicitly to import every type. The value must be a PHP boolean. Import does not delete previous titles that fall outside the filter. Back up and prune existing data separately when needed.

## Wikidata countries

IMDb remains the source of title metadata and ratings. Country enrichment uses Wikidata IMDb IDs (P345), countries of origin (P495), and optional ISO alpha-2 codes (P297). Multiple countries are stored separately; historical countries without a code retain their QID.

Country mode is disabled when `WIKIDATA_COUNTRIES_ENABLED` is absent or false. Set it to true after the explicit migration:

```powershell
.\run.bat --migrate-countries
.\run.bat --countries --country-limit=100
.\run.bat --countries
```

Migration requires SQLite 3.27.0+ for `VACUUM INTO`. It saves a verified backup beside the database before creating `title_country` and `country_lookup`. It does not change title columns. Both new actions run standalone; `--country-limit` is a positive integer and applies only to `--countries`. A repeated sync skips fresh entries and resumes missing or stale IDs. Repeated migration validates the existing schema without creating another backup. Partial or incompatible country schemas require explicit repair.

Settings: `EXCLUDE_COUNTRIES` defaults to `['IN']`; `WIKIDATA_REFRESH_DAYS` defaults to 30. `WIKIDATA_ENDPOINT` defaults to the public SPARQL endpoint. Production endpoints must use HTTPS without URL credentials; HTTP is allowed only on loopback for fixtures. No API key or extra dependency is required.

With country mode enabled, `-p`/`-a` first stream eligible basics IDs and refresh their cache, then import with local country checks. Network access runs before title transactions and before clearing for `-t -p`. Initial enrichment can take many requests. Requests are sequential, with batches of 100 IDs, a 2 MiB response limit, timeouts and bounded transient retries. HTTP failures stop import; they do not become unknown results. Completed cache batches remain saved after a later failure.

India is excluded if a country has code `IN` or QID `Q668`, including co-productions. Basics and ratings for excluded existing titles stay unchanged; import does not delete them. Unknown countries are retained. Country lookup counters describe newly refreshed IDs; `country_unknown` in basics counts selected rows with no country. `country_skipped` counts basics excluded by country and also contributes to `skipped`. Ratings without an eligible match count as unmatched.

For example, select titles linked to Ukraine without duplicating co-productions:

```sql
SELECT t.* FROM title AS t
WHERE EXISTS (
    SELECT 1 FROM title_country AS c
    WHERE c.tconst = t.tconst AND c.country_code = 'UA'
);
```

Use `country_qid` to select a historical country without an ISO code. A missing `country_lookup` row means not yet checked; a lookup row with no countries means a successful unknown result. Country cache is preserved by `-t` and can contain excluded IDs absent from `title`. Full coverage is not guaranteed: [Wikidata properties](https://www.wikidata.org/wiki/Property:P495) are community data. See [WDQS access and limits](https://www.mediawiki.org/wiki/Wikidata_Query_Service/Implementation).

## Data, failures and schema

The importer preserves literal quotes, UTF-8, long titles and empty trailing fields. `\N` becomes SQL NULL. Years use integers, including 1894; missing numeric fields and ratings stay NULL. Invalid headers, column counts, identifiers or numbers stop the import and report the file and line.

Basics uses upsert by `tconst`; the last duplicate wins. Records absent from a later dataset remain in the database. Basics leaves ratings intact. Ratings update selected existing titles; rows without an eligible match are counted as unmatched and do not create incomplete titles. `updated` uses the database's `CURRENT_TIMESTAMP` (UTC in SQLite).

Each committed transaction remains saved. A failure rolls back the current transaction; it does not undo earlier batches or the completed basics import. Fix the input or environment, then repeat `-p` from the start. No resume checkpoint is required. Exit 0 means success; errors use stderr and exit 1. Summaries show processed/committed rows, skipped basics, unmatched ratings, data SQL statement count, elapsed time and PHP peak memory. Basics counts only selected rows; skipped rows do not enter transactions. Ratings counts include unmatched rows, so committed is not the number of changed titles.

Downloads use timeouts and retry temporary network/HTTP failures. Download and extraction write separate temporary files and validate gzip content, checksum and trailer before replacing the destination. Failed transfers preserve the previous destination and attempt to clean up temporary files. If the OS blocks deletion, a .part file can remain without a residual-path diagnostic; inspect the download directory after a failed operation. Single-member gzip streams are supported.

The existing SQLite schema is retained. An incompatible table fails before import; `CREATE TABLE IF NOT EXISTS` does not migrate it. No column migration was required for this release. To change an incompatible schema, stop all database users, back up the complete SQLite database, and apply a separately reviewed migration. Do not use `-t` as a migration.

MySQL remains a legacy backend via `DATABASE_URL` and PDO MySQL. Its old schema has year, title-length and nullable-field restrictions. This completion plan verifies SQLite; do not treat MySQL as validated for current datasets.

## Tests and measurements

```sh
php tests/run.php
php tests/SqliteTest.php
php tests/benchmark.php baseline 100000
php tests/benchmark.php optimized 100000
php -l run.php
composer validate --no-check-publish
composer check-platform-reqs
```

Tests use explicit assertions, their own temporary SQLite files and a local HTTP fixture. They do not load your config, use MySQL or contact IMDb. No coverage threshold is set. The suite covers parsing, completion filters and year boundaries, duplicate updates, unmatched ratings, transaction failure/retry, CLI side effects/locking, timeout, HTTP errors and corrupted gzip. Benchmark databases are disposable. See [measured results](docs/import-benchmark.md).

IMDb datasets are for personal/non-commercial use, subject to [IMDb's terms and dataset specification](https://data.imdb.com/non-commercial-datasets/).
