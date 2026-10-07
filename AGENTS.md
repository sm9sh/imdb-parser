# Repository Guidelines

## Project Structure & Module Organization

This PHP CLI imports IMDb basics and ratings into SQLite. `run.php` owns arguments, config, process locking and workflow. `lib/importer.php` owns TSV parsing, schema checks and database writes; `lib/files.php` owns downloads and gzip extraction. `run.bat` selects PHP on Windows. `tests/*Test.php`, `tests/bootstrap.php` and `tests/fixtures/` provide isolated checks. `db/imdb.sqlite` holds the working database; `exchange/` holds datasets. Copy `config.php.example` to local `config.php`.

## Build, Test, and Development Commands

Use PHP 8.2+ with cURL, zlib and PDO SQLite; SQLite must be 3.24.0+. Tested versions are in README. The local runtime is `.tools/php-8.2/php.exe`; `run.bat` selects it. Generic commands require PHP 8.2 on PATH.

- `composer install`: resolve and install dependencies. Keep the local lockfile untracked under the current policy.
- `php tests/run.php`: run all isolated test files, including a local HTTP server.
- `php tests/SqliteTest.php`: run the original SQLite smoke test.
- `php tests/benchmark.php optimized 100000`: measure an owned disposable database.
- `php -l run.php`: check syntax; also lint changed helpers and tests.
- `php run.php --help`: inspect flags without side effects.
- `.\run.bat -a`: download, extract and import; preserve existing records.

There is no build step. See README for separate `-d`, `-u`, `-p` and destructive `-t` actions.

## Coding Style & Naming Conventions

Use four spaces, camelCase helpers and snake_case variables. Match nearby brace placement. Configuration keys use uppercase snake_case. Keep SQL names aligned with IMDb headers. Preserve streaming reads, bounded rows/bytes and parameterized SQL. Avoid dependencies for simple helpers. No formatter is configured.

## Testing Guidelines

Use explicit checks that work with PHP assertions disabled. Name tests `tests/*Test.php`. Exercise real CLI behavior, fixtures and disposable databases; never load working credentials. HTTP tests must use the local fixture server. Cover failures, cleanup and partial batches. No coverage threshold exists. Do not run tests against working data.

## Commit & Pull Request Guidelines

Use concise English messages, such as `fix: preserve TSV on extraction failure`. Stage named paths only. Commit verified changes. PR descriptions explain behavior, validation and configuration impact; link relevant issues. Obtain approval before pushing or creating a PR.

## Security & Configuration Tips

Keep config, tools, vendor, datasets, database and IDE files out of Git. Schema changes require an explicit migration and backup. A project process lock protects one checkout only. Report committed batches accurately after failure; do not claim whole-import rollback. MySQL remains an unvalidated legacy backend.
