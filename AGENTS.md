# Repository Guidelines

## Project Structure & Module Organization

This PHP CLI tool imports IMDb datasets into SQLite by default; MySQL remains available. `run.php` contains argument handling, downloads, gzip extraction, table creation, and imports. `run.bat` selects the runtime and forwards arguments on Windows. `composer.json` declares dependencies; `vendor/` contains installed packages. Copy `config.php.example` to `config.php` for local settings. `exchange/` holds downloads; `db/imdb.sqlite` stores the default database.

## Build, Test, and Development Commands

Use PHP 8.2 with cURL, zlib, and PDO SQLite. `.tools/php-8.2/php.exe` is the local runtime; `run.bat` selects it. For `php` and `composer` commands, use PHP 8.2 on PATH. Run from the repository root.

- `composer install`: install declared dependencies. The repository does not track a lockfile.
- `composer update`: refresh dependencies intentionally.
- `php -l run.php`: check parser syntax without executing imports.
- `php -l config.php.example`: check the configuration template.
- `php run.php -a`: download, extract, and import both configured datasets.
- `php run.php -d`, `-u`, or `-p`: run download, extraction, or import separately.
- `.\run.bat -a`: run the full workflow on Windows.

SQLite creates its file and directory automatically. Configure `DATABASE_URL` for another database. There is no separate build step.

## Coding Style & Naming Conventions

Use four spaces for indentation and match nearby brace placement. Existing helper functions use camelCase, such as `downloadFile`; local variables use snake_case, such as `$out_dir`; classes use PascalCase. Configuration keys use uppercase snake_case. Keep SQL column names aligned with IMDb TSV headers. Preserve streaming reads and transaction batching for large datasets. No formatter or linter is configured.

## Testing Guidelines

Run `php tests/SqliteTest.php` for the SQLite integration test. It uses the real CLI, isolated fixtures, and a temporary database; checks import, repeated import, NULL values, Unicode, historical years, and clearing; then cleans up. Lint changed PHP files. No coverage threshold exists. Name additional tests `tests/*Test.php` and document their commands.

## Commit & Pull Request Guidelines

History uses short messages such as `+ run.bat` and `* fix`. Prefer clear, concise English messages, such as `docs: add repository guidelines`. Stage specific paths. Describe changes, validation, configuration impact, and related issues in pull requests. Obtain approval before pushing or opening a pull request.

## Security & Configuration Tips

Keep local credentials, SQLite files, downloaded datasets, `vendor/`, and IDE files out of commits. Every configured invocation can create the `title` table. `-t` clears it; use a disposable database for tests. Keep database values parameterized through Doctrine DBAL.
