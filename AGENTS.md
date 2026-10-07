# Repository Guidelines

## Project Structure & Module Organization

This PHP CLI tool imports IMDb datasets into MySQL. `run.php` contains argument handling, downloads, gzip extraction, table creation, and imports. `run.bat` forwards arguments on Windows. `composer.json` declares dependencies; `vendor/` contains installed packages. Copy `config.php.example` to `config.php` for local settings. `exchange/` is the default download directory. There are no web assets.

## Build, Test, and Development Commands

Run commands from the repository root. Use PHP 7.1+ with cURL, zlib, and PDO MySQL. `composer.json` declares PHP 7.0, but installed DBAL requires 7.1+. Preserve compatible syntax.

- `composer install`: install declared dependencies. The repository does not track a lockfile.
- `composer update`: refresh dependencies intentionally.
- `php -l run.php`: check parser syntax without executing imports.
- `php -l config.php.example`: check the configuration template.
- `php run.php -a`: download, extract, and import both configured datasets.
- `php run.php -d`, `-u`, or `-p`: run download, extraction, or import separately.
- `.\run.bat -a`: run the full workflow on Windows.

Create the MySQL database and configure `DATABASE_URL` before running. There is no separate build step.

## Coding Style & Naming Conventions

Use four spaces for indentation and match nearby brace placement. Existing helper functions use camelCase, such as `downloadFile`; local variables use snake_case, such as `$out_dir`; classes use PascalCase. Configuration keys use uppercase snake_case. Keep SQL column names aligned with IMDb TSV headers. Preserve streaming reads and transaction batching for large datasets. No formatter or linter is configured.

## Testing Guidelines

No project test framework or coverage threshold exists. Lint changed PHP files. Test imports with small `title.basics.tsv` and `title.ratings.tsv` fixtures in a temporary download directory and a disposable MySQL database. Check header handling, IMDb `\N` values, inserts, updates, ratings, and transaction boundaries. Report the commands and results. If adding automated tests, place them in `tests/` with `*Test.php` names and document their runner.

## Commit & Pull Request Guidelines

History uses short messages such as `+ run.bat` and `* fix`. Prefer clear, concise English messages, such as `docs: add repository guidelines`. Stage specific paths. Describe changes, validation, configuration impact, and related issues in pull requests. Obtain approval before pushing or opening a pull request.

## Security & Configuration Tips

Keep local credentials, downloaded datasets, `vendor/`, and IDE files out of commits. Every configured invocation can create the `title` table. `-t` truncates it; use only a disposable database for tests. Keep database values parameterized through Doctrine DBAL.
