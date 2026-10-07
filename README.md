# imdb-parser
**Simple parser for free IMDB datasets (https://datasets.imdbws.com/) to SQLite or MySQL database**

Requires PHP 8.2 (Composer constraint `^8.2`), cURL, zlib, and PDO SQLite.

**Local Windows runtime:**

This checkout has PHP 8.2.34 in `.tools/php-8.2/`. `run.bat` selects that runtime and falls back to PHP on PATH when it is absent. Runtime binaries and local Composer are ignored by Git. For a fresh checkout, install [PHP 8.2 for Windows](https://www.php.net/downloads.php?os=windows&version=8.2) or provide PHP 8.2 on PATH.

- Run the importer: `.\run.bat -a`.
- Run tests: `.\.tools\php-8.2\php.exe tests\SqliteTest.php`.
- Install dependencies: `.\.tools\php-8.2\php.exe .tools\composer.phar install`.

Use PHP 8.2 on PATH for the generic `php` and `composer` commands below. In PhpStorm, the project language level is 8.2; the local CLI interpreter path is `.tools/php-8.2/php.exe`.

SQLite is the default backend. Enable PDO SQLite; `db/imdb.sqlite` and its directory are created automatically. Database files are excluded from Git.

**Install via composer:**

- Run `composer create-project sm9sh/imdb-parser`

**Install from github:**

1. Copy repository:

    `git clone https://github.com/sm9sh/imdb-parser`

2. Enter to dir:

    `cd imdb-parser`

3. Run to install dependencies

    `composer update`

**Next steps:**
- Copy `config.php.example` to `config.php` if needed. The default `DATABASE_URL` points to `db/imdb.sqlite`; configure your working directory with `DOWNLOAD_DIR`.
- To use MySQL instead, configure a MySQL `DATABASE_URL`, enable PDO MySQL, and create the database before running.

- Run to parse

    `php run.php -a`

**Command arguments:**

    -d : Download
    -u : Unzip
    -p : Parse
    -t : Truncate table
    -a : All proceeds

**SQLite integration test:**

Run `php tests/SqliteTest.php`. It imports small fixtures into its own temporary SQLite database, checks repeated import and table clearing, then removes its temporary files.

**Test suite and version policy:**

Run `php tests/run.php` (no network or MySQL). PHP 8.2+ and SQLite 3.24.0+ are required; tested on PHP 8.2.34 / SQLite 3.53.4. The CLI keeps `composer.lock` local and untracked. Use `composer install` on a fresh checkout; it resolves the declared constraints. Review dependency changes before updating your local lock.