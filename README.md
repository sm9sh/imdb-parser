# imdb-parser
**Simple parser for free IMDB datasets (https://datasets.imdbws.com/) to SQLite or MySQL database**

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
