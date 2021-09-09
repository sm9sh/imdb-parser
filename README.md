# imdb-parser
**Simple parser for free IMDB datasets (https://datasets.imdbws.com/) to mysql database**

Before installing you must create mysql database.

**Install via composer:**

- Run `composer create-project sm9sh/imdb-parser`

**Install from github:**

1. Copy repository:

    `git clone https://github.com/sm9sh/imdb-parser`

2. Enter to dir:

    `cd imdb-parser`

3. Run to install dependencies

    `composer update`

4. Run to copy `config.php.example` to `config.php`:

    `php run.php`

**Next steps:**
- Setup your database and working dir in `config.php`

- Run to parse

    `php run.php -a`

**Command arguments:**

    -d : Download
    -u : Unzip
    -p : Parse
    -t : Truncate table
    -a : All proceeds