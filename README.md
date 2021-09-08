# imdb-parser
####Simple parser for IMDB datasets (https://datasets.imdbws.com/) to mysql database

1. Create mysql database.

2. Copy repository:

    `git clone https://github.com/sm9sh/imdb-parser`

3. Enter to dir:

    `cd imdb-parser`

4. Run to copy `config.php.example` to `config.php`:

    `php run.php`

5. Setup your database and dir in `config.php`

6. Run to parse

    `php run.php -a`

####Command arguments
    -d : Download
    -u : Unzip
    -p : Parse
    -t : Truncate table
    -a : All proceeds