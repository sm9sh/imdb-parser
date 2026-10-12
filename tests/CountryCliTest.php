<?php
require __DIR__ . '/country-bootstrap.php';

function countryCliConfig($directory, $url, $extra = []) {
    testConfig($directory, array_replace(['WIKIDATA_COUNTRIES_ENABLED' => true, 'WIKIDATA_ENDPOINT' => $url,
        'IMPORT_COMPLETED_ONLY' => true, 'CONNECT_TIMEOUT' => 1, 'TRANSFER_TIMEOUT' => 1, 'DOWNLOAD_RETRIES' => 0], $extra));
}

function countryCliFiles($directory) {
    $future = (int) date('Y') + 1;
    file_put_contents($directory . '/exchange/title.basics.tsv',
        "tconst\ttitleType\tprimaryTitle\toriginalTitle\tisAdult\tstartYear\tendYear\truntimeMinutes\tgenres\n" .
        "tt1\tmovie\tIndia\tIndia\t0\t2000\t\\N\t90\tDrama\n" .
        "tt2\tmovie\tAllowed\tAllowed\t0\t2000\t\\N\t90\tDrama\n" .
        "tt3\ttvSeries\tEnded\tEnded\t0\t2000\t2010\t90\tDrama\n" .
        "tt4\ttvMiniSeries\tUnknown\tUnknown\t0\t2000\t2010\t90\tDrama\n" .
        "tt5\tmovie\tCo-production\tCo-production\t0\t2000\t\\N\t90\tDrama\n" .
        "tt6\tmovie\tFuture\tFuture\t0\t$future\t\\N\t90\tDrama\n" .
        "tt7\ttvSeries\tOngoing\tOngoing\t0\t2000\t\\N\t90\tDrama\n" .
        "tt2\tmovie\tUpdated\tUpdated\t0\t2000\t\\N\t90\tDrama\n");
    $ratings = "tconst\taverageRating\tnumVotes\n";
    for ($id = 1; $id <= 8; $id++) { $ratings .= "tt$id\t8.5\t100\n"; }
    file_put_contents($directory . '/exchange/title.ratings.tsv', $ratings);
}

runTests([
    'CLI migration creates a verified backup without changing titles' => function () {
        countryCase(function ($directory, $conn, $path, $url) {
            countryCliConfig($directory, $url);
            $conn->executeStatement("INSERT INTO title (tconst,titleType,primaryTitle,originalTitle,isAdult,startYear) VALUES ('tt99','movie','Saved','Saved',0,2000)");
            $result = testCli($directory, '--migrate-countries');
            expect($result['code'] === 0, $result['stderr']);
            assertCountrySchema($conn);
            expect($conn->fetchColumn('SELECT primaryTitle FROM title') === 'Saved', 'Migration changed titles.');
            expect(count(glob($directory . '/db/imdb-before-countries-*.sqlite')) === 1, 'CLI backup missing.');
            expect(testCli($directory, '--migrate-countries')['code'] === 0, 'Repeated migration failed.');
            expect(count(glob($directory . '/db/imdb-before-countries-*.sqlite')) === 1, 'Repeated migration created backup.');
        });
    },
    'import filters India and co-production while retaining unknown completed titles' => function () {
        countryCase(function ($directory, $conn, $path, $url) {
            migrateCountrySchema($conn, $path);
            countryCliConfig($directory, $url);
            countryCliFiles($directory);
            for ($attempt = 0; $attempt < 2; $attempt++) {
                $result = testCli($directory, '-p');
                expect($result['code'] === 0, $result['stderr']);
                expect($conn->fetchFirstColumn('SELECT tconst FROM title ORDER BY tconst') === ['tt2', 'tt3', 'tt4'], 'Country/completion filter failed.');
                expect($conn->fetchColumn("SELECT primaryTitle FROM title WHERE tconst='tt2'") === 'Updated', 'Selected upsert failed.');
                expect($conn->fetchColumn('SELECT COUNT(*) FROM title WHERE averageRating=8.5 AND numVotes=100') == 3, 'Selected ratings failed.');
                expect(strpos($result['stdout'], 'country_skipped=2') !== false && strpos($result['stdout'], 'country_unknown=1') !== false, 'Country counters missing.');
                expect(strpos($result['stdout'], 'unmatched=5') !== false, 'Filtered ratings count wrong.');
            }
            expect((int) file_get_contents($directory . '/country-count') === 1, 'Repeated import fetched fresh cache.');
            expect($conn->fetchColumn("SELECT COUNT(*) FROM country_lookup WHERE tconst IN ('tt6','tt7')") == 0, 'Excluded completion titles fetched.');
        });
    },
    'existing India without ISO code keeps all fields and ratings unchanged' => function () {
        countryCase(function ($directory, $conn, $path, $url) {
            migrateCountrySchema($conn, $path);
            syncCountryIds($conn, ['tt1','tt2','tt3','tt4','tt5'], fixtureCountrySettings($url));
            $conn->executeStatement("UPDATE title_country SET country_code=NULL WHERE country_qid='Q668'");
            $conn->executeStatement("INSERT INTO title (tconst,titleType,primaryTitle,originalTitle,isAdult,startYear,averageRating,numVotes,updated) VALUES ('tt1','movie','Saved','Saved',0,2000,1.0,1,'2000-01-01 00:00:00')");
            $before = $conn->fetchAssociative("SELECT * FROM title WHERE tconst='tt1'");
            countryCliConfig($directory, $url);
            countryCliFiles($directory);
            $result = testCli($directory, '-p');
            expect($result['code'] === 0, $result['stderr']);
            expect($conn->fetchAssociative("SELECT * FROM title WHERE tconst='tt1'") === $before, 'Existing excluded title changed.');
            expect($conn->fetchColumn('SELECT COUNT(*) FROM title') == 4, 'Existing title was deleted.');
        });
    },
    'country failure before clearing preserves title and committed cache batches' => function () {
        countryCase(function ($directory, $conn, $path, $url) {
            migrateCountrySchema($conn, $path);
            countryCliConfig($directory, $url);
            $conn->executeStatement("INSERT INTO title (tconst,titleType,primaryTitle,originalTitle,isAdult,startYear) VALUES ('tt99999','movie','Saved','Saved',0,2000)");
            $basics = "tconst\ttitleType\tprimaryTitle\toriginalTitle\tisAdult\tstartYear\tendYear\truntimeMinutes\tgenres\n";
            for ($id = 1; $id <= 102; $id++) { $basics .= "tt$id\tmovie\tTitle\tTitle\t0\t2000\t\\N\t90\tDrama\n"; }
            file_put_contents($directory . '/exchange/title.basics.tsv', $basics);
            file_put_contents($directory . '/exchange/title.ratings.tsv', "tconst\taverageRating\tnumVotes\n");
            $result = testCli($directory, '-t', '-p');
            expect($result['code'] !== 0, 'Failed country preflight succeeded.');
            expect($conn->fetchColumn('SELECT primaryTitle FROM title') === 'Saved' && $conn->fetchColumn('SELECT COUNT(*) FROM title') == 1, 'Preflight cleared title.');
            expect($conn->fetchColumn('SELECT COUNT(*) FROM country_lookup') == 100, 'Committed cache batch lost or failed batch cached.');
        }, 'fail-second');
    },
    'missing migration fails before clearing or HTTP access' => function () {
        countryCase(function ($directory, $conn, $path, $url) {
            countryCliConfig($directory, $url);
            countryCliFiles($directory);
            $conn->executeStatement("INSERT INTO title (tconst,titleType,primaryTitle,originalTitle,isAdult,startYear) VALUES ('tt99','movie','Saved','Saved',0,2000)");
            $result = testCli($directory, '-t', '-p');
            expect($result['code'] !== 0 && strpos($result['stderr'], '--migrate-countries') !== false, 'Missing country schema accepted.');
            expect($conn->fetchColumn('SELECT primaryTitle FROM title') === 'Saved', 'Missing schema cleared title.');
            expect(!is_file($directory . '/country-count'), 'HTTP ran before schema check.');
        });
    },
    'stored country command limits fresh lookups and resumes remaining IDs' => function () {
        countryCase(function ($directory, $conn, $path, $url) {
            migrateCountrySchema($conn, $path);
            countryCliConfig($directory, $url);
            for ($id = 1; $id <= 4; $id++) { $conn->executeStatement("INSERT INTO title (tconst,titleType,primaryTitle,originalTitle,isAdult,startYear) VALUES (?,'movie','Saved','Saved',0,2000)", ['tt' . $id]); }
            for ($attempt = 1; $attempt <= 2; $attempt++) {
                $result = testCli($directory, '--countries', '--country-limit=2');
                expect($result['code'] === 0, $result['stderr']);
                expect($conn->fetchColumn('SELECT COUNT(*) FROM country_lookup') == 2 * $attempt, 'Limit did not resume missing IDs.');
            }
            expect(testCli($directory, '--countries', '--country-limit=2')['code'] === 0, 'Fresh cache command failed.');
            expect((int) file_get_contents($directory . '/country-count') === 2, 'Fresh stored countries fetched again.');
            expect($conn->fetchColumn('SELECT COUNT(*) FROM title') == 4, 'Country sync deleted existing India.');
        });
    },
    'invalid country configuration fails before title clearing' => function () {
        foreach ([['WIKIDATA_COUNTRIES_ENABLED' => 'yes'], ['EXCLUDE_COUNTRIES' => ['in']], ['WIKIDATA_REFRESH_DAYS' => 0], ['WIKIDATA_ENDPOINT' => 'http://secret:pass@127.0.0.1/sparql']] as $settings) {
            countryCase(function ($directory, $conn, $path, $url) use ($settings) {
                migrateCountrySchema($conn, $path);
                countryCliConfig($directory, $url, $settings);
                countryCliFiles($directory);
                $conn->executeStatement("INSERT INTO title (tconst,titleType,primaryTitle,originalTitle,isAdult,startYear) VALUES ('tt99','movie','Saved','Saved',0,2000)");
                $result = testCli($directory, '-t', '-p');
                expect($result['code'] !== 0, 'Invalid country settings accepted.');
                expect($conn->fetchColumn('SELECT primaryTitle FROM title') === 'Saved', 'Invalid settings cleared title.');
                expect(!is_file($directory . '/country-count') && strpos($result['stderr'], 'secret') === false, 'Invalid settings reached HTTP or leaked credentials.');
            });
        }
    },
    'country-skipped rows preserve committed transaction boundaries on retry' => function () {
        countryCase(function ($directory, $conn, $path, $url) {
            migrateCountrySchema($conn, $path);
            countryCliConfig($directory, $url, ['TRANSACTION_PORTION' => 200]);
            $conn->executeStatement("CREATE TRIGGER fail_title BEFORE INSERT ON title WHEN NEW.tconst='tt350' BEGIN SELECT RAISE(ABORT,'fixture failure'); END");
            $basics = "tconst\ttitleType\tprimaryTitle\toriginalTitle\tisAdult\tstartYear\tendYear\truntimeMinutes\tgenres\n";
            for ($id = 1; $id <= 401; $id++) { $basics .= "tt$id\tmovie\tTitle\tTitle\t0\t2000\t\\N\t90\tDrama\n"; }
            file_put_contents($directory . '/exchange/title.basics.tsv', $basics);
            file_put_contents($directory . '/exchange/title.ratings.tsv', "tconst\taverageRating\tnumVotes\n");
            $result = testCli($directory, '-p');
            expect($result['code'] !== 0 && strpos($result['stderr'], 'committed=200 rolled_back=100') !== false, 'Country rows changed rollback boundaries.');
            expect($conn->fetchColumn('SELECT COUNT(*) FROM title') == 200, 'Current transaction was partially committed.');
            expect($conn->fetchColumn("SELECT COUNT(*) FROM title WHERE tconst IN ('tt1','tt5')") == 0, 'Excluded titles committed.');
            $conn->executeStatement('DROP TRIGGER fail_title');
            expect(testCli($directory, '-p')['code'] === 0, 'Country import retry failed.');
            expect($conn->fetchColumn('SELECT COUNT(*) FROM title') == 399, 'Retry lost or duplicated titles.');
        });
    },
    'new CLI arguments reject invalid limits and mixed destructive actions before side effects' => function () {
        $directory = testDirectory();
        try {
            foreach ([['--country-limit=2'], ['--countries','--country-limit=0'], ['--countries','--country-limit=1.5'], ['--migrate-countries','-t'], ['--countries','-p']] as $args) {
                expect(testCli($directory, ...$args)['code'] !== 0, 'Invalid country invocation accepted.');
                expect(!is_dir($directory . '/db') && !is_file($directory . '/.imdb-parser.lock'), 'Invalid invocation caused side effects.');
            }
            $result = testCli($directory, '--help');
            expect($result['code'] === 0 && strpos($result['stdout'], '--countries') !== false, 'Country help missing.');
        } finally { removeTestDirectory($directory); }
    },
    'country mode rejects legacy MySQL before connecting' => function () {
        $directory = testDirectory();
        try {
            testConfig($directory, ['WIKIDATA_COUNTRIES_ENABLED' => true, 'DATABASE_URL' => 'mysql://secret:pass@127.0.0.1:1/imdb', 'WIKIDATA_ENDPOINT' => 'http://127.0.0.1/sparql']);
            foreach (['title.basics.tsv','title.ratings.tsv'] as $name) { copy(__DIR__ . '/fixtures/' . $name, $directory . '/exchange/' . $name); }
            $result = testCli($directory, '-p');
            expect($result['code'] !== 0 && strpos($result['stderr'], 'SQLite') !== false && strpos($result['stderr'], 'secret') === false, 'Legacy country mode not rejected safely.');
        } finally { removeTestDirectory($directory); }
    },
]);
