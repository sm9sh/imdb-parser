<?php
require __DIR__ . '/country-bootstrap.php';

runTests([
    'migration backs up titles and creates a reusable compatible cache' => function () {
        countryCase(function ($directory, $conn, $path, $url) {
            $conn->executeStatement("INSERT INTO title (tconst,titleType,primaryTitle,originalTitle,isAdult,startYear,averageRating,numVotes) VALUES ('tt1','movie','Saved','Saved',0,2000,7.5,42)");
            $backup = migrateCountrySchema($conn, $path);
            expect(is_file($backup), 'Migration did not create backup.');
            $pdo = new PDO('sqlite:' . $backup);
            expect($pdo->query("SELECT primaryTitle FROM title")->fetchColumn() === 'Saved', 'Backup lost title.');
            expect($pdo->query('PRAGMA quick_check')->fetchColumn() === 'ok', 'Backup corrupt.');
            $pdo = null;
            expect($conn->fetchColumn('SELECT numVotes FROM title') == 42, 'Migration changed ratings.');
            assertCountrySchema($conn);
            expect(migrateCountrySchema($conn, $path) === null, 'Repeated migration changed the database.');
        });
    },
    'country cache preserves multiple QIDs unknowns and absent or ambiguous ISO codes' => function () {
        countryCase(function ($directory, $conn, $path, $url) {
            migrateCountrySchema($conn, $path);
            $stats = syncCountryIds($conn, ['tt1', 'tt2', 'tt3', 'tt4', 'tt5', 'tt6'], fixtureCountrySettings($url));
            expect($stats['checked'] === 6 && $stats['known'] === 5 && $stats['unknown'] === 1, 'Country counts wrong.');
            expect($conn->fetchAllAssociative('SELECT country_qid,country_code FROM title_country WHERE tconst=? ORDER BY country_qid', ['tt2']) === [['country_qid' => 'Q30', 'country_code' => 'US'], ['country_qid' => 'Q408', 'country_code' => 'AU']], 'Co-production lost.');
            expect($conn->fetchColumn("SELECT country_qid FROM title_country WHERE tconst='tt3'") === 'Q15180', 'Historical country lost.');
            expect($conn->fetchColumn("SELECT country_code FROM title_country WHERE tconst='tt3'") === null, 'Invented ISO code.');
            expect($conn->fetchColumn("SELECT country_code FROM title_country WHERE tconst='tt6'") === null, 'Ambiguous code guessed.');
            expect($conn->fetchColumn("SELECT COUNT(*) FROM title_country WHERE tconst='tt4'") == 0 && $conn->fetchColumn("SELECT COUNT(*) FROM country_lookup WHERE tconst='tt4'") == 1, 'Unknown not cached.');
            syncCountryIds($conn, ['tt1', 'tt2', 'tt3', 'tt4', 'tt5', 'tt6'], fixtureCountrySettings($url));
            expect((int) file_get_contents($directory . '/country-count') === 1, 'Fresh cache requested again.');
        });
    },
    'Wikidata SomeValue preserves known QIDs and caches unknown countries' => function () {
        foreach (['somevalue', 'bnode'] as $mode) {
            countryCase(function ($directory, $conn, $path, $url) {
                migrateCountrySchema($conn, $path);
                $stats = syncCountryIds($conn, ['tt1', 'tt4'], fixtureCountrySettings($url));
                expect($stats === ['checked' => 2, 'known' => 1, 'unknown' => 1], 'SomeValue counts wrong.');
                expect($conn->fetchAllAssociative("SELECT country_qid,country_code FROM title_country WHERE tconst='tt1'") === [['country_qid' => 'Q668', 'country_code' => null]], 'Unknown ISO lost India QID.');
                expect($conn->fetchColumn("SELECT COUNT(*) FROM country_lookup WHERE tconst='tt4'") == 1 && $conn->fetchColumn("SELECT COUNT(*) FROM title_country WHERE tconst='tt4'") == 0, 'Unknown country not cached.');
            }, $mode);
        }
    },
    'expired cache replaces countries atomically' => function () {
        countryCase(function ($directory, $conn, $path, $url) {
            migrateCountrySchema($conn, $path);
            syncCountryIds($conn, ['tt2'], fixtureCountrySettings($url));
            $conn->executeStatement("UPDATE country_lookup SET checked_at='2000-01-01 00:00:00'");
            syncCountryIds($conn, ['tt2'], fixtureCountrySettings(str_replace('/ok', '/changed', $url)));
            expect($conn->fetchAllAssociative("SELECT country_qid,country_code FROM title_country WHERE tconst='tt2'") === [['country_qid' => 'Q668', 'country_code' => 'IN']], 'Old countries survived refresh.');
        });
    },
    'duplicate response rows do not duplicate countries' => function () {
        countryCase(function ($directory, $conn, $path, $url) {
            migrateCountrySchema($conn, $path);
            syncCountryIds($conn, ['tt1'], fixtureCountrySettings($url));
            expect($conn->fetchColumn('SELECT COUNT(*) FROM title_country') == 1, 'Duplicate countries saved.');
        }, 'duplicate');
    },
    'invalid or incomplete responses preserve previous cache' => function () {
        foreach (['json', 'partial', 'foreign', 'code', 'uri', 'oversized'] as $mode) {
            countryCase(function ($directory, $conn, $path, $url) use ($mode) {
                migrateCountrySchema($conn, $path);
                $conn->executeStatement("INSERT INTO title_country VALUES ('tt1','Q30','US')");
                $conn->executeStatement("INSERT INTO country_lookup VALUES ('tt1','2000-01-01 00:00:00')");
                $failed = false;
                try { syncCountryIds($conn, ['tt1'], fixtureCountrySettings($url)); } catch (Throwable $error) { $failed = true; }
                expect($failed, $mode . ' response accepted.');
                expect($conn->fetchColumn("SELECT country_code FROM title_country WHERE tconst='tt1'") === 'US', 'Previous cache overwritten.');
                expect($conn->fetchColumn("SELECT checked_at FROM country_lookup WHERE tconst='tt1'") === '2000-01-01 00:00:00', 'Failed response marked fresh.');
            }, $mode);
        }
    },
    'rate limit retries a bounded request' => function () {
        countryCase(function ($directory, $conn, $path, $url) {
            migrateCountrySchema($conn, $path);
            syncCountryIds($conn, ['tt1'], fixtureCountrySettings($url, ['DOWNLOAD_RETRIES' => 2]));
            expect((int) file_get_contents($directory . '/country-count') === 2, '429 retry did not follow limit.');
            expect($conn->fetchColumn('SELECT country_code FROM title_country') === 'IN', 'Retry lost result.');
        }, 'retry');
    },
    'timeout and permanent HTTP failure never become unknown countries' => function () {
        foreach (['slow', '404'] as $mode) {
            countryCase(function ($directory, $conn, $path, $url) use ($mode) {
                migrateCountrySchema($conn, $path);
                $failed = false;
                try { syncCountryIds($conn, ['tt1'], fixtureCountrySettings($url, ['DOWNLOAD_RETRIES' => $mode === '404' ? 2 : 0])); } catch (Throwable $error) { $failed = true; }
                expect($failed, 'HTTP failure accepted.');
                expect($conn->fetchColumn('SELECT COUNT(*) FROM country_lookup') == 0, 'HTTP failure cached as unknown.');
                expect((int) file_get_contents($directory . '/country-count') === 1, 'Permanent failure retried.');
            }, $mode);
        }
    },
    'cache write failure rolls back all country replacements' => function () {
        countryCase(function ($directory, $conn, $path, $url) {
            migrateCountrySchema($conn, $path);
            syncCountryIds($conn, ['tt1', 'tt2'], fixtureCountrySettings($url));
            $before = $conn->fetchAllAssociative('SELECT * FROM title_country ORDER BY tconst,country_qid');
            $conn->executeStatement("UPDATE country_lookup SET checked_at='2000-01-01 00:00:00'");
            $conn->executeStatement("CREATE TRIGGER fail_country BEFORE INSERT ON title_country WHEN NEW.tconst='tt2' BEGIN SELECT RAISE(ABORT,'fixture failure'); END");
            $failed = false;
            try { syncCountryIds($conn, ['tt1', 'tt2'], fixtureCountrySettings($url)); } catch (Throwable $error) { $failed = true; }
            expect($failed && !$conn->isTransactionActive(), 'Cache failure left transaction open.');
            expect($conn->fetchAllAssociative('SELECT * FROM title_country ORDER BY tconst,country_qid') === $before, 'Partial country replacement committed.');
            expect($conn->fetchColumn("SELECT COUNT(*) FROM country_lookup WHERE checked_at='2000-01-01 00:00:00'") == 2, 'Failed batch updated lookup timestamps.');
        });
    },
    'incompatible country schema fails without migration writes' => function () {
        countryCase(function ($directory, $conn, $path, $url) {
            $conn->executeStatement('CREATE TABLE title_country (tconst TEXT PRIMARY KEY,country_qid TEXT,country_code TEXT)');
            $failed = false;
            try { migrateCountrySchema($conn, $path); } catch (RuntimeException $error) { $failed = strpos($error->getMessage(), 'schema') !== false; }
            expect($failed, 'Partial incompatible schema migrated silently.');
            expect($conn->fetchColumn("SELECT COUNT(*) FROM sqlite_master WHERE name='country_lookup'") == 0, 'Partial schema modified.');
        });
    },
]);
