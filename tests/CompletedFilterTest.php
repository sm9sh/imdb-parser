<?php

require __DIR__ . '/bootstrap.php';

function completedFilterFiles($directory) {
    $year = (int) date('Y');
    $rows = [
        ['movie', 1894, null],
        ['movie', $year, null],
        ['movie', $year + 1, null],
        ['movie', null, null],
        ['tvSeries', 2000, $year - 1],
        ['tvMiniSeries', 2000, $year],
        ['tvSeries', 2000, null],
        ['tvMiniSeries', 2000, $year + 1],
        ['short', 2000, null],
        ['tvMovie', 2000, null],
        ['tvEpisode', 2000, $year - 1],
    ];
    $basics = "tconst\ttitleType\tprimaryTitle\toriginalTitle\tisAdult\tstartYear\tendYear\truntimeMinutes\tgenres\n";
    $ratings = "tconst\taverageRating\tnumVotes\n";
    foreach ($rows as $index => [$type, $start, $end]) {
        $id = $index + 1;
        $start = $start ?? '\N';
        $end = $end ?? '\N';
        $basics .= "tt$id\t$type\tTitle $id\tTitle $id\t0\t$start\t$end\t90\tDrama\n";
        $ratings .= "tt$id\t8.5\t100\n";
    }
    $basics .= "tt1\tmovie\tUpdated title\tUpdated title\t0\t1894\t\\N\t90\tDrama\n";
    $ratings .= "tt99\t7.0\t10\n";
    file_put_contents($directory . '/exchange/title.basics.tsv', $basics);
    file_put_contents($directory . '/exchange/title.ratings.tsv', $ratings);
}

runTests([
    'default and enabled filter select released movies and ended series on repeat' => function () {
        foreach (['default', 'enabled'] as $mode) {
            $directory = testDirectory();
            $database = null;
            try {
                testConfig($directory, ['IMPORT_COMPLETED_ONLY' => true]);
                if ($mode === 'default') {
                    $config = require $directory . '/config.php';
                    unset($config['IMPORT_COMPLETED_ONLY']);
                    file_put_contents($directory . '/config.php', '<?php return ' . var_export($config, true) . ';');
                }
                completedFilterFiles($directory);
                for ($attempt = 0; $attempt < 2; $attempt++) {
                    $result = testCli($directory, '-p');
                    expect($result['code'] === 0, $result['stderr'] . $result['stdout']);
                    $database = new PDO('sqlite:' . $directory . '/db/imdb.sqlite');
                    expect($database->query('SELECT tconst FROM title ORDER BY tconst')->fetchAll(PDO::FETCH_COLUMN) === ['tt1', 'tt2', 'tt5', 'tt6'], 'Filter included unfinished or other title types.');
                    expect((int) $database->query('SELECT COUNT(*) FROM title WHERE averageRating=8.5 AND numVotes=100')->fetchColumn() === 4, 'Selected ratings were not updated.');
                    expect($database->query("SELECT primaryTitle FROM title WHERE tconst='tt1'")->fetchColumn() === 'Updated title', 'Selected duplicate did not update.');
                    expect(strpos($result['stdout'], 'title.basics.tsv: processed=5 committed=5 skipped=7') !== false, 'Incorrect selected/skipped basics counts.');
                    expect(strpos($result['stdout'], 'unmatched=8') !== false, 'Ratings without eligible titles were not counted.');
                }
            } finally {
                $database = null;
                removeTestDirectory($directory);
            }
        }
    },
    'filter preserves excluded existing titles and their ratings' => function () {
        $directory = testDirectory();
        $database = null;
        try {
            testConfig($directory, ['IMPORT_COMPLETED_ONLY' => false]);
            completedFilterFiles($directory);
            expect(testCli($directory, '-p')['code'] === 0, 'Initial full import failed.');
            $database = new PDO('sqlite:' . $directory . '/db/imdb.sqlite');
            $database->exec("UPDATE title SET primaryTitle='Saved', averageRating=1.0, numVotes=1, updated='2000-01-01 00:00:00'");
            testConfig($directory, ['IMPORT_COMPLETED_ONLY' => true]);
            $result = testCli($directory, '-p');
            expect($result['code'] === 0, $result['stderr']);
            expect((int) $database->query('SELECT COUNT(*) FROM title')->fetchColumn() === 11, 'Import deleted existing data.');
            expect((int) $database->query("SELECT COUNT(*) FROM title WHERE primaryTitle='Saved' AND averageRating=1.0 AND numVotes=1 AND updated='2000-01-01 00:00:00'")->fetchColumn() === 7, 'Excluded basics or ratings changed.');
            expect(strpos($result['stdout'], 'unmatched=8') !== false, 'Existing excluded ratings were not counted.');
        } finally {
            $database = null;
            removeTestDirectory($directory);
        }
    },
    'all excluded rows leave an empty database' => function () {
        $directory = testDirectory();
        $database = null;
        try {
            testConfig($directory, ['IMPORT_COMPLETED_ONLY' => true]);
            file_put_contents($directory . '/exchange/title.basics.tsv', "tconst\ttitleType\tprimaryTitle\toriginalTitle\tisAdult\tstartYear\tendYear\truntimeMinutes\tgenres\ntt1\ttvSeries\tOngoing\tOngoing\t0\t2000\t\\N\t90\tDrama\n");
            file_put_contents($directory . '/exchange/title.ratings.tsv', "tconst\taverageRating\tnumVotes\ntt1\t8.5\t100\n");
            $result = testCli($directory, '-p');
            expect($result['code'] === 0, $result['stderr']);
            $database = new PDO('sqlite:' . $directory . '/db/imdb.sqlite');
            expect((int) $database->query('SELECT COUNT(*) FROM title')->fetchColumn() === 0, 'Excluded titles were created.');
            expect(strpos($result['stdout'], 'processed=0 committed=0 skipped=1') !== false, 'Empty selection counts incorrect.');
        } finally {
            $database = null;
            removeTestDirectory($directory);
        }
    },
    'skipped rows do not change transaction boundaries or rollback' => function () {
        $directory = testDirectory();
        $database = null;
        try {
            testConfig($directory, ['IMPORT_COMPLETED_ONLY' => true, 'TRANSACTION_PORTION' => 200]);
            expect(testCli($directory, '-t')['code'] === 0, 'Could not initialize test database.');
            $database = new PDO('sqlite:' . $directory . '/db/imdb.sqlite');
            $database->exec("CREATE TRIGGER fail_title BEFORE INSERT ON title WHEN NEW.tconst='tt350' BEGIN SELECT RAISE(ABORT, 'fixture failure'); END");
            $basics = "tconst\ttitleType\tprimaryTitle\toriginalTitle\tisAdult\tstartYear\tendYear\truntimeMinutes\tgenres\n";
            for ($id = 1; $id <= 401; $id++) {
                $basics .= "tt$id\tmovie\tTitle $id\tTitle $id\t0\t1894\t\\N\t90\tDrama\n";
                $excluded = 1000 + $id;
                $basics .= "tt$excluded\ttvSeries\tOngoing\tOngoing\t0\t2000\t\\N\t90\tDrama\n";
            }
            file_put_contents($directory . '/exchange/title.basics.tsv', $basics);
            file_put_contents($directory . '/exchange/title.ratings.tsv', "tconst\taverageRating\tnumVotes\n");
            $result = testCli($directory, '-p');
            expect($result['code'] !== 0 && strpos($result['stderr'], 'committed=200 rolled_back=100') !== false, 'Skipped rows changed rollback counts.');
            expect((int) $database->query('SELECT COUNT(*) FROM title')->fetchColumn() === 200, 'Rollback lost committed titles or saved partial writes.');
            $database->exec('DROP TRIGGER fail_title');
            $result = testCli($directory, '-p');
            expect($result['code'] === 0, $result['stderr']);
            expect((int) $database->query('SELECT COUNT(*) FROM title')->fetchColumn() === 401, 'Retry imported excluded rows or lost selected rows.');
            expect(strpos($result['stdout'], 'processed=401 committed=401 skipped=401') !== false, 'Retry selection counts incorrect.');
        } finally {
            $database = null;
            removeTestDirectory($directory);
        }
    },
    'invalid filter settings fail before explicit clearing' => function () {
        foreach (['false', 0, 1, null] as $setting) {
            $directory = testDirectory();
            $database = null;
            try {
                testConfig($directory, ['IMPORT_COMPLETED_ONLY' => false]);
                completedFilterFiles($directory);
                expect(testCli($directory, '-p')['code'] === 0, 'Initial full import failed.');
                testConfig($directory, ['IMPORT_COMPLETED_ONLY' => $setting]);
                $result = testCli($directory, '-t', '-p');
                expect($result['code'] !== 0 && strpos($result['stderr'], 'IMPORT_COMPLETED_ONLY must be a boolean') !== false, 'Invalid filter setting accepted.');
                $database = new PDO('sqlite:' . $directory . '/db/imdb.sqlite');
                expect((int) $database->query('SELECT COUNT(*) FROM title')->fetchColumn() === 11, 'Invalid filter setting cleared data.');
            } finally {
                $database = null;
                removeTestDirectory($directory);
            }
        }
    },
]);
