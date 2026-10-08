<?php

require __DIR__ . '/bootstrap.php';

function importFiles($directory, $basics = null, $ratings = null) {
    foreach (['title.basics.tsv' => $basics, 'title.ratings.tsv' => $ratings] as $name => $contents) {
        $contents === null ? copy(__DIR__ . '/fixtures/' . $name, $directory . '/exchange/' . $name) : file_put_contents($directory . '/exchange/' . $name, $contents);
    }
}

runTests([
    'upsert, changed values, repeated ratings and unmatched count' => function () {
        $directory = testDirectory();
        $database = null;
        try {
            testConfig($directory, ['DOWNLOAD_DIR' => $directory . '/exchange/']);
            importFiles($directory);
            foreach ([1, 2] as $attempt) {
                $result = testCli($directory, '-p');
                expect($result['code'] === 0, $result['stderr'] . $result['stdout']);
                expect(strpos($result['stdout'], 'unmatched=1') !== false, 'Missing accurate unmatched count.');
            }
            $database = new PDO('sqlite:' . $directory . '/db/imdb.sqlite');
            expect((int) $database->query('SELECT COUNT(*) FROM title')->fetchColumn() === 3, 'Duplicate titles.');
            expect($database->query("SELECT primaryTitle FROM title WHERE tconst='tt0000002'")->fetchColumn() === 'Updated title', 'Last duplicate did not update title.');
            $changed = str_replace('Updated title', 'Changed title', file_get_contents(__DIR__ . '/fixtures/title.basics.tsv'));
            importFiles($directory, $changed, "numVotes\ttconst\taverageRating\n99\ttt0000001\t9.1\n");
            expect(testCli($directory, '-p')['code'] === 0, 'Changed import failed.');
            $row = $database->query("SELECT * FROM title WHERE tconst='tt0000001'")->fetch(PDO::FETCH_ASSOC);
            expect((float) $row['averageRating'] === 9.1 && (int) $row['numVotes'] === 99, 'Reordered rating headers mapped incorrectly.');
            expect($database->query("SELECT primaryTitle FROM title WHERE tconst='tt0000002'")->fetchColumn() === 'Changed title', 'Changed title did not update.');
        } finally {
            $database = null;
            removeTestDirectory($directory);
        }
    },
    'validate all headers before writing any title' => function () {
        $directory = testDirectory();
        $database = null;
        try {
            testConfig($directory, ['DOWNLOAD_DIR' => $directory . '/exchange/']);
            expect(testCli($directory, '-t')['code'] === 0, 'Could not initialize test DB.');
            importFiles($directory, null, "tconst\twrongRating\tnumVotes\n");
            expect(testCli($directory, '-p')['code'] !== 0, 'Bad header succeeded.');
            $database = new PDO('sqlite:' . $directory . '/db/imdb.sqlite');
            expect((int) $database->query('SELECT COUNT(*) FROM title')->fetchColumn() === 0, 'Bad ratings header allowed basics writes.');
        } finally {
            $database = null;
            removeTestDirectory($directory);
        }
    },
    'database failure rolls back current batch and keeps committed rows' => function () {
        $directory = testDirectory();
        $database = null;
        try {
            testConfig($directory, ['DOWNLOAD_DIR' => $directory . '/exchange/']);
            expect(testCli($directory, '-t')['code'] === 0, 'Could not initialize test DB.');
            $database = new PDO('sqlite:' . $directory . '/db/imdb.sqlite');
            $database->exec("CREATE TRIGGER fail_title BEFORE INSERT ON title WHEN NEW.tconst='tt4' BEGIN SELECT RAISE(ABORT, 'fixture failure'); END");
            $basics = "tconst\ttitleType\tprimaryTitle\toriginalTitle\tisAdult\tstartYear\tendYear\truntimeMinutes\tgenres\n";
            for ($i = 1; $i <= 4; $i++) {
                $basics .= "tt$i\tmovie\tTitle $i\tTitle $i\t0\t1894\t\\N\t1\tDrama\n";
            }
            importFiles($directory, $basics, "tconst\taverageRating\tnumVotes\n");
            $result = testCli($directory, '-p');
            expect($result['code'] !== 0, 'Database failure succeeded.');
            expect(strpos($result['stdout'] . $result['stderr'], 'committed=2') !== false, 'Failure does not identify committed batches.');
            expect((int) $database->query('SELECT COUNT(*) FROM title')->fetchColumn() === 2, 'Current batch partially committed.');
            $database->exec('DROP TRIGGER fail_title');
            expect(testCli($directory, '-p')['code'] === 0, 'Retry failed.');
            expect((int) $database->query('SELECT COUNT(*) FROM title')->fetchColumn() === 4, 'Retry did not finish.');
        } finally {
            $database = null;
            removeTestDirectory($directory);
        }
    },
    'reject invalid transaction portion' => function () {
        foreach ([0, -1, '2.5', 'invalid'] as $portion) {
            $directory = testDirectory();
            try {
                testConfig($directory, ['TRANSACTION_PORTION' => $portion, 'DOWNLOAD_DIR' => $directory . '/exchange/']);
                importFiles($directory);
                expect(testCli($directory, '-p')['code'] !== 0, 'Invalid portion succeeded.');
            } finally {
                removeTestDirectory($directory);
            }
        }
    },
    'incompatible schema fails without changing existing records' => function () {
        $directory = testDirectory();
        $database = null;
        try {
            testConfig($directory, ['DOWNLOAD_DIR' => $directory . '/exchange/']);
            mkdir($directory . '/db');
            $database = new PDO('sqlite:' . $directory . '/db/imdb.sqlite');
            $database->exec("CREATE TABLE title (tconst TEXT PRIMARY KEY, primaryTitle VARCHAR(255)); INSERT INTO title VALUES ('tt1', 'Saved')");
            importFiles($directory);
            expect(testCli($directory, '-p')['code'] !== 0, 'Incompatible schema accepted.');
            expect($database->query('SELECT primaryTitle FROM title')->fetchColumn() === 'Saved', 'Existing record changed.');
        } finally {
            $database = null;
            removeTestDirectory($directory);
        }
    },
]);
