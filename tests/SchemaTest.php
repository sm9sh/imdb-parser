<?php

require __DIR__ . '/bootstrap.php';
require dirname(__DIR__) . '/vendor/autoload.php';
require dirname(__DIR__) . '/lib/importer.php';

runTests(['unrated basics are NULL with existing zero defaults; existing ratings survive upsert' => function () {
    $conn = \Doctrine\DBAL\DriverManager::getConnection(['driver' => 'pdo_sqlite', 'memory' => true]);
    try {
        $conn->executeStatement('CREATE TABLE title (
            tconst TEXT NOT NULL PRIMARY KEY, titleType TEXT NOT NULL,
            primaryTitle TEXT NOT NULL, originalTitle TEXT NOT NULL, isAdult INTEGER NOT NULL,
            startYear INTEGER DEFAULT NULL, endYear INTEGER DEFAULT NULL,
            runtimeMinutes INTEGER DEFAULT NULL, genres TEXT DEFAULT NULL,
            averageRating REAL DEFAULT 0, numVotes INTEGER DEFAULT 0,
            updated DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        )');
        $conn->executeStatement("INSERT INTO title(tconst,titleType,primaryTitle,originalTitle,isAdult,averageRating,numVotes) VALUES('tt0000001','short','Saved','Saved',0,9.1,77)");
        ensureSchema($conn, true);
        importDataset($conn, __DIR__ . '/fixtures/title.basics.tsv', 'title.basics.tsv', 2000);
        $new = $conn->fetchAssociative("SELECT averageRating,numVotes FROM title WHERE tconst='tt0000002'");
        expect($new['averageRating'] === null && $new['numVotes'] === null, 'Unrated title inherited fake zero ratings.');
        $saved = $conn->fetchAssociative("SELECT averageRating,numVotes FROM title WHERE tconst='tt0000001'");
        expect((float) $saved['averageRating'] === 9.1 && (int) $saved['numVotes'] === 77, 'Basics overwrote an existing rating.');
    } finally {
        $conn->close();
    }
},

    'composite primary key fails before explicit clearing or schema writes' => function () {
        $directory = testDirectory();
        $database = null;
        try {
            testConfig($directory);
            mkdir($directory . '/db');
            $database = new PDO('sqlite:' . $directory . '/db/imdb.sqlite');
            $database->exec('CREATE TABLE title (
                tconst TEXT NOT NULL, titleType TEXT NOT NULL,
                primaryTitle TEXT NOT NULL, originalTitle TEXT NOT NULL, isAdult INTEGER NOT NULL,
                startYear INTEGER DEFAULT NULL, endYear INTEGER DEFAULT NULL,
                runtimeMinutes INTEGER DEFAULT NULL, genres TEXT DEFAULT NULL,
                averageRating REAL DEFAULT NULL, numVotes INTEGER DEFAULT NULL,
                updated DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(tconst,titleType)
            )');
            $database->exec("INSERT INTO title(tconst,titleType,primaryTitle,originalTitle,isAdult) VALUES('tt1','movie','Saved','Saved',0)");
            foreach (['title.basics.tsv', 'title.ratings.tsv'] as $name) {
                copy(__DIR__ . '/fixtures/' . $name, $directory . '/exchange/' . $name);
            }
            expect(testCli($directory, '-t', '-p')['code'] !== 0, 'Incompatible schema accepted.');
            expect($database->query('SELECT primaryTitle FROM title')->fetchColumn() === 'Saved', 'Preflight cleared existing data.');
            expect((int) $database->query("SELECT COUNT(*) FROM sqlite_master WHERE name IN ('title_averageRating','title_endYear','title_numVotes')")->fetchColumn() === 0, 'Preflight created indexes on incompatible schema.');
        } finally {
            $database = null;
            removeTestDirectory($directory);
        }
    },

]);
