<?php

require __DIR__ . '/bootstrap.php';
require dirname(__DIR__) . '/vendor/autoload.php';
require dirname(__DIR__) . '/lib/importer.php';

runTests(['rollback releases the transaction and preserves only committed batches' => function () {
    $directory = testDirectory();
    $conn = null;
    try {
        $path = $directory . '/title.basics.tsv';
        $contents = "tconst\ttitleType\tprimaryTitle\toriginalTitle\tisAdult\tstartYear\tendYear\truntimeMinutes\tgenres\n";
        for ($i = 1; $i <= 401; $i++) {
            $contents .= "tt$i\tmovie\tTitle $i\tTitle $i\t0\t1894\t\\N\t1\tDrama\n";
        }
        file_put_contents($path, $contents);
        $conn = \Doctrine\DBAL\DriverManager::getConnection(['driver' => 'pdo_sqlite', 'path' => $directory . '/rollback.sqlite']);
        ensureSchema($conn, true);
        $conn->executeStatement("CREATE TRIGGER fail_title BEFORE INSERT ON title WHEN NEW.tconst='tt350' BEGIN SELECT RAISE(ABORT, 'fixture failure'); END");
        $error = null;
        try {
            importDataset($conn, $path, 'title.basics.tsv', 200);
        } catch (Throwable $caught) {
            $error = $caught;
        }
        expect($error !== null && strpos($error->getMessage(), 'committed=200') !== false, 'Incorrect failure result.');
        expect(!$conn->isTransactionActive(), 'Failed import left transaction open.');
        expect((int) $conn->fetchColumn('SELECT COUNT(*) FROM title') === 200, 'Rollback retained uncommitted rows.');
        $conn->executeStatement('DROP TRIGGER fail_title');
        $stats = importDataset($conn, $path, 'title.basics.tsv', 200);
        expect($stats['processed'] === 401 && $stats['committed'] === 401, 'Retry counts incorrect.');
        expect((int) $conn->fetchColumn('SELECT COUNT(*) FROM title') === 401, 'Retry lost rows or created duplicates.');
    } finally {
        if ($conn !== null) {
            $conn->close();
        }
        removeTestDirectory($directory);
    }
}]);
