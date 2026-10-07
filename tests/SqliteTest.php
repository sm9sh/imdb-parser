<?php

function expectSqlite($condition, $message) {
    if (!$condition) {
        throw new RuntimeException($message);
    }
}

function runSqliteCli($directory, $argument) {
    $command = escapeshellarg(PHP_BINARY) . ' -d error_reporting=' . E_ALL . ' -d display_errors=1 -d log_errors=0 ' . escapeshellarg($directory . '/run.php') . ' ' . escapeshellarg($argument);
    $process = proc_open($command, [1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes, $directory);
    expectSqlite(is_resource($process), 'Could not start the importer.');
    $output = stream_get_contents($pipes[1]) . stream_get_contents($pipes[2]);
    fclose($pipes[1]);
    fclose($pipes[2]);
    expectSqlite(proc_close($process) === 0, 'SQLite CLI import failed: ' . $output);
    expectSqlite(strpos($output, 'Deprecated:') === false, 'Importer emits PHP deprecation notices: ' . $output);
}

function removeSqliteTestDirectory($directory) {
    foreach (scandir($directory) as $name) {
        if ($name === '.' || $name === '..') {
            continue;
        }
        $path = $directory . '/' . $name;
        if (is_dir($path) && !is_link($path)) {
            removeSqliteTestDirectory($path);
        }
        else {
            unlink($path);
        }
    }
    rmdir($directory);
}

$root = dirname(__DIR__);
$directory = __DIR__ . '/.sqlite test-' . bin2hex(random_bytes(8));
$database = null;
$exit_code = 0;
try {
    mkdir($directory . '/vendor', 0777, true);
    mkdir($directory . '/exchange');
    copy($root . '/run.php', $directory . '/run.php');
    file_put_contents($directory . '/vendor/autoload.php', '<?php require ' . var_export($root . '/vendor/autoload.php', true) . ';');
    $config = [
        'DATABASE_URL' => 'sqlite:///' . str_replace('\\', '/', $directory) . '/db/imdb.sqlite',
        'TRANSACTION_PORTION' => 2,
        'DOWNLOAD_DIR' => $directory . '/exchange/',
    ];
    file_put_contents($directory . '/config.php', '<?php return ' . var_export($config, true) . ';');
    $long_title = str_repeat('x', 300);
    file_put_contents($directory . '/exchange/title.basics.tsv',
        "tconst\ttitleType\tprimaryTitle\toriginalTitle\tisAdult\tstartYear\tendYear\truntimeMinutes\tgenres\n" .
        "tt0000001\tshort\tSilent film\tSilent film\t0\t1894\t\\N\t1\tDocumentary\n" .
        "tt0000002\tmovie\tТестове кіно\tТестове кіно\t0\t2019\t\\N\t\\N\t\\N\n" .
        "tt0000003\tmovie\t{$long_title}\t{$long_title}\t0\t2020\t\\N\t100\tDrama\n"
    );
    file_put_contents($directory . '/exchange/title.ratings.tsv',
        "tconst\taverageRating\tnumVotes\n" .
        "tt0000001\t7.5\t12\n" .
        "tt0000002\t8.0\t24\n"
    );

    runSqliteCli($directory, '-p');
    expectSqlite(is_file($directory . '/db/imdb.sqlite'), 'The database file was not created.');
    $database = new PDO('sqlite:' . $directory . '/db/imdb.sqlite');
    $database->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
    $rows = $database->query('SELECT * FROM title ORDER BY tconst')->fetchAll(PDO::FETCH_ASSOC);
    expectSqlite(count($rows) === 3, 'Expected three imported titles.');
    expectSqlite((int) $rows[0]['startYear'] === 1894, 'The historical year was not preserved.');
    expectSqlite($rows[0]['endYear'] === null, 'Missing year must be SQL NULL.');
    expectSqlite((float) $rows[0]['averageRating'] === 7.5 && (int) $rows[0]['numVotes'] === 12, 'Ratings were not imported.');
    expectSqlite($rows[1]['primaryTitle'] === 'Тестове кіно', 'Unicode title was not preserved.');
    expectSqlite($rows[1]['runtimeMinutes'] === null && $rows[1]['genres'] === null, 'Missing fields must be SQL NULL.');
    expectSqlite($rows[2]['primaryTitle'] === $long_title, 'The long title was truncated.');
    expectSqlite($rows[2]['averageRating'] === null && $rows[2]['numVotes'] === null, 'Unrated titles must have NULL ratings.');
    echo "PASS: SQLite file creation, basics, ratings, Unicode, long titles, years and NULL values.\n";

    runSqliteCli($directory, '-p');
    expectSqlite((int) $database->query('SELECT COUNT(*) FROM title')->fetchColumn() === 3, 'Repeated import created duplicates.');
    echo "PASS: repeated SQLite import.\n";

    runSqliteCli($directory, '-t');
    expectSqlite((int) $database->query('SELECT COUNT(*) FROM title')->fetchColumn() === 0, 'The -t flag did not empty the SQLite table.');
    echo "PASS: SQLite table clearing.\n";
} catch (Throwable $error) {
    fwrite(STDERR, 'FAIL: ' . $error->getMessage() . "\n");
    $exit_code = 1;
} finally {
    $database = null;
    if (is_dir($directory)) {
        $resolved_directory = str_replace('\\', '/', realpath($directory));
        $resolved_tests = str_replace('\\', '/', realpath(__DIR__)) . '/';
        if (strpos($resolved_directory, $resolved_tests . '.sqlite test-') === 0) {
            removeSqliteTestDirectory($directory);
        }
        else {
            throw new RuntimeException('Refusing to clean a path outside the test directory.');
        }
    }
}
exit($exit_code);
