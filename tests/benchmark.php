<?php

require __DIR__ . '/bootstrap.php';
require dirname(__DIR__) . '/vendor/autoload.php';
require dirname(__DIR__) . '/lib/importer.php';
require dirname(__DIR__) . '/lib/files.php';

$mode = $argv[1] ?? 'optimized';
$rows = isset($argv[2]) ? filter_var($argv[2], FILTER_VALIDATE_INT) : 100000;
if (!in_array($mode, ['baseline', 'optimized'], true) || $rows === false || $rows < 1) {
    fwrite(STDERR, "Usage: php tests/benchmark.php baseline|optimized [rows]\n");
    exit(1);
}
$directory = testDirectory();
$conn = null;
try {
    $path = $directory . '/title.basics.tsv';
    $handle = fopen($path, 'wb');
    writeAll($handle, implode("\t", datasetFields('title.basics.tsv')) . "\n");
    for ($i = 1; $i <= $rows; $i++) {
        writeAll($handle, "tt$i\tmovie\tBenchmark $i\tBenchmark $i\t0\t1894\t2020\t90\tDrama\n");
    }
    fclose($handle);
    $conn = \Doctrine\DBAL\DriverManager::getConnection(['driver' => 'pdo_sqlite', 'path' => $directory . '/benchmark.sqlite']);
    ensureSchema($conn, true);
    memory_reset_peak_usage();
    $started = microtime(true);
    if ($mode === 'optimized') {
        ob_start();
        $stats = importDataset($conn, $path, 'title.basics.tsv', 2000);
        ob_end_clean();
    } else {
        // Data-write algorithm from 6b0e2a4: fgetcsv, UPDATE then INSERT.
        $handle = fopen($path, 'rb');
        $fields = fgetcsv($handle, 1000, "\t");
        $processed = $queries = 0;
        $conn->beginTransaction();
        while (($values = fgetcsv($handle, 1000, "\t")) !== false) {
            $row = array_combine($fields, $values);
            $row['updated'] = date('Y-m-d H:i:s');
            $update = $row;
            unset($update['tconst']);
            $queries++;
            if (!$conn->update('title', $update, ['tconst' => $row['tconst']])) {
                $queries++;
                $conn->insert('title', $row);
            }
            $processed++;
            if ($processed % 2000 === 0) {
                $conn->commit();
                $conn->beginTransaction();
            }
        }
        $conn->commit();
        fclose($handle);
        $stats = ['processed' => $processed, 'queries' => $queries];
    }
    $elapsed = microtime(true) - $started;
    expect((int) $conn->fetchColumn('SELECT COUNT(*) FROM title') === $rows, 'Benchmark row count differs.');
    echo json_encode([
        'mode' => $mode, 'php' => PHP_VERSION, 'sqlite' => $conn->fetchColumn('SELECT sqlite_version()'),
        'rows' => $rows, 'file_bytes' => filesize($path), 'queries' => $stats['queries'],
        'elapsed_seconds' => round($elapsed, 3), 'rows_per_second' => round($rows / $elapsed),
        'peak_memory_bytes' => memory_get_peak_usage(true), 'transaction_portion' => 2000,
    ], JSON_UNESCAPED_SLASHES) . "\n";
} finally {
    if ($conn !== null) {
        $conn->close();
    }
    removeTestDirectory($directory);
}
