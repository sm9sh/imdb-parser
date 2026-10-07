<?php

function printHelp() {
    echo "Usage: php run.php [-d] [-u] [-p] [-t] [-a]\n";
    echo "-d : Download\n-u : Unzip\n-p : Import\n-t : Clear title table (explicit only)\n-a : Download, unzip and import; preserves existing titles\n-h, --help : Show help\n";
}

function cliMain($arguments) {
    if (!$arguments || $arguments === ['-h'] || $arguments === ['--help']) {
        printHelp();
        return;
    }
    $actions = ['-d' => false, '-u' => false, '-p' => false, '-t' => false];
    foreach ($arguments as $argument) {
        if ($argument === '-a') {
            $actions['-d'] = $actions['-u'] = $actions['-p'] = true;
        } elseif (array_key_exists($argument, $actions)) {
            $actions[$argument] = true;
        } else {
            throw new RuntimeException('Unknown argument. Use --help.');
        }
    }
    if (!is_file(__DIR__ . '/config.php')) {
        throw new RuntimeException('Missing config.php. Copy config.php.example and review the settings.');
    }
    try {
        $config = require __DIR__ . '/config.php';
    } catch (Throwable $error) {
        throw new RuntimeException('Could not load config.php. Check PHP syntax and settings.');
    }
    if (!is_array($config)) {
        throw new RuntimeException('config.php must return an array.');
    }
    require_once __DIR__ . '/vendor/autoload.php';
    require_once __DIR__ . '/lib/importer.php';
    require_once __DIR__ . '/lib/files.php';
    $portion = $config['TRANSACTION_PORTION'] ?? 2000;
    if ($actions['-p'] && (!is_int($portion) || $portion < 1)) {
        throw new RuntimeException('TRANSACTION_PORTION must be a positive integer.');
    }
    $out_dir = $config['DOWNLOAD_DIR'] ?? __DIR__ . '/exchange';
    if (!is_string($out_dir) || $out_dir === '') {
        throw new RuntimeException('DOWNLOAD_DIR must be a nonempty path.');
    }
    // Resolve relative download paths from the project, not the caller's directory.
    if (!preg_match('~^(?:[A-Za-z]:[\\\\/]|[\\\\/])~', $out_dir)) {
        $out_dir = __DIR__ . '/' . $out_dir;
    }
    $out_dir = rtrim($out_dir, '/\\') . DIRECTORY_SEPARATOR;
    $lock = @fopen(__DIR__ . '/.imdb-parser.lock', 'c');
    if ($lock === false) {
        throw new RuntimeException('Cannot open project process lock.');
    }
    $conn = null;
    try {
        if (!flock($lock, LOCK_EX | LOCK_NB)) {
            throw new RuntimeException('Another importer is running in this project.');
        }
        $base_url = $config['DATASET_BASE_URL'] ?? 'https://datasets.imdbws.com';
        if (!is_string($base_url) || !in_array(parse_url($base_url, PHP_URL_SCHEME), ['http', 'https'], true)) {
            throw new RuntimeException('DATASET_BASE_URL must use HTTP(S).');
        }
        $base_url = rtrim($base_url, '/');
        $datasets = [
            'title.basics.tsv' => $base_url . '/title.basics.tsv.gz',
            'title.ratings.tsv' => $base_url . '/title.ratings.tsv.gz',
        ];
        if (($actions['-d'] || $actions['-u']) && !is_dir($out_dir) && !@mkdir($out_dir, 0777, true) && !is_dir($out_dir)) {
            throw new RuntimeException('Cannot create DOWNLOAD_DIR.');
        }
        foreach ($datasets as $name => $url) {
            if ($actions['-d']) {
                echo "Downloading $name.gz\n";
                downloadFile($url, $out_dir . $name . '.gz', $config);
            }
            if ($actions['-u']) {
                echo "Extracting $name.gz\n";
                ungzip($out_dir . $name . '.gz', $out_dir . $name, true);
            }
        }
        if ($actions['-p']) {
            foreach ($datasets as $name => $url) {
                validateTsvHeader($out_dir . $name, $name);
            }
        }
        if ($actions['-p'] || $actions['-t']) {
            if (empty($config['DATABASE_URL']) || !is_string($config['DATABASE_URL'])) {
                throw new RuntimeException('DATABASE_URL must be configured.');
            }
            try {
                $conn = \Doctrine\DBAL\DriverManager::getConnection(['url' => $config['DATABASE_URL']]);
                $params = $conn->getParams();
                if (!in_array($params['driver'], ['pdo_sqlite', 'pdo_mysql'], true)) {
                    throw new RuntimeException('Unsupported database driver.');
                }
                $is_sqlite = $params['driver'] === 'pdo_sqlite';
                if ($is_sqlite && !empty($params['path'])) {
                    $db_dir = dirname($params['path']);
                    if (!is_dir($db_dir) && !@mkdir($db_dir, 0777, true) && !is_dir($db_dir)) {
                        throw new RuntimeException('Cannot create database directory.');
                    }
                }
                $conn->connect();
            } catch (Throwable $error) {
                throw new RuntimeException('Database connection failed. Check DATABASE_URL and the PDO extension.');
            }
            ensureSchema($conn, $is_sqlite);
            if ($actions['-t']) {
                $conn->executeStatement($is_sqlite ? 'DELETE FROM title' : 'TRUNCATE TABLE title');
                echo "Cleared title table.\n";
            }
            if ($actions['-p']) {
                foreach ($datasets as $name => $url) {
                    $result = importDataset($conn, $out_dir . $name, $name, $portion);
                    printf("%s: processed=%d committed=%d unmatched=%d queries=%d elapsed=%.3fs\n", $name, $result['processed'], $result['committed'], $result['unmatched'], $result['queries'], $result['elapsed']);
                }
            }
        }
    } finally {
        if ($conn !== null) {
            $conn->close();
        }
        fclose($lock);
    }
}

$started = microtime(true);
set_error_handler(function ($severity, $message) {
    if (error_reporting() & $severity) {
        throw new RuntimeException($message);
    }
    return false;
});
try {
    cliMain(array_slice($argv, 1));
    if ($argc > 1 && !in_array($argv[1], ['--help', '-h'], true)) {
        printf("status=success elapsed=%.3fs peak_memory=%.2fMiB\n", microtime(true) - $started, memory_get_peak_usage(true) / 1048576);
    }
    exit(0);
} catch (Throwable $error) {
    $message = $error instanceof \Doctrine\DBAL\DBALException ? 'Database operation failed.' : $error->getMessage();
    fwrite(STDERR, "status=failed: $message\n");
    exit(1);
}
