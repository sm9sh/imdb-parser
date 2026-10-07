<?php

function datasetFields($dataset) {
    return $dataset === 'title.basics.tsv'
        ? ['tconst', 'titleType', 'primaryTitle', 'originalTitle', 'isAdult', 'startYear', 'endYear', 'runtimeMinutes', 'genres']
        : ['tconst', 'averageRating', 'numVotes'];
}

function readHeader($handle, $path, $dataset) {
    $line = fgets($handle);
    $fields = $line === false ? [] : explode("\t", rtrim($line, "\r\n"));
    $expected = datasetFields($dataset);
    $sorted = $fields;
    sort($sorted);
    sort($expected);
    if ($sorted !== $expected || count(array_unique($fields)) !== count($fields)) {
        throw new RuntimeException("$path:1: invalid or duplicate TSV headers.");
    }
    return $fields;
}

function validateTsvHeader($path, $dataset) {
    $handle = @fopen($path, 'rb');
    if ($handle === false) {
        throw new RuntimeException("$path:1: cannot open TSV.");
    }
    try {
        readHeader($handle, $path, $dataset);
    } finally {
        fclose($handle);
    }
}

function readTsv($path, $dataset) {
    $handle = @fopen($path, 'rb');
    if ($handle === false) {
        throw new RuntimeException("$path:1: cannot open TSV.");
    }
    try {
        $fields = readHeader($handle, $path, $dataset);
        $number = 1;
        while (($line = fgets($handle)) !== false) {
            $number++;
            $values = explode("\t", rtrim($line, "\r\n"));
            if (count($values) !== count($fields) || preg_match('//u', $line) !== 1) {
                throw new RuntimeException("$path:$number: invalid column count or UTF-8.");
            }
            $row = array_combine($fields, $values);
            foreach ($row as $field => $value) {
                if ($value === '\N') {
                    $row[$field] = null;
                }
            }
            if ($row['tconst'] === null || !preg_match('/^tt[0-9]+$/D', $row['tconst'])) {
                throw new RuntimeException("$path:$number: invalid tconst.");
            }
            foreach (['isAdult', 'startYear', 'endYear', 'runtimeMinutes', 'numVotes'] as $field) {
                if (!array_key_exists($field, $row) || $row[$field] === null) {
                    continue;
                }
                $value = $row[$field];
                if (!preg_match('/^[0-9]+$/D', $value) || strlen($value) > 18 ||
                    ($field === 'isAdult' && $value !== '0' && $value !== '1') ||
                    (($field === 'startYear' || $field === 'endYear') && (int) $value > 9999)) {
                    throw new RuntimeException("$path:$number: invalid $field.");
                }
                $row[$field] = (int) $value;
            }
            if ($dataset === 'title.basics.tsv') {
                foreach (['titleType', 'primaryTitle', 'originalTitle', 'isAdult'] as $field) {
                    if ($row[$field] === null) {
                        throw new RuntimeException("$path:$number: missing required $field.");
                    }
                }
            } elseif ($row['averageRating'] !== null) {
                $rating = $row['averageRating'];
                if (!preg_match('/^[0-9]+(?:\.[0-9]+)?$/D', $rating) || (float) $rating < 0 || (float) $rating > 10) {
                    throw new RuntimeException("$path:$number: invalid averageRating.");
                }
                $row['averageRating'] = (float) $rating;
            }
            yield $number => $row;
        }
        if (!feof($handle)) {
            throw new RuntimeException("$path:" . ($number + 1) . ': read failed.');
        }
    } finally {
        fclose($handle);
    }
}

function assertSqliteSchema($conn) {
    $version = $conn->fetchColumn('SELECT sqlite_version()');
    if (version_compare($version, '3.24.0', '<')) {
        throw new RuntimeException('SQLite 3.24.0 or newer is required.');
    }
    $columns = $conn->fetchAllAssociative('PRAGMA table_info(title)');
    if (!$columns) {
        return;
    }
    $actual = array_column($columns, null, 'name');
    $types = array_fill_keys(['tconst', 'titleType', 'primaryTitle', 'originalTitle', 'genres'], 'TEXT') +
        array_fill_keys(['isAdult', 'startYear', 'endYear', 'runtimeMinutes', 'numVotes'], 'INTEGER') +
        ['averageRating' => 'REAL', 'updated' => 'DATETIME'];
    foreach ($types as $name => $type) {
        if (!isset($actual[$name]) || strtoupper($actual[$name]['type']) !== $type ||
            ($name === 'tconst' && (int) $actual[$name]['pk'] !== 1) ||
            (in_array($name, ['startYear', 'endYear', 'runtimeMinutes', 'genres', 'averageRating', 'numVotes'], true) && $actual[$name]['notnull'])) {
            throw new RuntimeException("Incompatible title schema ($name). Back up the database and migrate explicitly; no columns were changed.");
        }
    }
}

function importDataset($conn, $path, $dataset, $portion) {
    if (!is_int($portion) || $portion < 1) {
        throw new RuntimeException('TRANSACTION_PORTION must be a positive integer.');
    }
    validateTsvHeader($path, $dataset);
    $sqlite = $conn->getDatabasePlatform()->getName() === 'sqlite';
    $basics = $dataset === 'title.basics.tsv';
    $fields = datasetFields($dataset);
    $updates = [];
    if ($basics) {
        foreach (array_slice($fields, 1) as $field) {
            $updates[] = $field . '=' . ($sqlite ? 'excluded.' . $field : 'VALUES(' . $field . ')');
        }
    }
    $statement = $exists = null;
    $prepared_count = 0;
    if (!$sqlite && !$basics) {
        $exists = $conn->prepare('SELECT 1 FROM title WHERE tconst=?');
    }
    $processed = $committed = $batch = $unmatched = $queries = 0;
    $pending = [];
    $pending_bytes = 0;
    $started = microtime(true);
    $last_line = 1;
    // At most 900 bound parameters, below SQLite's historical 999 limit.
    $row_limit = $basics ? 100 : 1;
    $byte_limit = 262144;
    $flush = function () use ($conn, $sqlite, $basics, $fields, $updates, $portion, $dataset, &$statement, &$prepared_count, $exists, &$pending, &$pending_bytes, &$processed, &$committed, &$batch, &$unmatched, &$queries) {
        $count = count($pending);
        if ($count === 0) {
            return;
        }
        if (!$conn->isTransactionActive()) {
            $conn->beginTransaction();
        }
        if ($prepared_count !== $count) {
            if ($statement !== null) {
                $statement->closeCursor();
            }
            if ($basics) {
                $values = '(' . implode(', ', array_fill(0, count($fields), '?')) . ', CURRENT_TIMESTAMP)';
                $sql = 'INSERT INTO title (' . implode(', ', $fields) . ', updated) VALUES ' .
                    implode(', ', array_fill(0, $count, $values)) .
                    ($sqlite ? ' ON CONFLICT(tconst) DO UPDATE SET ' : ' ON DUPLICATE KEY UPDATE ') .
                    implode(', ', $updates) . ', updated=CURRENT_TIMESTAMP';
            } else {
                $sql = 'UPDATE title SET averageRating=?, numVotes=?, updated=CURRENT_TIMESTAMP WHERE tconst=?';
            }
            $statement = $conn->prepare($sql);
            $prepared_count = $count;
        }
        $parameters = [];
        foreach ($pending as $row) {
            if ($basics) {
                foreach ($fields as $field) {
                    $parameters[] = $row[$field];
                }
            } else {
                $parameters = [$row['averageRating'], $row['numVotes'], $row['tconst']];
            }
        }
        $queries++;
        $statement->execute($parameters);
        if (!$basics && $statement->rowCount() === 0) {
            if ($exists !== null) {
                $queries++;
                $exists->execute([$pending[0]['tconst']]);
            }
            if ($exists === null || !$exists->fetchColumn()) {
                $unmatched++;
            }
        }
        $processed += $count;
        $batch += $count;
        $pending = [];
        $pending_bytes = 0;
        if ($batch === $portion) {
            $conn->commit();
            $committed += $batch;
            $batch = 0;
            if ($committed % 100000 === 0) {
                echo "$dataset: committed=$committed\n";
            }
        }
    };
    try {
        foreach (readTsv($path, $dataset) as $number => $row) {
            $last_line = $number;
            $bytes = 0;
            foreach ($row as $value) {
                $bytes += strlen((string) $value);
            }
            if ($pending && $pending_bytes + $bytes > $byte_limit) {
                $flush();
            }
            $pending[] = $row;
            $pending_bytes += $bytes;
            if (count($pending) >= min($row_limit, $portion - $batch) || $pending_bytes >= $byte_limit) {
                $flush();
            }
        }
        $flush();
        if ($conn->isTransactionActive()) {
            $conn->commit();
            $committed += $batch;
        }
        return ['processed' => $processed, 'committed' => $committed, 'unmatched' => $unmatched, 'queries' => $queries, 'elapsed' => microtime(true) - $started];
    } catch (Throwable $error) {
        if ($conn->isTransactionActive()) {
            $conn->rollBack();
        }
        $reason = $error instanceof \Doctrine\DBAL\DBALException ? 'database write failed.' : $error->getMessage();
        throw new RuntimeException("$path:$last_line: import failed; processed=$processed committed=$committed rolled_back=$batch. $reason", 0, $error);
    } finally {
        if ($statement !== null) {
            $statement->closeCursor();
        }
        if ($exists !== null) {
            $exists->closeCursor();
        }
    }
}

function ensureSchema($conn, $is_sqlite) {
    if ($is_sqlite) {
        assertSqliteSchema($conn);
        $conn->executeQuery("
            CREATE TABLE IF NOT EXISTS `title` (
              `tconst` TEXT NOT NULL PRIMARY KEY,
              `titleType` TEXT NOT NULL,
              `primaryTitle` TEXT NOT NULL,
              `originalTitle` TEXT NOT NULL,
              `isAdult` INTEGER NOT NULL,
              `startYear` INTEGER DEFAULT NULL,
              `endYear` INTEGER DEFAULT NULL,
              `runtimeMinutes` INTEGER DEFAULT NULL,
              `genres` TEXT DEFAULT NULL,
              `averageRating` REAL DEFAULT NULL,
              `numVotes` INTEGER DEFAULT NULL,
              `updated` DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL
            )
        ");
        foreach (['averageRating', 'endYear', 'numVotes'] as $column) {
            $conn->executeQuery("CREATE INDEX IF NOT EXISTS `title_{$column}` ON `title` (`{$column}`)");
        }
    }
    else {
        $conn->executeQuery("
            CREATE TABLE IF NOT EXISTS `title` (
              `tconst` varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL,
              `titleType` varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL,
              `primaryTitle` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
              `originalTitle` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
              `isAdult` tinyint(1) NOT NULL,
              `startYear` year(4) NOT NULL,
              `endYear` year(4) NOT NULL,
              `runtimeMinutes` smallint(5) unsigned NOT NULL,
              `genres` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
              `averageRating` float NOT NULL,
              `numVotes` int(10) unsigned NOT NULL,
              `updated` datetime DEFAULT CURRENT_TIMESTAMP NOT NULL,
              PRIMARY KEY (`tconst`),
              KEY `averageRating` (`averageRating`),
              KEY `endYear` (`endYear`),
              KEY `numVotes` (`numVotes`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        ");
    }
}
