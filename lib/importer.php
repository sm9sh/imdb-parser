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
    $fields = datasetFields($dataset);
    if ($dataset === 'title.basics.tsv') {
        $columns = implode(', ', $fields);
        $values = implode(', ', array_fill(0, count($fields), '?'));
        $updates = [];
        foreach (array_slice($fields, 1) as $field) {
            $updates[] = $field . '=' . ($sqlite ? 'excluded.' . $field : 'VALUES(' . $field . ')');
        }
        $sql = "INSERT INTO title ($columns, updated) VALUES ($values, CURRENT_TIMESTAMP) " .
            ($sqlite ? 'ON CONFLICT(tconst) DO UPDATE SET ' : 'ON DUPLICATE KEY UPDATE ') .
            implode(', ', $updates) . ', updated=CURRENT_TIMESTAMP';
    } else {
        $sql = 'UPDATE title SET averageRating=?, numVotes=?, updated=CURRENT_TIMESTAMP WHERE tconst=?';
    }
    $statement = $conn->prepare($sql);
    $exists = !$sqlite && $dataset === 'title.ratings.tsv' ? $conn->prepare('SELECT 1 FROM title WHERE tconst=?') : null;
    $processed = $committed = $batch = $unmatched = $queries = 0;
    $started = microtime(true);
    $last_line = 1;
    try {
        foreach (readTsv($path, $dataset) as $number => $row) {
            $last_line = $number;
            if (!$conn->isTransactionActive()) {
                $conn->beginTransaction();
            }
            $parameters = $dataset === 'title.basics.tsv'
                ? array_map(function ($field) use ($row) { return $row[$field]; }, $fields)
                : [$row['averageRating'], $row['numVotes'], $row['tconst']];
            $queries++;
            $statement->execute($parameters);
            if ($dataset === 'title.ratings.tsv' && $statement->rowCount() === 0) {
                if ($exists !== null) {
                    $queries++;
                    $exists->execute([$row['tconst']]);
                }
                if ($exists === null || !$exists->fetchColumn()) {
                    $unmatched++;
                }
            }
            $processed++;
            $batch++;
            if ($batch === $portion) {
                $conn->commit();
                $committed += $batch;
                $batch = 0;
            }
        }
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
        $statement->closeCursor();
        if ($exists !== null) {
            $exists->closeCursor();
        }
    }
}

