<?php

function countrySettings($config) {
    $enabled = array_key_exists('WIKIDATA_COUNTRIES_ENABLED', $config) ? $config['WIKIDATA_COUNTRIES_ENABLED'] : false;
    $excluded = $config['EXCLUDE_COUNTRIES'] ?? ['IN'];
    $days = $config['WIKIDATA_REFRESH_DAYS'] ?? 30;
    $endpoint = $config['WIKIDATA_ENDPOINT'] ?? 'https://query.wikidata.org/sparql';
    $connect = $config['CONNECT_TIMEOUT'] ?? 10;
    $timeout = $config['TRANSFER_TIMEOUT'] ?? 60;
    $retries = $config['DOWNLOAD_RETRIES'] ?? 2;
    if (!is_bool($enabled) || !is_array($excluded) || !array_is_list($excluded) || count($excluded) > 50 ||
        !is_int($days) || $days < 1 || !is_int($connect) || $connect < 1 || !is_int($timeout) || $timeout < 1 ||
        !is_int($retries) || $retries < 0 || $retries > 5) {
        throw new RuntimeException('Invalid Wikidata country settings.');
    }
    foreach ($excluded as $code) {
        if (!is_string($code) || !preg_match('/^[A-Z]{2}$/D', $code)) {
            throw new RuntimeException('EXCLUDE_COUNTRIES must contain uppercase ISO alpha-2 codes.');
        }
    }
    $url = is_string($endpoint) ? parse_url($endpoint) : false;
    if (!$url || empty($url['host']) || isset($url['user']) || isset($url['pass']) || isset($url['fragment']) ||
        (($url['scheme'] ?? '') !== 'https' && !(($url['scheme'] ?? '') === 'http' && in_array($url['host'], ['127.0.0.1', 'localhost'], true)))) {
        throw new RuntimeException('WIKIDATA_ENDPOINT must use HTTPS without credentials (HTTP allowed on loopback only).');
    }
    return ['enabled' => $enabled, 'excluded' => array_values(array_unique($excluded)), 'days' => $days,
        'endpoint' => $endpoint, 'connect' => min(10, $connect), 'timeout' => min(60, $timeout), 'retries' => min(2, $retries)];
}

function assertCountrySchema($conn) {
    if ($conn->getDatabasePlatform()->getName() !== 'sqlite') {
        throw new RuntimeException('Wikidata countries require SQLite.');
    }
    foreach (['title_country' => ['tconst' => 'TEXT', 'country_qid' => 'TEXT', 'country_code' => 'TEXT'],
        'country_lookup' => ['tconst' => 'TEXT', 'checked_at' => 'DATETIME']] as $table => $types) {
        $columns = $conn->fetchAllAssociative('PRAGMA table_info(' . $table . ')');
        if (!$columns) {
            throw new RuntimeException('Country schema is missing. Run --migrate-countries first.');
        }
        $actual = array_column($columns, null, 'name');
        $primary = array_column(array_filter($columns, function ($column) { return (int) $column['pk'] > 0; }), 'name');
        if ($primary !== ($table === 'title_country' ? ['tconst', 'country_qid'] : ['tconst'])) {
            throw new RuntimeException('Incompatible country schema; back up and repair explicitly.');
        }
        foreach ($types as $field => $type) {
            if (!isset($actual[$field]) || strtoupper($actual[$field]['type']) !== $type ||
                ($field === 'country_code' ? (bool) $actual[$field]['notnull'] : !(bool) $actual[$field]['notnull'])) {
                throw new RuntimeException('Incompatible country schema; back up and repair explicitly.');
            }
        }
    }
}

function migrateCountrySchema($conn, $path) {
    if ($conn->getDatabasePlatform()->getName() !== 'sqlite' || $conn->isTransactionActive()) {
        throw new RuntimeException('Country migration requires SQLite outside a transaction.');
    }
    $existing = (int) $conn->fetchColumn("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('title_country','country_lookup')");
    if ($existing === 2) {
        assertCountrySchema($conn);
        return null;
    }
    if ($existing !== 0 || !is_file($path)) {
        throw new RuntimeException('Partial country schema or missing SQLite file; repair explicitly.');
    }
    if (version_compare($conn->fetchColumn('SELECT sqlite_version()'), '3.27.0', '<')) {
        throw new RuntimeException('Country migration backup requires SQLite 3.27.0 or newer.');
    }
    $backup = dirname($path) . '/imdb-before-countries-' . gmdate('Ymd-His') . '-' . bin2hex(random_bytes(4)) . '.sqlite';
    $conn->executeStatement('VACUUM INTO ?', [$backup]);
    $copy = new PDO('sqlite:' . $backup, null, null, [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
    $copy->exec('PRAGMA query_only=ON');
    if ($copy->query('PRAGMA quick_check')->fetchColumn() !== 'ok') {
        throw new RuntimeException('Country migration backup failed quick_check; schema unchanged.');
    }
    $copy = null;
    $conn->beginTransaction();
    try {
        $conn->executeStatement('CREATE TABLE title_country (tconst TEXT NOT NULL, country_qid TEXT NOT NULL, country_code TEXT DEFAULT NULL, PRIMARY KEY (tconst,country_qid))');
        $conn->executeStatement('CREATE TABLE country_lookup (tconst TEXT NOT NULL PRIMARY KEY, checked_at DATETIME NOT NULL)');
        $conn->executeStatement('CREATE INDEX title_country_code ON title_country (country_code,tconst)');
        $conn->executeStatement('CREATE INDEX title_country_qid ON title_country (country_qid,tconst)');
        assertCountrySchema($conn);
        $conn->commit();
    } catch (Throwable $error) {
        $conn->rollBack();
        throw $error;
    }
    return $backup;
}

function countryIds($ids) {
    if (!is_array($ids) || count($ids) > 100) {
        throw new RuntimeException('Country lookup accepts at most 100 IMDb IDs.');
    }
    foreach ($ids as $id) {
        if (!is_string($id) || !preg_match('/^tt[0-9]+$/D', $id)) {
            throw new RuntimeException('Invalid IMDb ID for country lookup.');
        }
    }
    return array_values(array_unique($ids));
}

function countrySomeValue($binding) {
    if (!is_array($binding) || !is_string($binding['value'] ?? null) || $binding['value'] === '') { return false; }
    return ($binding['type'] ?? null) === 'bnode' || (($binding['type'] ?? null) === 'uri' &&
        preg_match('~^https?://www\.wikidata\.org/\.well-known/genid/[^\s]+$~D', $binding['value']));
}

function parseCountryResponse($body, $ids) {
    try {
        $data = json_decode($body, true, 512, JSON_THROW_ON_ERROR);
    } catch (Throwable $error) {
        throw new RuntimeException('Invalid Wikidata JSON.', 0, $error);
    }
    $vars = $data['head']['vars'] ?? null;
    $rows = $data['results']['bindings'] ?? null;
    if (!is_array($vars) || array_diff(['imdb', 'country', 'code'], $vars) || !is_array($rows) || !array_is_list($rows)) {
        throw new RuntimeException('Invalid Wikidata response structure.');
    }
    $countries = array_fill_keys($ids, []);
    $seen = [];
    foreach ($rows as $row) {
        $imdb = $row['imdb'] ?? null;
        if (!is_array($imdb) || ($imdb['type'] ?? null) !== 'literal' || !is_string($imdb['value'] ?? null) || !array_key_exists($imdb['value'], $countries)) {
            throw new RuntimeException('Wikidata returned an unexpected IMDb ID.');
        }
        $id = $imdb['value'];
        $seen[$id] = true;
        if (!isset($row['country'])) {
            if (isset($row['code'])) { throw new RuntimeException('Country code without country entity.'); }
            continue;
        }
        $country = $row['country'];
        if (countrySomeValue($country)) {
            if (isset($row['code']) && !countrySomeValue($row['code'])) { throw new RuntimeException('Country code without country entity.'); }
            continue;
        }
        if (!is_array($country) || ($country['type'] ?? null) !== 'uri' ||
            !is_string($country['value'] ?? null) || !preg_match('~^https?://www\.wikidata\.org/entity/(Q[1-9][0-9]*)$~D', $country['value'], $match)) {
            throw new RuntimeException('Invalid Wikidata country entity.');
        }
        $code = '';
        if (isset($row['code']) && !countrySomeValue($row['code'])) {
            if (!is_array($row['code']) || ($row['code']['type'] ?? null) !== 'literal' ||
                !is_string($row['code']['value'] ?? null) || !preg_match('/^[A-Z]{2}$/D', $row['code']['value'])) {
                throw new RuntimeException('Invalid Wikidata ISO country code.');
            }
            $code = $row['code']['value'];
        }
        $countries[$id][$match[1]][$code] = true;
    }
    if (count($seen) !== count($ids)) {
        throw new RuntimeException('Incomplete Wikidata response; requested IDs are missing.');
    }
    foreach ($countries as &$list) {
        foreach ($list as &$codes) {
            $codes = array_values(array_filter(array_keys($codes), function ($code) { return $code !== ''; }));
            $codes = count($codes) === 1 ? $codes[0] : null;
        }
        unset($codes);
    }
    unset($list);
    return $countries;
}

function fetchCountryBatch($ids, $settings) {
    $ids = countryIds($ids);
    if (!$ids) { return []; }
    $query = 'PREFIX wdt: <http://www.wikidata.org/prop/direct/> SELECT ?imdb ?country ?code WHERE { VALUES ?imdb { "' .
        implode('" "', $ids) . '" } OPTIONAL { ?item wdt:P345 ?imdb . OPTIONAL { ?item wdt:P495 ?country . OPTIONAL { ?country wdt:P297 ?code . } } } }';
    for ($attempt = 0; $attempt <= $settings['retries']; $attempt++) {
        $body = '';
        $oversized = false;
        $retry_after = 0;
        $curl = curl_init($settings['endpoint']);
        try {
            curl_setopt_array($curl, [
                CURLOPT_POST => true, CURLOPT_POSTFIELDS => http_build_query(['query' => $query, 'format' => 'json']),
                CURLOPT_HTTPHEADER => ['Accept: application/sparql-results+json'],
                CURLOPT_USERAGENT => 'imdb-parser/1.0 (https://github.com/sm9sh/imdb-parser)',
                CURLOPT_PROTOCOLS => CURLPROTO_HTTP | CURLPROTO_HTTPS,
                CURLOPT_CONNECTTIMEOUT => $settings['connect'], CURLOPT_TIMEOUT => $settings['timeout'],
                CURLOPT_WRITEFUNCTION => function ($curl, $chunk) use (&$body, &$oversized) {
                    if (strlen($body) + strlen($chunk) > 2097152) { $oversized = true; return 0; }
                    $body .= $chunk;
                    return strlen($chunk);
                },
                CURLOPT_HEADERFUNCTION => function ($curl, $header) use (&$retry_after) {
                    if (stripos($header, 'Retry-After:') === 0) {
                        $value = trim(substr($header, 12));
                        $seconds = ctype_digit($value) ? (float) $value : max(0, (strtotime($value) ?: 0) - time());
                        $retry_after = $seconds;
                    }
                    return strlen($header);
                },
            ]);
            $ok = curl_exec($curl);
            $status = (int) curl_getinfo($curl, CURLINFO_RESPONSE_CODE);
            $error = curl_errno($curl);
        } finally {
            curl_close($curl);
        }
        if ($oversized) { throw new RuntimeException('Wikidata response exceeds 2 MiB.'); }
        if ($ok && $status >= 200 && $status < 300) {
            return parseCountryResponse($body, $ids);
        }
        $transient = in_array($status, [408, 425, 429, 500, 502, 503, 504], true) ||
            in_array($error, [CURLE_COULDNT_RESOLVE_HOST, CURLE_COULDNT_CONNECT, CURLE_OPERATION_TIMEDOUT, CURLE_PARTIAL_FILE, CURLE_RECV_ERROR, CURLE_SEND_ERROR], true);
        if ($transient && $retry_after > 60) {
            throw new RuntimeException(sprintf('Wikidata Retry-After exceeds 60 seconds (%.0f seconds required); rerun after cooldown. Country cache preserved.', $retry_after));
        }
        if (!$transient || $attempt === $settings['retries']) {
            throw new RuntimeException("Wikidata request failed (HTTP $status, cURL $error); country cache preserved.");
        }
        usleep(max($retry_after * 1000000, 250000 * ($attempt + 1)));
    }
}

function syncCountryIds($conn, $ids, $settings) {
    assertCountrySchema($conn);
    if ($conn->isTransactionActive()) {
        throw new RuntimeException('Country HTTP must run outside title transactions.');
    }
    $ids = countryIds($ids);
    $stats = ['checked' => 0, 'known' => 0, 'unknown' => 0];
    if (!$ids) { return $stats; }
    $placeholders = implode(',', array_fill(0, count($ids), '?'));
    $fresh = $conn->fetchFirstColumn('SELECT tconst FROM country_lookup WHERE tconst IN (' . $placeholders . ') AND checked_at>=?',
        array_merge($ids, [gmdate('Y-m-d H:i:s', time() - $settings['days'] * 86400)]));
    $pending = array_values(array_diff($ids, $fresh));
    if (!$pending) { return $stats; }
    $countries = fetchCountryBatch($pending, $settings);
    $conn->beginTransaction();
    try {
        foreach ($countries as $id => $list) {
            $conn->executeStatement('DELETE FROM title_country WHERE tconst=?', [$id]);
            foreach ($list as $qid => $code) {
                $conn->executeStatement('INSERT INTO title_country (tconst,country_qid,country_code) VALUES (?,?,?)', [$id, $qid, $code]);
            }
            $conn->executeStatement('INSERT INTO country_lookup (tconst,checked_at) VALUES (?,CURRENT_TIMESTAMP) ON CONFLICT(tconst) DO UPDATE SET checked_at=excluded.checked_at', [$id]);
            $stats['checked']++;
            $stats[$list ? 'known' : 'unknown']++;
        }
        $conn->commit();
    } catch (Throwable $error) {
        $conn->rollBack();
        throw $error;
    }
    return $stats;
}

function countryExclusionSql($codes) {
    if (!$codes) { return ['0', []]; }
    $sql = 'tc.country_code IN (' . implode(',', array_fill(0, count($codes), '?')) . ')';
    $parameters = $codes;
    if (in_array('IN', $codes, true)) { $sql .= ' OR tc.country_qid=?'; $parameters[] = 'Q668'; }
    return [$sql, $parameters];
}

function syncCountryDataset($conn, $path, $year, $settings) {
    $stats = ['checked' => 0, 'known' => 0, 'unknown' => 0];
    $pending = [];
    try {
        foreach (readTsv($path, 'title.basics.tsv') as $row) {
            if (!completedTitle($row, $year)) { continue; }
            $pending[] = $row['tconst'];
            if (count($pending) === 100) {
                $batch = syncCountryIds($conn, $pending, $settings);
                foreach ($stats as $key => $value) { $stats[$key] += $batch[$key]; }
                $pending = [];
                if ($stats['checked'] > 0 && $stats['checked'] % 1000 === 0) { echo "countries: checked={$stats['checked']} known={$stats['known']} unknown={$stats['unknown']}\n"; }
            }
        }
        if ($pending) {
            $batch = syncCountryIds($conn, $pending, $settings);
            foreach ($stats as $key => $value) { $stats[$key] += $batch[$key]; }
        }
    } catch (Throwable $error) {
        $reason = $error instanceof \Doctrine\DBAL\DBALException ? 'country database write failed.' : $error->getMessage();
        throw new RuntimeException("Country preflight failed; checked={$stats['checked']} known={$stats['known']} unknown={$stats['unknown']}. $reason", 0, $error);
    }
    return $stats;
}

function syncStoredCountries($conn, $settings, $limit = null) {
    assertCountrySchema($conn);
    $stats = ['checked' => 0, 'known' => 0, 'unknown' => 0];
    $cursor = '';
    $visited = 0;
    $cutoff = gmdate('Y-m-d H:i:s', time() - $settings['days'] * 86400);
    try {
        while ($limit === null || $visited < $limit) {
            $size = $limit === null ? 100 : min(100, $limit - $visited);
            $ids = $conn->fetchFirstColumn('SELECT t.tconst FROM title t WHERE t.tconst>? AND NOT EXISTS (SELECT 1 FROM country_lookup l WHERE l.tconst=t.tconst AND l.checked_at>=?) ORDER BY t.tconst LIMIT ' . $size, [$cursor, $cutoff]);
            if (!$ids) { break; }
            $batch = syncCountryIds($conn, $ids, $settings);
            foreach ($stats as $key => $value) { $stats[$key] += $batch[$key]; }
            $cursor = end($ids);
            $visited += count($ids);
            if ($stats['checked'] > 0 && $stats['checked'] % 1000 === 0) { echo "countries: checked={$stats['checked']} known={$stats['known']} unknown={$stats['unknown']}\n"; }
        }
    } catch (Throwable $error) {
        $reason = $error instanceof \Doctrine\DBAL\DBALException ? 'country database write failed.' : $error->getMessage();
        throw new RuntimeException("Country sync failed; checked={$stats['checked']} known={$stats['known']} unknown={$stats['unknown']}. $reason", 0, $error);
    }
    return $stats;
}
