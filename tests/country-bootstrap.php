<?php
require_once __DIR__ . '/bootstrap.php';
require_once dirname(__DIR__) . '/vendor/autoload.php';
require_once dirname(__DIR__) . '/lib/importer.php';
if (is_file(dirname(__DIR__) . '/lib/countries.php')) { require_once dirname(__DIR__) . '/lib/countries.php'; }

function countryCase($check, $mode = 'ok') {
    $directory = testDirectory();
    $server = $conn = null;
    try {
        copy(__DIR__ . '/fixtures/wikidata.php', $directory . '/wikidata.php');
        $socket = stream_socket_server('tcp://127.0.0.1:0', $code, $message);
        expect($socket !== false, 'Cannot allocate country fixture port.');
        $address = stream_socket_get_name($socket, false);
        fclose($socket);
        $server = proc_open([PHP_BINARY, '-S', $address, $directory . '/wikidata.php'], [1 => ['file', $directory . '/server.log', 'a'], 2 => ['file', $directory . '/server.log', 'a']], $pipes, $directory);
        expect(is_resource($server), 'Cannot start country fixture.');
        $ready = false;
        for ($attempt = 0; $attempt < 100; $attempt++) {
            $connection = @stream_socket_client('tcp://' . $address, $code, $message, 0.05);
            if ($connection) { fclose($connection); $ready = true; break; }
            usleep(20000);
        }
        expect($ready, 'Country fixture not ready.');
        mkdir($directory . '/db');
        $path = $directory . '/db/imdb.sqlite';
        $conn = \Doctrine\DBAL\DriverManager::getConnection(['driver' => 'pdo_sqlite', 'path' => $path]);
        ensureSchema($conn, true);
        $check($directory, $conn, $path, 'http://' . $address . '/' . $mode);
    } finally {
        if ($conn !== null) { $conn->close(); }
        if (is_resource($server)) { proc_terminate($server); proc_close($server); }
        removeTestDirectory($directory);
    }
}

function fixtureCountrySettings($url, $extra = []) {
    return countrySettings(array_replace(['WIKIDATA_ENDPOINT' => $url, 'CONNECT_TIMEOUT' => 1, 'TRANSFER_TIMEOUT' => 1, 'DOWNLOAD_RETRIES' => 0], $extra));
}
