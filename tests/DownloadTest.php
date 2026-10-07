<?php

require __DIR__ . '/bootstrap.php';
require dirname(__DIR__) . '/lib/files.php';

function downloadCase($check) {
    $directory = testDirectory();
    $server = null;
    try {
        copy(__DIR__ . '/fixtures/http.php', $directory . '/http.php');
        foreach (['title.basics.tsv', 'title.ratings.tsv'] as $name) {
            copy(__DIR__ . '/fixtures/' . $name, $directory . '/' . $name);
        }
        $socket = stream_socket_server('tcp://127.0.0.1:0', $code, $message);
        expect($socket !== false, 'Cannot allocate local HTTP port.');
        $address = stream_socket_get_name($socket, false);
        fclose($socket);
        $server = proc_open([PHP_BINARY, '-S', $address, $directory . '/http.php'], [1 => ['file', $directory . '/server.log', 'a'], 2 => ['file', $directory . '/server.log', 'a']], $pipes, $directory);
        expect(is_resource($server), 'HTTP fixture did not start.');
        $ready = false;
        for ($i = 0; $i < 100; $i++) {
            $connection = @stream_socket_client('tcp://' . $address, $code, $message, 0.05);
            if ($connection) {
                fclose($connection);
                $ready = true;
                break;
            }
            usleep(20000);
        }
        expect($ready, 'HTTP fixture not ready.');
        $check($directory, 'http://' . $address);
    } finally {
        if (is_resource($server)) {
            proc_terminate($server);
            proc_close($server);
        }
        removeTestDirectory($directory);
    }
}

$tests = [];
foreach (['404', '500', 'slow', 'cut', 'broken'] as $path) {
    $tests['failed ' . $path . ' download preserves old gzip and cleans temp files'] = function () use ($path) {
        downloadCase(function ($directory, $url) use ($path) {
            $destination = $directory . '/saved.gz';
            $original = gzencode('old file');
            file_put_contents($destination, $original);
            $failed = false;
            try {
                downloadFile($url . '/' . $path, $destination, ['CONNECT_TIMEOUT' => 1, 'TRANSFER_TIMEOUT' => 1, 'DOWNLOAD_RETRIES' => 1]);
            } catch (Throwable $error) {
                $failed = true;
            }
            expect($failed, 'Failure was accepted.');
            expect(file_get_contents($destination) === $original, 'Old file overwritten.');
            expect(!glob($destination . '.part-*'), 'Temporary file leaked.');
            if ($path === '404') {
                expect((int) file_get_contents($directory . '/count') === 1, 'Permanent error retried.');
            }
        });
    };
}
$tests['temporary error retries and installs validated download'] = function () {
    downloadCase(function ($directory, $url) {
        $destination = $directory . '/saved.gz';
        downloadFile($url . '/retry', $destination, ['CONNECT_TIMEOUT' => 1, 'TRANSFER_TIMEOUT' => 1, 'DOWNLOAD_RETRIES' => 1]);
        expect(gzdecode(file_get_contents($destination)) === 'complete', 'Retry did not finish.');
        expect((int) file_get_contents($directory . '/count') === 2, 'Incorrect retry count.');
    });
};
$tests['corrupt, truncated and disguised gzip preserve old TSV'] = function () {
    $directory = testDirectory();
    try {
        foreach (['plain', substr(gzencode('partial'), 0, -8), substr_replace(gzencode('bad CRC'), 'xxxx', -8, 4)] as $contents) {
            file_put_contents($directory . '/input.gz', $contents);
            file_put_contents($directory . '/output.tsv', 'old TSV');
            $failed = false;
            try {
                ungzip($directory . '/input.gz', $directory . '/output.tsv', true);
            } catch (Throwable $error) {
                $failed = true;
            }
            expect($failed, 'Invalid gzip accepted.');
            expect(file_get_contents($directory . '/output.tsv') === 'old TSV', 'Old TSV overwritten.');
            expect(!glob($directory . '/output.tsv.part-*'), 'Temporary extraction leaked.');
        }
        file_put_contents($directory . '/input.data', gzencode('complete TSV'));
        ungzip($directory . '/input.data', $directory . '/output.tsv', true);
        expect(file_get_contents($directory . '/output.tsv') === 'complete TSV', 'Content validation depends on extension.');
    } finally {
        removeTestDirectory($directory);
    }
};
$tests['download-only and full CLI workflow use local HTTP fixture'] = function () {
    downloadCase(function ($directory, $url) {
        testConfig($directory, ['DATASET_BASE_URL' => $url, 'DATABASE_URL' => 'mysql://secret:password@invalid.invalid/imdb']);
        $result = testCli($directory, '-d');
        expect($result['code'] === 0 && !is_dir($directory . '/db'), 'Download opened DB: ' . $result['stderr']);
        testConfig($directory, ['DATASET_BASE_URL' => $url]);
        $result = testCli($directory, '-a');
        expect($result['code'] === 0, 'Full workflow failed: ' . $result['stderr']);
        $database = new PDO('sqlite:' . $directory . '/db/imdb.sqlite');
        expect((int) $database->query('SELECT COUNT(*) FROM title')->fetchColumn() === 3, 'Full workflow imported incorrect count.');
        $database = null;
    });
};
$tests['unavailable destination fails without files'] = function () {
    $directory = testDirectory();
    try {
        file_put_contents($directory . '/blocked', 'file instead of directory');
        $failed = false;
        try {
            ungzip(__DIR__ . '/fixtures/does-not-exist.gz', $directory . '/blocked/result.tsv', true);
        } catch (Throwable $error) {
            $failed = true;
        }
        expect($failed && file_get_contents($directory . '/blocked') === 'file instead of directory', 'Invalid destination modified.');
    } finally {
        removeTestDirectory($directory);
    }
};
runTests($tests);
