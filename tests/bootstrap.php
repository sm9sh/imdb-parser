<?php

function expect($condition, $message) {
    if (!$condition) {
        throw new RuntimeException($message);
    }
}

function testDirectory() {
    $directory = __DIR__ . '/.tmp-' . bin2hex(random_bytes(8));
    mkdir($directory . '/vendor', 0777, true);
    mkdir($directory . '/exchange');
    $root = dirname(__DIR__);
    copy($root . '/run.php', $directory . '/run.php');
    foreach (glob($root . '/lib/*.php') as $file) {
        if (!is_dir($directory . '/lib')) {
            mkdir($directory . '/lib');
        }
        copy($file, $directory . '/lib/' . basename($file));
    }
    file_put_contents($directory . '/vendor/autoload.php', '<?php require ' . var_export($root . '/vendor/autoload.php', true) . ';');
    return $directory;
}

function testConfig($directory, $settings = []) {
    $config = array_replace([
        'DATABASE_URL' => 'sqlite:///' . str_replace('\\', '/', $directory) . '/db/imdb.sqlite',
        'DOWNLOAD_DIR' => $directory . '/exchange',
        'TRANSACTION_PORTION' => 2,
    ], $settings);
    file_put_contents($directory . '/config.php', '<?php return ' . var_export($config, true) . ';');
}

function testCli($directory, ...$arguments) {
    if (in_array('-d', $arguments, true) || in_array('-a', $arguments, true)) {
        $settings = require $directory . '/config.php';
        expect(parse_url($settings['DATASET_BASE_URL'] ?? '', PHP_URL_HOST) === '127.0.0.1', 'HTTP tests must use the local fixture server.');
    }
    $process = proc_open(array_merge([PHP_BINARY, '-d', 'error_reporting=' . E_ALL, '-d', 'display_errors=1', '-d', 'log_errors=0', $directory . '/run.php'], $arguments), [1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes, $directory);
    expect(is_resource($process), 'Could not start CLI.');
    $stdout = stream_get_contents($pipes[1]);
    $stderr = stream_get_contents($pipes[2]);
    fclose($pipes[1]);
    fclose($pipes[2]);
    return ['code' => proc_close($process), 'stdout' => $stdout, 'stderr' => $stderr];
}

function removeTestDirectory($directory) {
    $resolved = realpath($directory);
    expect($resolved !== false && strpos(str_replace('\\', '/', $resolved), str_replace('\\', '/', realpath(__DIR__)) . '/.tmp-') === 0, 'Refusing cleanup outside tests.');
    $iterator = new RecursiveIteratorIterator(new RecursiveDirectoryIterator($resolved, FilesystemIterator::SKIP_DOTS), RecursiveIteratorIterator::CHILD_FIRST);
    foreach ($iterator as $file) {
        $file->isDir() && !$file->isLink() ? rmdir($file->getPathname()) : unlink($file->getPathname());
    }
    rmdir($resolved);
}

function runTests($tests) {
    $failed = 0;
    foreach ($tests as $name => $test) {
        try {
            $test();
            echo "PASS: $name\n";
        } catch (Throwable $error) {
            fwrite(STDERR, "FAIL: $name: " . $error->getMessage() . "\n");
            $failed++;
        }
    }
    exit($failed === 0 ? 0 : 1);
}
