<?php

$failed = 0;
foreach (glob(__DIR__ . '/*Test.php') as $file) {
    $process = proc_open([PHP_BINARY, '-d', 'error_reporting=' . E_ALL, '-d', 'display_errors=1', '-d', 'log_errors=0', $file], [1 => ['pipe', 'w'], 2 => ['redirect', 1]], $pipes);
    if (!is_resource($process)) {
        fwrite(STDERR, 'Could not start ' . basename($file) . "\n");
        $failed++;
        continue;
    }
    $output = stream_get_contents($pipes[1]);
    fclose($pipes[1]);
    $code = proc_close($process);
    if ($output !== false) {
        echo $output;
    }
    if ($output === false || $code !== 0) {
        $failed++;
    }
}
echo $failed === 0 ? "PASS: all test files.\n" : "FAIL: $failed test files.\n";
exit($failed === 0 ? 0 : 1);
