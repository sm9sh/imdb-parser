<?php

$failed = 0;
foreach (glob(__DIR__ . '/*Test.php') as $file) {
    $process = proc_open([PHP_BINARY, '-d', 'error_reporting=' . E_ALL, '-d', 'display_errors=1', '-d', 'log_errors=0', $file], [1 => STDOUT, 2 => STDERR], $pipes);
    if (!is_resource($process) || proc_close($process) !== 0) {
        $failed++;
    }
}
echo $failed === 0 ? "PASS: all test files.\n" : "FAIL: $failed test files.\n";
exit($failed === 0 ? 0 : 1);
