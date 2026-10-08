<?php

require __DIR__ . '/bootstrap.php';
$tests = [];
foreach ([0, 1] as $code) {
    $tests['runner preserves redirected child output and exit code ' . $code] = function () use ($code) {
        $directory = testDirectory();
        try {
            copy(__DIR__ . '/run.php', $directory . '/run.php');
            file_put_contents($directory . '/AlphaTest.php', '<?php fwrite(STDOUT, "alpha-child-marker\n");');
            file_put_contents($directory . '/BetaTest.php', '<?php fwrite(STDERR, "beta-child-marker\n"); exit(' . $code . ');');
            $process = proc_open([PHP_BINARY, $directory . '/run.php'], [1 => ['file', $directory . '/output.log', 'w'], 2 => ['file', $directory . '/error.log', 'w']], $pipes);
            expect(is_resource($process), 'Could not start runner.');
            $result = proc_close($process);
            $output = file_get_contents($directory . '/output.log') . file_get_contents($directory . '/error.log');
            expect($result === $code, 'Runner lost failed-file exit status.');
            expect(strpos($output, 'alpha-child-marker') !== false, 'Redirected first child output lost.');
            expect(strpos($output, 'beta-child-marker') !== false, 'Redirected second child diagnostic lost.');
        } finally {
            removeTestDirectory($directory);
        }
    };
}
runTests($tests);
