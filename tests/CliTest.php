<?php

require __DIR__ . '/bootstrap.php';
runTests([
    'help does not read config or create files' => function () {
        $directory = testDirectory();
        try {
            foreach ([[], ['--help'], ['-h']] as $arguments) {
                $result = testCli($directory, ...$arguments);
                expect($result['code'] === 0 && strpos($result['stdout'], '-p') !== false, 'Help failed.');
                expect(!file_exists($directory . '/config.php') && !is_dir($directory . '/db') && !file_exists($directory . '/.imdb-parser.lock'), 'Help caused side effects.');
            }
            file_put_contents($directory . '/config.php', '<?php throw new Exception("must not read");');
            expect(testCli($directory, '--help')['code'] === 0, 'Help read config.');
        } finally {
            removeTestDirectory($directory);
        }
    },
    'unknown option and missing config return stderr failure' => function () {
        $directory = testDirectory();
        try {
            foreach (['--unknown', '-p'] as $argument) {
                $result = testCli($directory, $argument);
                expect($result['code'] !== 0 && $result['stderr'] !== '', 'Bad invocation succeeded.');
                expect(!file_exists($directory . '/config.php') && !is_dir($directory . '/db'), 'Bad invocation created config or DB.');
            }
        } finally {
            removeTestDirectory($directory);
        }
    },
    'unzip supports spaces and missing trailing slash without DB access' => function () {
        $directory = testDirectory();
        try {
            $downloads = $directory . '/path with spaces';
            mkdir($downloads);
            testConfig($directory, ['DOWNLOAD_DIR' => $downloads, 'DATABASE_URL' => 'mysql://secret-user:secret-pass@invalid.invalid/imdb']);
            foreach (['title.basics.tsv', 'title.ratings.tsv'] as $name) {
                file_put_contents($downloads . '/' . $name . '.gz', gzencode(file_get_contents(__DIR__ . '/fixtures/' . $name)));
            }
            $result = testCli($directory, '-u');
            expect($result['code'] === 0, 'Unzip opened DB or failed: ' . $result['stderr']);
            expect(file_get_contents($downloads . '/title.basics.tsv') === file_get_contents(__DIR__ . '/fixtures/title.basics.tsv'), 'Unzip path incorrect.');
            expect(!is_dir($directory . '/db'), 'Unzip created DB.');
        } finally {
            removeTestDirectory($directory);
        }
    },
    'second process exits before filesystem or DB changes' => function () {
        $directory = testDirectory();
        $lock = null;
        try {
            testConfig($directory);
            $lock = fopen($directory . '/.imdb-parser.lock', 'c');
            expect(flock($lock, LOCK_EX | LOCK_NB), 'Fixture lock failed.');
            $result = testCli($directory, '-t');
            expect($result['code'] !== 0, 'Concurrent process succeeded.');
            expect(!is_dir($directory . '/db'), 'Concurrent process touched DB.');
        } finally {
            if ($lock !== null) {
                fclose($lock);
            }
            removeTestDirectory($directory);
        }
    },
    'import failure does not clear table even when -t is supplied' => function () {
        $directory = testDirectory();
        $database = null;
        try {
            testConfig($directory);
            expect(testCli($directory, '-t')['code'] === 0, 'Initialize failed.');
            $database = new PDO('sqlite:' . $directory . '/db/imdb.sqlite');
            $database->exec("INSERT INTO title(tconst,titleType,primaryTitle,originalTitle,isAdult) VALUES('tt1','movie','Saved','Saved',0)");
            $result = testCli($directory, '-t', '-p');
            expect($result['code'] !== 0, 'Missing files succeeded.');
            expect($database->query('SELECT primaryTitle FROM title')->fetchColumn() === 'Saved', 'Preflight failure cleared table.');
        } finally {
            $database = null;
            removeTestDirectory($directory);
        }
    },
    'database errors hide credentials and return nonzero' => function () {
        $directory = testDirectory();
        try {
            testConfig($directory, ['DATABASE_URL' => 'mysql://secret-user:secret-pass@127.0.0.1:1/imdb']);
            $result = testCli($directory, '-t');
            expect($result['code'] !== 0 && $result['stderr'] !== '', 'DB error did not fail.');
            expect(strpos($result['stderr'] . $result['stdout'], 'secret-') === false, 'Credentials leaked.');
        } finally {
            removeTestDirectory($directory);
        }
    },
]);
