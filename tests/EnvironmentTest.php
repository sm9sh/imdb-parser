<?php

require __DIR__ . '/bootstrap.php';
runTests(['supported SQLite version on disposable database' => function () {
    $directory = testDirectory();
    try {
        $database = new PDO('sqlite:' . $directory . '/version.sqlite');
        expect(version_compare($database->query('SELECT sqlite_version()')->fetchColumn(), '3.24.0', '>='), 'SQLite 3.24.0 or newer is required for upsert.');
        $database = null;
    } finally {
        removeTestDirectory($directory);
    }
}]);
