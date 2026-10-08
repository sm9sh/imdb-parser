<?php

require __DIR__ . '/bootstrap.php';

function parserCase($basics, $ratings, $check) {
    $directory = testDirectory();
    try {
        testConfig($directory, ['DOWNLOAD_DIR' => $directory . '/exchange/']);
        file_put_contents($directory . '/exchange/title.basics.tsv', $basics);
        file_put_contents($directory . '/exchange/title.ratings.tsv', $ratings);
        $result = testCli($directory, '-p');
        $check($result, $directory);
    } finally {
        removeTestDirectory($directory);
    }
}

$basics = file_get_contents(__DIR__ . '/fixtures/title.basics.tsv');
$ratings = "tconst\taverageRating\tnumVotes\n";
$tests = [
    'literal quotes, Unicode, full lines and trailing empty fields' => function () use ($basics, $ratings) {
        parserCase($basics, $ratings, function ($result, $directory) {
            expect($result['code'] === 0, 'Import failed: ' . $result['stderr'] . $result['stdout']);
            $database = new PDO('sqlite:' . $directory . '/db/imdb.sqlite');
            $rows = $database->query('SELECT * FROM title ORDER BY tconst')->fetchAll(PDO::FETCH_ASSOC);
            expect(count($rows) === 3, 'Long records were split or dropped.');
            expect($rows[0]['primaryTitle'] === '"Тестове" кіно', 'Literal quotes or Unicode changed.');
            expect((int) $rows[0]['startYear'] === 1894 && $rows[0]['endYear'] === null, 'Year or NULL changed.');
            expect(strlen($rows[2]['primaryTitle']) === 1200, 'Long title truncated.');
            expect($rows[1]['genres'] === '' && $rows[1]['runtimeMinutes'] === null, 'Trailing empty field or NULL changed.');
            $database = null;
        });
    },
];
foreach ([
    'duplicate header' => str_replace('originalTitle', 'primaryTitle', $basics),
    'missing header' => str_replace('startYear', 'wrongYear', $basics),
    'column count' => "tconst\ttitleType\tprimaryTitle\toriginalTitle\tisAdult\tstartYear\tendYear\truntimeMinutes\tgenres\ntt1\tmovie\tbroken\n",
    'invalid numeric' => str_replace("\t1894\t", "\t18x4\t", $basics),
    'invalid boolean' => str_replace("\t0\t1894\t", "\t2\t1894\t", $basics),
] as $name => $contents) {
    $tests['reject ' . $name . ' with file and line'] = function () use ($contents, $ratings) {
        parserCase($contents, $ratings, function ($result) {
            expect($result['code'] !== 0, 'Invalid input succeeded.');
            expect(strpos($result['stdout'] . $result['stderr'], 'title.basics.tsv:') !== false, 'Error has no file and line.');
        });
    };
}
runTests($tests);
