<?php

require __DIR__ . '/bootstrap.php';
runTests(['bounded writes preserve all rows, huge titles and final partial batch' => function () {
    $directory = testDirectory();
    $database = null;
    try {
        testConfig($directory, ['TRANSACTION_PORTION' => 137]);
        $basics = "tconst\ttitleType\tprimaryTitle\toriginalTitle\tisAdult\tstartYear\tendYear\truntimeMinutes\tgenres\n";
        for ($i = 1; $i <= 231; $i++) {
            $title = $i === 130 ? str_repeat('x', 300000) : "Title $i";
            $basics .= "tt$i\tmovie\t$title\t$title\t0\t1894\t\\N\t1\tDrama\n";
        }
        file_put_contents($directory . '/exchange/title.basics.tsv', $basics);
        file_put_contents($directory . '/exchange/title.ratings.tsv', "tconst\taverageRating\tnumVotes\n");
        $result = testCli($directory, '-p');
        expect($result['code'] === 0, 'Bounded import failed: ' . $result['stderr']);
        preg_match('/title.basics.tsv: .*queries=([0-9]+)/', $result['stdout'], $matches);
        expect(isset($matches[1]) && (int) $matches[1] < 20, 'Large import still sends one SQL statement per row.');
        $database = new PDO('sqlite:' . $directory . '/db/imdb.sqlite');
        expect((int) $database->query('SELECT COUNT(*) FROM title')->fetchColumn() === 231, 'Final batch lost rows.');
        expect((int) $database->query("SELECT LENGTH(primaryTitle) FROM title WHERE tconst='tt130'")->fetchColumn() === 300000, 'Large row lost bytes.');
        expect($database->query("SELECT primaryTitle FROM title WHERE tconst='tt231'")->fetchColumn() === 'Title 231', 'Last partial batch omitted.');
    } finally {
        $database = null;
        removeTestDirectory($directory);
    }
}]);
