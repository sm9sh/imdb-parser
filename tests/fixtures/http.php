<?php

$path = parse_url($_SERVER['REQUEST_URI'], PHP_URL_PATH);
$count_file = __DIR__ . '/count';
$count = is_file($count_file) ? (int) file_get_contents($count_file) : 0;
file_put_contents($count_file, (string) ($count + 1));
if ($path === '/404') {
    http_response_code(404);
} elseif ($path === '/500' || ($path === '/retry' && $count === 0)) {
    http_response_code(500);
} elseif ($path === '/slow') {
    sleep(2);
    echo gzencode('late');
} elseif ($path === '/broken') {
    echo substr(gzencode('broken'), 0, -8);
} elseif ($path === '/cut') {
    header('Content-Length: 10000');
    echo substr(gzencode('cut'), 0, 12);
} elseif (str_ends_with($path, '.tsv.gz')) {
    echo gzencode(file_get_contents(__DIR__ . '/' . basename($path, '.gz')));
} else {
    echo gzencode('complete');
}
