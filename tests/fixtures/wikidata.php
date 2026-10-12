<?php
$count_file = __DIR__ . '/country-count';
$count = is_file($count_file) ? (int) file_get_contents($count_file) : 0;
file_put_contents($count_file, (string) ($count + 1));
$mode = parse_url($_SERVER['REQUEST_URI'], PHP_URL_PATH);
header('Content-Type: application/sparql-results+json');
if ($mode === '/404' || $mode === '/500' || ($mode === '/retry' && $count === 0)) {
    http_response_code($mode === '/404' ? 404 : ($mode === '/500' ? 500 : 429));
    header('Retry-After: 0');
    echo '{}';
    return;
}
if ($mode === '/fail-second' && $count > 0) { http_response_code(503); echo '{}'; return; }
if ($mode === '/slow') { sleep(2); }
if ($mode === '/json') { echo '{broken'; return; }
if ($mode === '/oversized') { echo str_repeat(' ', 2097153); return; }
$query = $_POST['query'] ?? $_GET['query'] ?? '';
preg_match_all('/"(tt[0-9]+)"/', $query, $matches);
$ids = array_values(array_unique($matches[1]));
if ($mode === '/partial') { array_pop($ids); }
$bindings = [];
$map = [
    'tt1' => [['Q668', 'IN']],
    'tt2' => [['Q30', 'US'], ['Q408', 'AU']],
    'tt3' => [['Q15180', null]],
    'tt5' => [['Q668', 'IN'], ['Q30', 'US']],
    'tt6' => [['Q145', 'GB'], ['Q145', 'UK']],
];
foreach ($ids as $id) {
    $countries = $map[$id] ?? [];
    if ($mode === '/changed' && $id === 'tt2') { $countries = [['Q668', 'IN']]; }
    if (!$countries) { $bindings[] = ['imdb' => ['type' => 'literal', 'value' => $id]]; }
    foreach ($countries as [$qid, $code]) {
        $row = ['imdb' => ['type' => 'literal', 'value' => $id], 'country' => ['type' => 'uri', 'value' => 'http://www.wikidata.org/entity/' . $qid]];
        if ($code !== null) { $row['code'] = ['type' => 'literal', 'value' => $mode === '/code' ? 'IND' : $code]; }
        if ($mode === '/uri') { $row['country']['value'] = 'https://invalid.invalid/Q668'; }
        $bindings[] = $row;
    }
}
if ($mode === '/somevalue' || $mode === '/bnode') {
    $unknown = $mode === '/bnode' ? ['type' => 'bnode', 'value' => 'unknown'] :
        ['type' => 'uri', 'value' => 'http://www.wikidata.org/.well-known/genid/unknown'];
    foreach ($bindings as &$row) {
        if ($row['imdb']['value'] === 'tt4') { $row['country'] = $unknown; }
        if ($row['imdb']['value'] === 'tt1') { $row['code'] = $unknown; }
    }
    unset($row);
}
if ($mode === '/foreign') { $bindings[] = ['imdb' => ['type' => 'literal', 'value' => 'tt99999']]; }
if ($mode === '/duplicate' && $bindings) { $bindings[] = $bindings[0]; }
echo json_encode(['head' => ['vars' => ['imdb', 'country', 'code']], 'results' => ['bindings' => $bindings]]);
