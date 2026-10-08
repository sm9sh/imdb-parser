<?php

function writeAll($handle, $data) {
    $offset = 0;
    $length = strlen($data);
    while ($offset < $length) {
        $written = @fwrite($handle, substr($data, $offset));
        if ($written === false || $written === 0) {
            throw new RuntimeException('File write failed.');
        }
        $offset += $written;
    }
}

function streamGzip($path, $output = null, $chunk_length = 8192) {
    $input = @fopen($path, 'rb');
    if ($input === false) {
        throw new RuntimeException('Cannot open gzip input.');
    }
    try {
        $context = inflate_init(ZLIB_ENCODING_GZIP);
        $total = 0;
        $ended = false;
        while (!feof($input)) {
            $data = @fread($input, $chunk_length);
            if ($data === false || ($data === '' && !feof($input))) {
                throw new RuntimeException('Gzip read failed.');
            }
            if ($data === '') {
                break;
            }
            if ($ended || ($total === 0 && substr($data, 0, 2) !== "\x1f\x8b")) {
                throw new RuntimeException('Invalid gzip content.');
            }
            $total += strlen($data);
            $decoded = @inflate_add($context, $data, ZLIB_SYNC_FLUSH);
            if ($decoded === false) {
                throw new RuntimeException('Invalid gzip stream or checksum.');
            }
            if ($output !== null) {
                writeAll($output, $decoded);
            }
            $ended = inflate_get_status($context) === ZLIB_STREAM_END;
            if ($ended && inflate_get_read_len($context) !== $total) {
                throw new RuntimeException('Unexpected bytes after gzip stream.');
            }
        }
        if (!$ended) {
            throw new RuntimeException('Truncated or empty gzip stream.');
        }
    } finally {
        fclose($input);
    }
}

function replaceFile($temporary, $destination) {
    if (!@rename($temporary, $destination)) {
        throw new RuntimeException('Cannot replace destination; the previous file was preserved.');
    }
}

function downloadFile($url, $destination, $config = []) {
    $connect_timeout = $config['CONNECT_TIMEOUT'] ?? 10;
    $transfer_timeout = $config['TRANSFER_TIMEOUT'] ?? 3600;
    $retries = $config['DOWNLOAD_RETRIES'] ?? 2;
    if (!is_int($connect_timeout) || $connect_timeout < 1 || !is_int($transfer_timeout) || $transfer_timeout < 1 ||
        !is_int($retries) || $retries < 0 || $retries > 5) {
        throw new RuntimeException('Timeouts must be positive integers; DOWNLOAD_RETRIES must be 0..5.');
    }
    if (!in_array(parse_url($url, PHP_URL_SCHEME), ['http', 'https'], true) ||
        parse_url($url, PHP_URL_USER) !== null || parse_url($url, PHP_URL_PASS) !== null) {
        throw new RuntimeException('Dataset URL must use HTTP(S) without credentials.');
    }
    $temporary = $destination . '.part-' . bin2hex(random_bytes(8));
    $output = null;
    $curl = null;
    try {
        for ($attempt = 0; $attempt <= $retries; $attempt++) {
            $output = @fopen($temporary, $attempt === 0 ? 'x+b' : 'w+b');
            if ($output === false) {
                throw new RuntimeException('Cannot create temporary download in destination directory.');
            }
            $write_failed = false;
            $curl = curl_init($url);
            curl_setopt_array($curl, [
                CURLOPT_FOLLOWLOCATION => true,
                CURLOPT_MAXREDIRS => 5,
                CURLOPT_PROTOCOLS => CURLPROTO_HTTP | CURLPROTO_HTTPS,
                CURLOPT_REDIR_PROTOCOLS => CURLPROTO_HTTP | CURLPROTO_HTTPS,
                CURLOPT_CONNECTTIMEOUT => $connect_timeout,
                CURLOPT_TIMEOUT => $transfer_timeout,
                CURLOPT_FAILONERROR => true,
                CURLOPT_WRITEFUNCTION => function ($curl, $data) use ($output, &$write_failed) {
                    try {
                        writeAll($output, $data);
                        return strlen($data);
                    } catch (Throwable $error) {
                        $write_failed = true;
                        return 0;
                    }
                },
            ]);
            $ok = curl_exec($curl);
            $code = curl_errno($curl);
            $status = (int) curl_getinfo($curl, CURLINFO_RESPONSE_CODE);
            curl_close($curl);
            $curl = null;
            if (!fflush($output)) {
                throw new RuntimeException('Could not flush download.');
            }
            fclose($output);
            $output = null;
            if ($ok && $status >= 200 && $status < 300) {
                streamGzip($temporary);
                replaceFile($temporary, $destination);
                return;
            }
            $transient = in_array($status, [408, 425, 429, 500, 502, 503, 504], true) ||
                in_array($code, [CURLE_COULDNT_RESOLVE_HOST, CURLE_COULDNT_CONNECT, CURLE_OPERATION_TIMEDOUT, CURLE_PARTIAL_FILE, CURLE_RECV_ERROR, CURLE_SEND_ERROR], true);
            if ($write_failed || !$transient || $attempt === $retries) {
                throw new RuntimeException("Download failed (HTTP $status, cURL $code); previous file preserved.");
            }
            usleep(250000 * ($attempt + 1));
        }
    } finally {
        if ($curl !== null) {
            curl_close($curl);
        }
        if (is_resource($output)) {
            fclose($output);
        }
        if (is_file($temporary)) {
            @unlink($temporary);
        }
    }
}

function ungzip($input, $destination = null, $allow_overwrite = false, $chunk_length = 8192) {
    $destination = $destination ?? (str_ends_with(strtolower($input), '.gz') ? substr($input, 0, -3) : $input . '.tsv');
    if ((!$allow_overwrite && file_exists($destination)) || !is_int($chunk_length) || $chunk_length < 2) {
        throw new RuntimeException('Output exists or invalid extraction chunk length.');
    }
    $temporary = $destination . '.part-' . bin2hex(random_bytes(8));
    $output = @fopen($temporary, 'xb');
    if ($output === false) {
        throw new RuntimeException('Cannot create temporary extraction in destination directory.');
    }
    try {
        streamGzip($input, $output, $chunk_length);
        if (!fflush($output)) {
            throw new RuntimeException('Could not flush extracted file.');
        }
        fclose($output);
        $output = null;
        replaceFile($temporary, $destination);
        return $destination;
    } finally {
        if (is_resource($output)) {
            fclose($output);
        }
        if (is_file($temporary)) {
            @unlink($temporary);
        }
    }
}
