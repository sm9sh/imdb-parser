<?php
function downloadFile($url, $dest) {
    $options = [
        CURLOPT_FILE => is_resource($dest) ? $dest : fopen($dest, 'w'),
        CURLOPT_FOLLOWLOCATION => true,
        CURLOPT_URL => $url,
        CURLOPT_FAILONERROR => true, // HTTP code > 400 will throw curl error
    ];

    $ch = curl_init();
    curl_setopt_array($ch, $options);
    $return = curl_exec($ch);

    if ($return === false) {
        throw new Error(curl_error($ch));
    }

    return true;
}

function ungzip($gz_filename, $output_filename = null, $allow_overwrite = false, $read_chunk_length = 10240) {
    //error check zipped file
    if (!$gz_filename) {
        throw new Error('Can’t unzip without a filename.');
    }
    if (strtolower(substr($gz_filename,-3)) !== '.gz') {
        throw new Error('The provided filename does not have the expected .gz extension.');
    }
    if (!file_exists($gz_filename)) {
        throw new Error('The zipped file does not exist.');
    }

    //error check output file
    if (!$output_filename) {
        $output_filename = substr($gz_filename, 0, -3);
    } //just drop the .gz from incoming file by default
    if ((!$allow_overwrite) && file_exists($output_filename)) {
        throw new Error('A file already exists at the output file location.');
    }
    if (file_exists($output_filename) && (!is_writable($output_filename))) {
        throw new Error('The output file location is not writeable.');
    }

    //open the files
    $gz = gzopen($gz_filename, 'rb');
    if (!$gz) {
        throw new Error('The zipped file cannot be opened for reading.');
    }
    $out = fopen($output_filename, 'wb');
    if (!$out) {
        throw new Error('The output file cannot be opened for writing.');
    }

    //keep unzipping $read_chunk_length bytes at a time until we hit the end of the file
    while (!gzeof($gz)) {
        $unzipped = gzread($gz, $read_chunk_length);
        if (fwrite($out, $unzipped) === false) {
            throw new Error('There was an error writing to the output file.');
        }
    }

    //close the files
    gzclose($gz);
    fclose($out);

    //return the output filename
    return $output_filename;
}
