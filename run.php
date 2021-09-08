<?php

require_once __DIR__ . '/vendor/autoload.php';

$cfg_fname = __DIR__ . '/config.php';
if (file_exists($cfg_fname)) {
    $config = require($cfg_fname);
}
else {
    copy($cfg_fname . '.example', $cfg_fname);
    echo "!!!Setup your config.php!!!\n";
}

if (empty($config['DATABASE_URL'])) {
    throw new Error('DATABASE_URL param is not defined in config.php');
}

$urls = [
    'https://datasets.imdbws.com/title.basics.tsv.gz',  // movie titles
    'https://datasets.imdbws.com/title.ratings.tsv.gz', // movie ratings
//    'https://datasets.imdbws.com/name.basics.tsv.gz',   // names of actors
];

$conn = \Doctrine\DBAL\DriverManager::getConnection(['url' => $config['DATABASE_URL']]);
$portion = $config['TRANSACTION_PORTION'] ?? 2000;
$out_dir = $config['DOWNLOAD_DIR'] ?? __DIR__ . "/exchange/";

class Args {
    public $is_download = false;
    public $is_unzip = false;
    public $is_truncate_table = false;
    public $is_parse = false;

    function __construct() {
        global $argv, $argc;

        if ($argc === 1) {
            echo "-d : Download\n-u : Unzip\n-p : Parse\n-t : Truncate table\n-a : All proceeds\n\n";
            return;
        }

        foreach ($argv as $arg) {
            switch ($arg)
            {
                case '-d':
                    $this->is_download = true;
                    break;
                case '-u':
                    $this->is_unzip = true;
                    break;
                case '-p':
                    $this->is_parse = true;
                    break;
                case '-t':
                    $this->is_truncate_table = true;
                    break;
                case '-a':
                    $this->is_download = true;
                    $this->is_unzip = true;
                    $this->is_truncate_table = true;
                    $this->is_parse = true;
                    break;
            }
        }
    }
}

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

if (!is_dir($out_dir) && !mkdir($out_dir, 0777, true)) {
    throw new Error("Can't access to the '$out_dir' dir");
}

$a = new Args;

$fnames = [];
foreach ($urls as $url) {
    $zname = trim(parse_url($url, PHP_URL_PATH), '/');
    $fname = str_replace('.gz', '', $zname);

    if ($a->is_download) {
        echo "Downloading $url ...\n";
        downloadFile($url, $out_dir . $zname);
    }

    if ($a->is_unzip) {
        echo "Unpacking $zname to {$out_dir}{$fname} ...\n";
        ungzip($out_dir . $zname, $out_dir . $fname, true);
    }
    $fnames[] = $fname;
}

$conn->executeQuery("
    CREATE TABLE IF NOT EXISTS `title` (
      `tconst` varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL,
      `titleType` varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL,
      `primaryTitle` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
      `originalTitle` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
      `isAdult` tinyint(1) NOT NULL,
      `startYear` year(4) NOT NULL,
      `endYear` year(4) NOT NULL,
      `runtimeMinutes` smallint(5) unsigned NOT NULL,
      `genres` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
      `averageRating` float NOT NULL,
      `numVotes` int(10) unsigned NOT NULL,
      PRIMARY KEY (`tconst`),
      KEY `averageRating` (`averageRating`),
      KEY `endYear` (`endYear`),      
      KEY `numVotes` (`numVotes`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
");

if ($a->is_parse && $a->is_truncate_table) {
    echo "Truncating db table 'title' ...\n";
    $conn->executeQuery('TRUNCATE TABLE title');
}

if ($a->is_parse) {
    foreach ($fnames as $fname) {
        $fpath = $out_dir . $fname;
        echo "Parsing {$out_dir}{$fname} ...\n";
        $conn->beginTransaction();
        $cnt = 0;
        if (($handle = fopen($fpath, "r")) !== FALSE) {
            while (($data = fgetcsv($handle, 1000, "\t")) !== FALSE) {
                $cnt++;

                if ($cnt === 1) {
                    $fields = $data;
                    continue;
                }


                if ($fname === 'title.basics.tsv' && count($fields) === count($data)) {
                    $conn->insert('title', array_combine($fields, $data));
                }

                if ($fname === 'title.basics.tsv') {
                    list($tconst, $averageRating, $numVotes) = $data;
                    $conn->update('title',
                        ['averageRating' => $averageRating, 'numVotes' => $numVotes],
                        ['tconst' => $tconst]);
                }


                if ($cnt % $portion === 0) {
                    $conn->commit();
                    echo "$cnt ";
                    $conn->beginTransaction();
                }
            }
            fclose($handle);
            $conn->commit();
        }
        echo "\n$cnt processed\n\n";
    }
}