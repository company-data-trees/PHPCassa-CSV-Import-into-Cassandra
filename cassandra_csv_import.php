<?php

/**
 *
 * CassandraCSVImport
 *
 * Imports a CSV file into Cassandra
 *
 * Requires PHPCassa (https://github.com/thobbs/phpcassa)
 *
 * @author Brandon Smith <brandon@sproutworks.com>
 *
 *
 */

class CassandraCSVImport {

	private $map;
	private $keyName;
	private $rowLimit = false;

	private $batchSize = 1000;

	private $connection = false;

	function __construct($keyName, $map) {
		$this->keyName = $keyName;
		$this->map = $map;
	
	}

	/**
	 *
	 * @method void connect() Connect to Cassandra server
	 * @param $host host address
	 * @param $keyspace keyspace to use
	 * @param $port port to connect with
	 *
	 */

	function connect($host, $keyspace, $port = 9160) {
		$servers[0]['host'] = $host;
		$servers[0]['port'] = $port;
		$this->connection = new Connection($keyspace, $servers);	
	}

	function setRowLimit($limit) {
		$this->rowLimit = $limit;
	}

	/**
	 *
	 * @method import() Import from CSV file
	 * @param string $fileName path to CSV file
	 * @param string $columnFamilyName name of column family to import to
	 *
	 */

	function import($fileName, $columnFamilyName) {

		$columnFamily = new ColumnFamily($this->connection, $columnFamilyName);

		$csvFile = fopen($fileName, 'r');

		$headerParts = fgetcsv($csvFile);	// fix this, if there is no header

		foreach($headerParts as $key => $part) {
			//$headerParts[$key] = strtolower($part);
		}
	
		foreach($this->map as $key => $value) {
			if ($value) {
				if (is_numeric($value)) {
					if (isset($headerParts[$value])) {
						$mapColumns[$key] = $value;
					} else {
						throw new Exception("key index $value not found in csv");
					}
				} else {
					$csvKey = array_search($value, $headerParts);
					if ($csvKey) {
						$mapColumns[$key] = $csvKey;
					}
					else {
						throw new Exception("key $value not found in csv header");
					}
				}
			} else {
				if (($csvKey = array_search($key, $headerParts)) !== false) {
					$mapColumns[$key] = $csvKey;
				} else {
					$mapColumns[$key] = false;	
				}
			}
		}

		$rows = 0;
		while (($lineParts = fgetcsv($csvFile)) !== false) {
	
			$curEntry = array();
	
			foreach($mapColumns as $key => $index) {
				$curEntry[$key] = ($mapColumns[$key]) ? $lineParts[$mapColumns[$key]] : '';
			}
		
			$batch[$lineParts[0]] = $curEntry;
	
			if (count($batch) > $this->batchSize) {
				$columnFamily->batch_insert($batch);
				$batch = false;	
			}

			if ($rows % 10000 == 0) {
				echo 'row ' . $rows . "\n";
			}
	
			$rows++;
	
			if ($this->rowLimit && $rows == $this->rowLimit) break;
		}

		if ($batch) {
			$columnFamily->batch_insert($batch);
		}

		fclose($csvFile);
		
		return $rows;
	}
}
?>