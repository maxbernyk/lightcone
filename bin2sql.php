<?php

/**
*
*    light-cone generating script - uploading script example
*
*    Copyright (C) 2014  Max Bernyk
*
*    This program is free software: you can redistribute it and/or modify
*    it under the terms of the GNU General Public License as published by
*    the Free Software Foundation, either version 3 of the License, or
*    (at your option) any later version.
*
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU General Public License for more details.
*
*    You should have received a copy of the GNU General Public License
*    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*
*/

if ($argc < 2) {
    echo "usage:\n";
    echo "\tphp bin2sql.php <data path prefix>\n\n";
    echo "\n";
    exit();
}

$path_and_prefix  = $argv[1];

$bin2sql = new binToSql();
$bin2sql->run($path_and_prefix);

class binToSql {
	
	public $counter = 0;
	public $log_file_name = "log.txt";
	
    private $n_files = 8;
	private $struct = array(
            'GalaxyType'              =>'i',  
            'GalaxyIndex'             =>'i3', 
            'HaloIndex'               =>'i',  
            'FOFHaloIdx'              =>'i',  
            'TreeIdx'                 =>'i',  
            'SnapNum'                 =>'i',  
            'CentralGal'              =>'i',  
            'CentralMvir'             =>'f',  
            'Pos'                     =>'f3', 
            'Vel'                     =>'f3',
            'Spin'                    =>'f3', 
            'Len'                     =>'i',  
            'Mvir'                    =>'f',  
            'Rvir'                    =>'f',  
            'Vvir'                    =>'f',  
            'Vmax'                    =>'f',  
            'VelDisp'                 =>'f',  
            'ColdGas'                 =>'f',  
            'StellarMass'             =>'f',  
            'BulgeMass'               =>'f',  
            'HotGas'                  =>'f',  
            'EjectedMass'             =>'f',  
            'BlackHoleMass'           =>'f',  
            'IntraClusterStars'       =>'f',  
            'MetalsColdGas'           =>'f', 
            'MetalsStellarMass'       =>'f', 
            'MetalsBulgeMass'         =>'f', 
            'MetalsHotGas'            =>'f', 
            'MetalsEjectedMass'       =>'f', 
            'MetalsIntraClusterStars' =>'f', 
            'Sfr'                     =>'f', 
            'SfrBulge'                =>'f', 
            'SfrIntraClusterStars'    =>'f', 
            'DiskRadius'              =>'f', 
            'Cooling'                 =>'f', 
            'Heating'                 =>'f',
            'LastMajorMerger'         =>'f',
            'OutflowRate'             =>'f',
            'infallMvir'              =>'f',
            'infallVvir'              =>'f',
            'infallVmax'              =>'f',
            'Heating'                 =>'f',
            'linebreak'               =>'i'
		);
	
	function __construct() {
		
		ini_set('error_reporting', E_ALL);
		ini_set('display_errors', '1');
		$this->start_time = time();
		
		include('config.php');
        $this->db = new db();
		$this->buildFields();
		$this->f = fopen($this->log_file_name, "w+");
		$this->counter = 0;
	}
	
	function __destruct() {
		$time = time() - $this->start_time;
		$this->db->close();
		fwrite($this->f, "\n\n\tfinished in $time sec.\n\n");

	}

	public function run($path_and_prefix) {
        $this->createDatabaseTable($this->db->table);
        $total_files = $this->n_files*count($this->db->snapshots);
        $current_file = 1;
		foreach (array_reverse(array_keys($this->db->snapshots)) as $s) {
			for ($i = 0; $i <= $this->n_files - 1; $i++) {
				$file_in = $path_and_prefix . sprintf("%01.3f", $this->db->snapshots[$s]) . "_$i";
				fwrite($this->f, sprintf("%01.3f", $this->db->snapshots[$s]) . "_$i...");
				$this->processBinary($file_in, $this->db->snapshots[$s], $this->db->table, $i);
				
				$this->progress($current_file, $total_files);
				$current_file++;
				
				fwrite($this->f, "done\n");
			}
		}
		$count = $this->db->query("select count(*) as n from " . $this->db->table);
		fwrite($this->f, "\nin files: " . $this->counter . ", in the database: " . $count[0]['n'] . "\n");
	}
	
	private function processBinary($file_in, $redshift, $table_name, $file_index) {
		if (!is_file($file_in)) {
			echo "file not found:\n\t\t$file_in\n";
			return;
		}
		try {
			$f = fopen($file_in, "rb");
			$data = unpack("i2", fread($f,8));
			$trees = $data[1];
			$galaxies = $data[2];
			$offset = ($trees + 2 ) * 4;
			rewind($f);
			fseek($f, $offset);
			$format = $this->getFormat($this->struct);
			$bytes = $this->getBytes($this->struct);
			$pos = ftell($f);
			$count = 0;
		} catch (Exception $e) {
			die("Can't read file header $file_in:\n " . $e->getMessage() . "\n");
		}
		// echo "bytes: $bytes, in file: " . (filesize($file_in) - $offset)/$galaxies . "\n";
		// die();
		$data_array = array();
		while ($binary_data = fread($f,$bytes)) {
			$data_array[] = unpack($format, $binary_data);
			$this->counter++;
		}
		$this->writeToDatabase($data_array);
		fclose($f);
	}
	
	private function getFormat($struct) {
		$format = '';
		$comma = "";
		foreach (array_keys($struct) as $s) {
			$format .= $struct[$s] . $s . "/";
		}
		return $format;
	}
	
	private function getBytes($struct) {
		$bytes = 0;
		foreach (array_keys($struct) as $s) {
			$b = 0;
			switch (substr($struct[$s],0,1)) {
				case "i":
					$b = 4;
				break;
				case "f":
					$b = 4;
				break;
			}
			if (strlen($struct[$s]) == 1) {
				$bytes += 1*$b;
			} else {
				$bytes += substr($struct[$s],1,strlen($struct[$s])-1)*$b;
			}
		}
		return $bytes;
	}
	
	public function createDatabaseTable() {
		$this->db->query("drop table if exists {$this->db->table}");
		$fields = '';
		$comma = '';
		foreach (array_keys($this->struct) as $s) {
			$type = "";
			switch (substr($this->struct[$s],0,1)) {
				case ("i"):
					$type = " integer";
					break;
				case ("f"):
					$type = " float";
					break;
				case ("L"):
					$type = " bigint";
					break;
				default:
					$type = " text";
					break;
			}
			if (strlen($this->struct[$s]) > 1 && substr($this->struct[$s],0,1) != "V") {
				$index = (int) substr($this->struct[$s],1,strlen($this->struct[$s])-1);
				for ($i = 1; $i <= $index; $i++) {
					$fields .= $comma . $s . $i . $type;
					if ($comma == "") $comma = ", ";
				}
			} else {
				$fields .= $comma . $s . $type;
			}
			if ($comma == "") $comma = ", ";
		}
		
		$query = "create table {$this->db->table} ($fields)";
		
		$this->db->exec($query);

		$this->db->create_index("snapnum", "SnapNum", $this->db->table);
		$this->db->create_index("position", "Pos1, Pos2, Pos3", $this->db->table);
	}
	
	private function buildFields() {
		$fields = array();
		foreach (array_keys($this->struct) as $s) {
			if (strlen($this->struct[$s]) > 1 && $this->struct[$s] != "L2") {
				$index = (int) substr($this->struct[$s],1,strlen($this->struct[$s])-1);
				for ($i = 1; $i <= $index; $i++) {
					$fields[] = "`" .$s . $i . "`";
				}
			} else {
				$fields[] = "`$s`";
			}
		}
		$this->fields_data = implode(", ", $fields);
		$this->fields = implode(", ", $fields);
	}
	
	private function writeToDatabase($data_array) {

		if (count($data_array) == 0) {
			return;
		}

		$data = "";
		$coma = "";
		
		foreach ($data_array as $d) {
			$data .= $coma . "(" . implode(",",$d) . ")";
			$coma = ",\n";
		}
		$fields = implode(", ",array_keys($d));
		
		$query = "insert into {$this->db->table} ($fields) values $data";
		$this->db->exec($query);
	}
	
	function progress($current, $total) {
	    $length = 70;
	    $stop_sign = "| " . round(100*$current/$total) . "% ($current/$total) ";
	    $stop_sign_last = "| 100% ($total/$total) ";
	    if (is_numeric(exec('tput cols'))) {
	        $length = exec('tput cols') - strlen($stop_sign_last) - 1;
	    }
	    for ($place = $length + strlen($stop_sign_last); $place >= 0; $place--) {
	        echo "\x08";
	    }
	    echo "|";
	    for ($place = 1; $place <= $length; $place++) {
	        echo $place <= ($length*$current/$total) ? "~" : " ";
	    }
	    echo $stop_sign;
	    for ($i = 0; $i < (strlen($stop_sign_last) - strlen($stop_sign)); $i++) {
	        echo " ";
	    }
	    if ($current == $total) {
	        echo "\n";
	    }
	}
}

?>