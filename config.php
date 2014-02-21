<?php

/**
*
*    light-cone generating script - database config and access file
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

class db {
    
    private $host = "localhost";
    private $name = "work";
    private $user = "work";
    private $pass = "work";
    private $port = 5432;

    public $table_prfx = "galaxies"; // Simulation table prefix in the database
    public $gal_id = "galaxyindex1"; // Galaxy index column in the simulation tables
    public $box_size = 62.5;         // Simulation box size, Mpc
    public $x  = "pos1";             // X position in the simulation tables, must be in Mpc
    public $y  = "pos2";             // Y position in the simulation tables, must be in Mpc
    public $z  = "pos3";             // Z position in the simulation tables, must be in Mpc
    public $H0 = 100;                // Hubble constant, km/s/Mpc
    public $c  = 299792.458;         // Speed of light, km/s

    private $header_needed = false; // Change to include commented header
    
    public $snapshots = array(0=>127.000,1=>79.998,2=>50.000,3=>30.000,
        4=>19.916,5=>18.244,6=>16.725,7=>15.343,8=>14.086,9=>12.941,10=>11.897,
        11=>10.944,12=>10.073,13=>9.278,14=>8.550,15=>7.883,16=>7.272,17=>6.712,
        18=>6.197,19=>5.724,20=>5.289,21=>4.888,22=>4.520,23=>4.179,24=>3.866,
        25=>3.576,26=>3.308,27=>3.060,28=>2.831,29=>2.619,30=>2.422,31=>2.239,
        32=>2.070,33=>1.913,34=>1.766,35=>1.630,36=>1.504,37=>1.386,38=>1.276,
        39=>1.173,40=>1.078,41=>0.989,42=>0.905,43=>0.828,44=>0.755,45=>0.687,
        46=>0.624,47=>0.564,48=>0.509,49=>0.457,50=>0.408,51=>0.362,52=>0.320,
        53=>0.280,54=>0.242,55=>0.208,56=>0.175,57=>0.144,58=>0.116,59=>0.089,
        60=>0.064,61=>0.041,62=>0.020,63=>0.000);

    function __construct() {
        $this->output_file = fopen($this->table_prfx . ".dat", "w");
        $this->connect();
    }

    function __destruct() {
        fclose($this->output_file);
    }

    public function connect() {
        $connection_string  = "host={$this->host} ";
        $connection_string .= "port={$this->port} ";
        $connection_string .= "user={$this->user} ";
        $connection_string .= "password={$this->pass} ";
        $connection_string .= "dbname={$this->name}";
        $this->db = pg_connect($connection_string) 
            or die("can't connect to the database.\n");
    }

    public function close() {
        pg_close($this->db);
    }

    public function query($query) {
        $pg_result = pg_query($this->db, $query);
        if (!$pg_result) {
            die("\nerror in:\n$query\n");
        }
        $result = array();
        while ($row = pg_fetch_assoc($pg_result)) {
            $result[] = $row;
        }
        return $result;
    }

    public function queryAndPrint($query) {
        $pg_result = pg_query($this->db, $query);
        if (!$pg_result) {
            die("\nerror in:\n$query\n");
        }
        if ($this->header_needed) {
            echo "#";
            for ($i = 0; $i < pg_num_fields($pg_result); $i++) {
                echo " " . pg_field_name($pg_result, $i);
            }
            echo "\n";
            $this->header_needed = false;
        }
        while ($row = pg_fetch_assoc($pg_result)) {
            echo implode(" ", $row) . "\n";
        }
    }

    public function queryAndSave($query) {
        $pg_result = pg_query($this->db, $query);
        if (!$pg_result) {
            die("\nerror in:\n$query\n");
        }
        if ($this->header_needed) {
            fwrite($this->output_file, "#");
            for ($i = 0; $i < pg_num_fields($pg_result); $i++) {
                fwrite($this->output_file, " " . pg_field_name($pg_result, $i));
            }
            fwrite($this->output_file, "\n");
            $this->header_needed = false;
        }
        while ($row = pg_fetch_assoc($pg_result)) {
            fwrite($this->output_file, implode(" ", $row) . "\n");
        }
    }

    public function exec($query) {
        $pg_result = pg_query($this->db, $query);
        if (!$pg_result) {
            die("\nerror in:\n$query\n");
        }
    }

    public function escape_string($str) {
        return pg_escape_string($this->db, $str);
    }

    public function create_index($name, $fields, $table) {
        $pg_result = pg_query($this->db, "create index $name on $table ($fields)");
        if (!$pg_result) {
            die("\nerror in:\n$query\n");
        }
    }

    public function add_ignore_rule($table, $key) {
        $this->exec("create or replace rule insert_ignore as on insert to $table where exists (select true from $table where $key = new.$key) do instead nothing;");
    }
}

?>