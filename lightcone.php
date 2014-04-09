<?php

/**
*
*    light-cone generating script with structure randomisation
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

if ($argc < 7) {
    echo "usage:\n";
    echo "\tphp lightcone.php ";
    echo "<ra min> <ra max> <dec min> <dec max> <z min> <z max> <cut> <include>";
    echo "\n\nparameters:\n\tra/dec - right ascention/declination in degrees;\n";
    echo "\tz - redshift of the light-cone;\n";
    echo "\tcut - conditional expression in quotes (e.g. \"StellarMass > 0.1\");\n";
    echo "\tinclude - additional galaxy properties to include in the catalogue\n";
    echo "\t\t  (comma separated in quotes).\n\n";
    exit();
}

$ra_min  = $argv[1];
$ra_max  = $argv[2];
$dec_min = $argv[3];
$dec_max = $argv[4];
$z_min   = $argv[5];
$z_max   = $argv[6];
$cut     = $argc >= 8 ? $argv[7] : "";
$include = $argc >= 9 ? explode(",", $argv[8]) : array();

$cone = new lightcone($ra_min, $ra_max, $dec_min, $dec_max, $z_min, $z_max, $cut, $include);

$cone->getGalaxies();

class lightcone {

    public $random = true;
    
    function __construct($ra_min, $ra_max, $dec_min, $dec_max, $z_min, $z_max, $cut, $include) {
        include('config.php');
        $this->db = new db();

        $this->ra0  = deg2rad($ra_min);
        $this->ra1  = deg2rad($ra_max);
        $this->dec0 = deg2rad($dec_min);
        $this->dec1 = deg2rad($dec_max);
        $this->z0   = $z_min;
        $this->z1   = $z_max;
        $this->d0   = $this->z2d($z_min);
        $this->d1   = $this->z2d($z_max);
        $this->cut  = $cut;
        $this->incl = $include;
        // $this->make_z_table();

        $this->stackBoxes();
        $this->buildQuery();
    }

    function __destruct() {
        $this->db->close();
    }

    public function getGalaxies() {
        echo "\nsaving the light-cone from `{$this->db->table_prfx}_xxх` into {$this->db->table_prfx}.dat:\n";
        $t0 = microtime(true);
        $n = count($this->queries);
        for ($i = 0; $i < $n; $i++) {
            $this->db->queryAndSave($this->queries[$i]);
            $eta = 0;
            $time_elapsed = microtime(true) - $t0;
            if ($time_elapsed > 30) {
                $eta = $n*$time_elapsed/($i+1) - $time_elapsed;
            }
            $this->progress($i+1, $n, round($eta));
        }
        $t1 = microtime(true);
        echo "finished in " . round(($t1 - $t0),2) . " sec\n\n";
    }

    private function buildQuery() {
        $this->queries = array();
        foreach ($this->boxes as $box) {
            $x  = $box['x'];
            $y  = $box['y'];
            $z  = $box['z'];
            $d0 = $box['d0'];
            $d1 = $box['d1'];
            $sn = $box['snapshot'];
            $id = $this->db->gal_id;
            $q  = "select $id, $x as x, $y as y, $z as z, ";
            $q .= "sqrt($x*$x + $y*$y + $z*$z) as distance, ";
            $q .= "atan2($y,$x) as ra, ";
            $q .= "atan2($z,sqrt($x*$x + $y*$y)) as dec";
            $q .= count($this->incl) > 0 ? ", " . implode(", ", $this->incl) : " ";
            $q .= " from {$this->db->table_prfx}_" . $box['snapshot'] . " where";
            $q .= " sqrt($x*$x + $y*$y + $z*$z) > $d0 ";
            $q .= "and sqrt($x*$x + $y*$y + $z*$z) <= $d1 ";
            $q .= "and atan2($y,$x) > {$this->ra0} ";
            $q .= "and atan2($y,$x) < {$this->ra1} ";
            $q .= "and atan2($z,sqrt($x*$x + $y*$y)) > {$this->dec0} ";
            $q .= "and atan2($z,sqrt($x*$x + $y*$y)) < {$this->dec1}";
            $q .= $this->cut == "" ? "" : " and " . $this->cut;
            $q .= "\n";
            $this->queries[] = $q;
        }
    }

    private function stackBoxes() {
        $this->boxes = array();
        $b = $this->db->box_size;
        $snapshot_ids = array_keys($this->db->snapshots);

        $d_max = floor(1 + $this->d1/$b)*$b;

        for ($x0 = -$d_max; $x0 < $d_max; $x0 += $b) {
            for ($y0 = -$d_max; $y0 < $d_max; $y0 += $b) {
                for ($z0 = -$d_max; $z0 < $d_max; $z0 += $b) {
                    $x1 = $x0+$b;
                    $y1 = $y0+$b;
                    $z1 = $z0+$b;

                    $x00 = $x0 >= 0 ? $x0 : $x1;
                    $y00 = $y0 >= 0 ? $y0 : $y1;
                    $z00 = $z0 >= 0 ? $z0 : $z1;
                    $x11 = $x0 >= 0 ? $x1 : $x0;
                    $y11 = $y0 >= 0 ? $y1 : $y0;
                    $z11 = $z0 >= 0 ? $z1 : $z0;

                    $ra0 = atan2($y00,$x11);
                    $ra1 = atan2($y11,$x00);

                    $dec0 = atan2($z00, sqrt($x11*$x11 + $y11*$y11));
                    $dec1 = atan2($z11, sqrt($x00*$x00 + $y00*$y00));

                    $d0 = sqrt($x00*$x00 + $y00*$y00 + $z00*$z00);
                    $d1 = sqrt($x11*$x11 + $y11*$y11 + $z11*$z11);

                    if ($d1 > $this->d0 && $d0 < $this->d1 &&
                        $ra0 <= $this->ra1 && $ra1 >= $this->ra0 &&
                        $dec0 <= $this->dec1 && $dec1 >= $this->dec0 ) {

                        $need2break = false;
                        $xyz = $this->randomRotation();
                        for ($s = count($snapshot_ids)-1; $s >= 1; $s--) {
                            $zz0 = $this->db->snapshots[$snapshot_ids[$s]];
                            $zz1 = $this->db->snapshots[$snapshot_ids[$s-1]];
                            $dz0 = $this->z2d($zz0);
                            $dz1 = $this->z2d($zz1);
                            
                            if ($dz1 >= $d0) {
                                $snapshot = $snapshot_ids[$s];
                                if ($dz0 < $this->d0) {
                                    $dz0 = $this->d0;
                                }
                                if ($dz1 > $this->d1) {
                                    $dz1 = $this->d1;
                                    $need2break = true;
                                }
                                $this->boxes[] = array('x'=>"({$xyz[0]}+$x0)",
                                    'y'=>"({$xyz[1]}+$y0)", 
                                    'z'=>"({$xyz[2]}+$z0)", 'd0'=>$dz0, 'd1'=>$dz1, 
                                    'snapshot'=>$snapshot);
                            }
                            if ($dz1 >= $d1 || $need2break) {
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    private function randomRotation() {
        $x = $this->db->x;
        $y = $this->db->y;
        $z = $this->db->z;
        if (!$this->random) {
            return array("$x", "$y", "$z");
        }
        $b = $this->db->box_size;
        $rand_mirror = rand(1,8);
        switch ($rand_mirror) {
            case 1:
                $x = "$x";
                $y = "$y";
                $z = "$z";
            break;
            case 2:
                $x = "$b-$x";
                $y = "$y";
                $z = "$z";
            break;
            case 3:
                $x = "$x";
                $y = "$b-$y";
                $z = "$z";
            break;
            case 4:
                $x = "$x";
                $y = "$y";
                $z = "$b-$z";
            break;
            case 5:
                $x = "$b-$x";
                $y = "$b-$y";
                $z = "$z";
            break;
            case 6:
                $x = "$x";
                $y = "$b-$y";
                $z = "$b-$z";
            break;
            case 7:
                $x = "$b-$x";
                $y = "$y";
                $z = "$b-$z";
            break;
            case 8:
                $x = "$b-$x";
                $y = "$b-$y";
                $z = "$b-$z";
            break;
        }

        $rand_rotate = rand(1,6);
        $xyz = array();
        switch ($rand_rotate) {
            case 1:
                $xyz[] = $x;
                $xyz[] = $y;
                $xyz[] = $z;
            break;
            case 2:
                $xyz[] = $z;
                $xyz[] = $x;
                $xyz[] = $y;
            break;
            case 3:
                $xyz[] = $y;
                $xyz[] = $z;
                $xyz[] = $x;
            break;
            case 4:
                $xyz[] = $x;
                $xyz[] = $z;
                $xyz[] = $y;
            break;
            case 5:
                $xyz[] = $y;
                $xyz[] = $x;
                $xyz[] = $z;
            break;
            case 6:
                $xyz[] = $z;
                $xyz[] = $y;
                $xyz[] = $x;
            break;
        }
        return $xyz;
    }

    private function make_z_table() {
        $this->z_table[] = array("z"=>0,"d"=>0,"k"=>0);
        $z_min = 0;
        $z_max = $this->snapshots[0];
        
        for ($z = 0.1; $z <= $z_max; $z += $z*$this->redshifts_incr) {
            $d = $this->z2d($z);
            $this->z_table[] = array("z"=>$z,"d"=>$d,"k"=>0);
        }
        for ($k = 0; $k < count($this->z_table) - 2; $k++) {
            $this->z_table[$k]['k'] = ($this->z_table[$k+1]['z'] -
                $this->z_table[$k]['z'])/
                ($this->z_table[$k+1]['d'] - 
                $this->z_table[$k]['d']);
        }
        $this->z_table[count($this->z_table) - 1]['k'] = 
            $this->z_table[count($this->z_table) - 2]['k'];
    }

    private function z2d($z) {
        $n = 1000;
        $dz = $z/$n;
        $integral = 0;
        
        $c = $this->db->c;
        $H0 = $this->db->H0;
        $h = $H0/100;
        $WM = 0.25;
        $WV = 1.0 - $WM - 0.4165/($H0*$H0);
        $WR = 4.165E-5/($h*$h);
        $WK = 1-$WM-$WR-$WV;
        $az = 1.0/(1+1.0*$z);
        $DTT = 0.0;
        $DCMR = 0.0;
        for ($i = 0; $i < $n; $i++) {
            $a = $az+(1-$az)*($i+0.5)/$n;
            $adot = sqrt($WK+($WM/$a)+($WR/($a*$a))+($WV*$a*$a));
            $DTT = $DTT + 1.0/$adot;
            $DCMR = $DCMR + 1.0/($a*$adot);
        }
        $DTT = (1.-$az)*$DTT/$n;
        $DCMR = (1.-$az)*$DCMR/$n;
        $d = ($c/$H0)*$DCMR;
    
        return $d;
    }
    
    private function d2z($d) {
        $z = 0;
        for ($i = 0; $i < count($this->z_table)-1; $i++) {
            if ($this->z_table[$i]['d'] > $d) {
                $z = $this->z_table[$i-1]['z'] + ($d - 
                    $this->z_table[$i-1]['d'])*
                    $this->z_table[$i-1]['k'];
                break;
            }
        }
        if ($i > 0 && $z == 0) {
            $z = $this->z_table[$i-1]['z'] + 
                ($d - $this->z_table[$i-1]['d'])*
                $this->z_table[$i-1]['k'];
        }
        return $z;
    }

    function progress($current, $total, $eta = 0) {
        $length = 70;
        $time_left = $eta == 0 ? "" : " ETA: $eta sec";
        $stop_sign = "| " . round(100*$current/$total) . "% ($current/$total)$time_left";
        $stop_sign_last = "| 100% ($total/$total)$time_left";
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