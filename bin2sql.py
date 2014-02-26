#!/usr/bin/env python

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#
#    light-cone generating script - uploading script example
#
#    Copyright (C) 2014  Max Bernyk
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

import sys, psycopg2
import numpy as np

if len(sys.argv) < 2:
    print "no path given\n"

data_path_prfx = sys.argv[1]
snapshots      = [127.000, 79.998, 50.000, 30.000, 19.916, 18.244, 16.725, 15.343, 
                  14.086, 12.941, 11.897, 10.944, 10.073, 9.278, 8.550, 7.883, 
                  7.272, 6.712, 6.197, 5.724, 5.289, 4.888, 4.520, 4.179, 3.866, 
                  3.576, 3.308, 3.060, 2.831, 2.619, 2.422, 2.239, 2.070, 1.913, 
                  1.766, 1.630, 1.504, 1.386, 1.276, 1.173, 1.078, 0.989, 0.905, 
                  0.828, 0.755, 0.687, 0.624, 0.564, 0.509, 0.457, 0.408, 0.362, 
                  0.320, 0.280, 0.242, 0.208, 0.175, 0.144, 0.116, 0.089, 0.064, 
                  0.041, 0.020, 0.000];
total_files    = 8

table_prfx = "galaxies_py"
dbhost     = "localhost"
dbname     = "work"
dbuser     = "work"
dbpass     = "work"
dbport     = 5432

Galdesc = [
    ('Type'                         , np.int32,   'float'),
    ('GalaxyIndex'                  , np.int64,   'bigint'),
    ('HaloIndex'                    , np.int32,   'int'),
    ('FOFHaloIdx'                   , np.int32,   'int'),
    ('TreeIdx'                      , np.int32,   'int'),
    ('SnapNum'                      , np.int32,   'int'),
    ('CentralGal'                   , np.int32,   'int'),
    ('CentralMvir'                  , np.float32, 'float'),
    ('Pos1'                         , np.float32, 'float'),
    ('Pos2'                         , np.float32, 'float'),
    ('Pos3'                         , np.float32, 'float'),
    ('Vel1'                         , np.float32, 'float'),
    ('Vel2'                         , np.float32, 'float'),
    ('Vel3'                         , np.float32, 'float'),
    ('Spin1'                        , np.float32, 'float'),
    ('Spin2'                        , np.float32, 'float'),
    ('Spin3'                        , np.float32, 'float'),
    ('Len'                          , np.int32,   'int'),
    ('Mvir'                         , np.float32, 'float'),
    ('Rvir'                         , np.float32, 'float'),
    ('Vvir'                         , np.float32, 'float'),
    ('Vmax'                         , np.float32, 'float'),
    ('VelDisp'                      , np.float32, 'float'),
    ('ColdGas'                      , np.float32, 'float'),
    ('StellarMass'                  , np.float32, 'float'),
    ('BulgeMass'                    , np.float32, 'float'),
    ('HotGas'                       , np.float32, 'float'),
    ('EjectedMass'                  , np.float32, 'float'),
    ('BlackHoleMass'                , np.float32, 'float'),
    ('IntraClusterStars'            , np.float32, 'float'),
    ('MetalsColdGas'                , np.float32, 'float'),
    ('MetalsStellarMass'            , np.float32, 'float'),
    ('MetalsBulgeMass'              , np.float32, 'float'),
    ('MetalsHotGas'                 , np.float32, 'float'),
    ('MetalsEjectedMass'            , np.float32, 'float'),
    ('MetalsIntraClusterStars'      , np.float32, 'float'),
    ('Sfr'                          , np.float32, 'float'),
    ('SfrBulge'                     , np.float32, 'float'),
    ('SfrIntraClusterStars'         , np.float32, 'float'),
    ('DiskRadius'                   , np.float32, 'float'),
    ('Cooling'                      , np.float32, 'float'),
    ('Heating'                      , np.float32, 'float'),
    ('LastMajorMerger'              , np.float32, 'float'),
    ('OutflowRate'                  , np.float32, 'float'),
    ('HeatingRate'                  , np.float32, 'float'),
    ('CoolingRate'                  , np.float32, 'float'),
    ('Rshocked'                     , np.float32, 'float'),
    ('Qjet'                         , np.float32, 'float')
]

names = [Galdesc[i][0] for i in xrange(len(Galdesc))]
formats = [Galdesc[i][1] for i in xrange(len(Galdesc))]
Galdesc_np = np.dtype({'names':names, 'formats':formats}, align=True)
sql_columns = ["%s %s" % (Galdesc[i][0],Galdesc[i][2]) for i in xrange(len(Galdesc))]

conn = psycopg2.connect(database=dbname, user=dbuser, password=dbpass, host=dbhost, port=dbport)
curs = conn.cursor() 

for i in range(0, len(snapshots)):
    tablename = "%s_%d" % (table_prfx, i)
    curs.execute("drop table if exists %s" % tablename)  
    curs.execute("create table %s (%s)" % (tablename, ", ".join(sql_columns)))
    conn.commit()
    n = 0
    for j in range(0,total_files):
        file_name = "%s%4.3f_%d" % (data_path_prfx, snapshots[i], j)
        file = open(file_name, "rb")
        trees = np.fromfile(file, np.dtype(np.int32), 1)
        n_gal = np.fromfile(file, np.dtype(np.int32), 1)
        offset = (trees + 2) * np.dtype(np.int32).itemsize;
        file.seek(offset)
        galaxies = np.fromfile(file,np.dtype(Galdesc_np),n_gal)
        print "%s: %d" % (file_name, n_gal)
        
        data = []
        for g in galaxies:
            data.append("(%s)" % ','.join(str(p) for p in g))
            n = n + 1

        if len(data) > 0:
            insert = "insert into %s values %s" % (tablename, ','.join(d for d in data))
            curs.execute(insert) 
            conn.commit()

        file.close()

    curs.execute("select count(*) from %s" % tablename)
    count = curs.fetchone()[0]
    print "\tread from files: %d, inserted into the database: %d\n" % (n, count)

    if count != n:
        print "error: missing %d records.\nexiting\n" % (n - count)
        sys.exit()


print "\t\tdone.\n"
