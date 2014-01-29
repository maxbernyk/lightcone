lightcone
=========

light-cone generating script

The script is designed to work with simulated galaxy data stored in a relational
database. Database configuration file is set up for PostgreSQL RDBMS, but can be
modified for use with and other SQL database.

Simulated galaxy data is expexted to be in a box volume. The script re-arranges
the data in a shape of a light-cone.

Included bin2sql.php file is a sample data import script that reads binary simu-
lation data and uploads it into an SQL database. The script is designed to work
with output from SAGE semi analytic-model (Croton et al. in prep.)