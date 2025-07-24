#!/bin/bash

# Replace the paths here with your own paths as needed
RUNDIR=/sdf/home/k/kherner/u/OpenSearch

#load up some release; doesn't matter too much which one
. /cvmfs/sw.lsst.eu/almalinux-x86_64/lsst_distrib/w_2025_27/loadLSST-ext.bash 

# You need a working version of OpenSearch that you can import into Python,
# but the directory structure does not have to follow this example.
export PYTHONPATH=${RUNDIR}/opensearch-py/lib/python$(python --version | awk '{print $2}' | cut -d "." -f1,2)/site-packages:$PYTHONPATH

# Path to some protected file that contains the password for
# the "condor" shared account in OpenSearch.
# htcondor_queries.py looks for the OSPWD environment variable 
export OSPWD=$(cat $HOME/.some_file)

cd $RUNDIR

python htcondor_queries.py
