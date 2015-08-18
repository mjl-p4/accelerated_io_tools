#!/bin/bash
#Quick restart script for dev use
iquery -aq "unload_library('genx')" > /dev/null 2>&1
set -e

DBNAME="mydb"
mydir=`dirname $0`
pushd $mydir

SCIDB_INSTALL="/development/scidbtrunk/stage/install/"
make SCIDB_3RDPARTY="/opt/scidb/14.12"

scidb.py stopall $DBNAME 
cp libgenx.so ${SCIDB_INSTALL}/lib/scidb/plugins/
scidb.py startall $DBNAME 
iquery -aq "load_library('genx')"
