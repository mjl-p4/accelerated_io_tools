#!/bin/bash
#Quick restart script for dev use
iquery -aq "unload_library('load_tools')" > /dev/null 2>&1
set -e

DBNAME="single_server"
mydir=`dirname $0`
pushd $mydir

SCIDB_INSTALL="/opt/scidb/14.12"
make SCIDB_3RDPARTY=$SCIDB_INSTALL

scidb.py stopall $DBNAME 
cp libload_tools.so ${SCIDB_INSTALL}/lib/scidb/plugins/

#for multinode setups, dont forget to copy to every instance
scidb.py startall $DBNAME
iquery -aq "load_library('load_tools')"

