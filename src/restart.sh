#!/bin/bash
#Quick restart script for dev use

set -e

#Replace this with your install directory!
SCIDB_INSTALL="/home/apoliakov/workspace/scidb_trunk/stage/install"
DBNAME="mydb"

mydir=`dirname $0`
pushd $mydir
#Replace this with your 3RDParty directory!
make SCIDB=$SCIDB_INSTALL SCIDB_3RDPARTY="/opt/scidb/14.9/3rdparty"
iquery -aq "unload_library('load_tools')"
scidb.py stopall $DBNAME 
cp libload_tools.so $SCIDB_INSTALL/lib/scidb/plugins/
#for multinode setups, dont forget to copy to every instance
scidb.py startall $DBNAME
iquery -aq "load_library('load_tools')"

