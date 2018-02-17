#!/bin/sh

set -o errexit


# Install prerequisites
apt-get update
apt-get install --assume-yes --no-install-recommends \
        libpqxx-dev


# Reset SciDB instance count to 4
scidb.py stopall $SCIDB_NAME
rm --recursive $SCIDB_INSTALL_PATH/DB-$SCIDB_NAME
sed --in-place s/server-0=127.0.0.1,1/server-0=127.0.0.1,3/ \
    $SCIDB_INSTALL_PATH/etc/config.ini
su --command="                                                       \
    $SCIDB_INSTALL_PATH/bin/scidb.py init-syscat $SCIDB_NAME         \
        --db-password `cut --delimiter : --fields 5  /root/.pgpass`" \
    postgres
scidb.py init-all --force $SCIDB_NAME
scidb.py startall $SCIDB_NAME
iquery --afl --query "list('instances')"


# Compile and install plugin
scidb.py stopall $SCIDB_NAME
make --directory /accelerated_io_tools
cp /accelerated_io_tools/libaccelerated_io_tools.so $SCIDB_INSTALL_PATH/lib/scidb/plugins/
scidb.py startall $SCIDB_NAME
iquery --afl --query "load_library('accelerated_io_tools')"
