#!/bin/sh

set -o errexit


wget -O- https://paradigm4.github.io/extra-scidb-libs/install.sh \
|  sh -s -- --only-prereq

# Install prerequisites
if [ `lsb_release --id | cut --fields=2` = "CentOS" ]
then
    # yum install --assumeyes                                                    \
    #     https://dl.fedoraproject.org/pub/epel/epel-release-latest-6.noarch.rpm

    ## https://github.com/red-data-tools/packages.red-data-tools.org#centos
    # yum install --assumeyes \
    #     https://packages.red-data-tools.org/centos/red-data-tools-release-1.0.0-1.noarch.rpm

    ## Use Bintray packages:
    ## - Compiled without ORC support (avoid Protocol Buffers conflict)
    # wget --output-document /etc/yum.repos.d/bintray-rvernica-rpm.repo \
    #      https://bintray.com/rvernica/rpm/rpm

    yum install --assumeyes \
        centos-release-scl

    yum install --assumeyes    \
        arrow-devel-$ARROW_VER \
        libpqxx-devel          \
        python27

    source /opt/rh/python27/enable
else
    ## https://github.com/red-data-tools/packages.red-data-tools.org#ubuntu
    ## No packages for Debian jessie, use Ubuntu trusty
    # cat <<APT_LINE | tee /etc/apt/sources.list.d/red-data-tools.list
    # deb https://packages.red-data-tools.org/ubuntu/ trusty universe
    # APT_LINE
    # apt-get update
    # apt-get install                                                      \
    #         --assume-yes --no-install-recommends --allow-unauthenticated \
    #         red-data-tools-keyring

    ## Use Bintray packages:
    ## - Compiled with g++ 4.9
    ## - Compiled without ORC support (avoid Protocol Buffers conflict)
    # cat <<APT_LINE | tee /etc/apt/sources.list.d/bintray-rvernica.list
    # deb https://dl.bintray.com/rvernica/deb trusty universe
    # APT_LINE
    # apt-key adv --keyserver hkp://keyserver.ubuntu.com --recv 46BD98A354BA5235

    apt-get update
    apt-get install                              \
            --assume-yes --no-install-recommends \
            libarrow-dev=$ARROW_VER-1            \
            libpqxx-dev
fi


wget --no-verbose https://bootstrap.pypa.io/get-pip.py
python get-pip.py
pip install pandas pyarrow==$ARROW_VER scidb-py


# Reset SciDB instance count to 4
scidb.py stopall $SCIDB_NAME
rm --recursive $SCIDB_INSTALL_PATH/DB-$SCIDB_NAME
sed --in-place s/server-0=127.0.0.1,1/server-0=127.0.0.1,3/ \
    $SCIDB_INSTALL_PATH/etc/config.ini
scidb.py init-all --force $SCIDB_NAME
scidb.py startall $SCIDB_NAME
iquery --afl --query "list('instances')"


# Turn on aio in Shim
echo aio=1 > /var/lib/shim/conf
service shimsvc restart


# Compile and install plugin
scidb.py stopall $SCIDB_NAME
make --directory /accelerated_io_tools
cp /accelerated_io_tools/libaccelerated_io_tools.so $SCIDB_INSTALL_PATH/lib/scidb/plugins/
scidb.py startall $SCIDB_NAME
iquery --afl --query "load_library('accelerated_io_tools')"
