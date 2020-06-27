#!/bin/sh

set -o errexit

wget -O- https://paradigm4.github.io/extra-scidb-libs/install.sh \
|  sh -s -- --only-prereq

id=`lsb_release --id --short`

# Install prerequisites
if [ "$id" = "CentOS" ]
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

    yum install --assumeyes https://apache.bintray.com/arrow/centos/$(
        cut --delimiter : --fields 5 /etc/system-release-cpe
        )/apache-arrow-release-latest.rpm

    for pkg in centos-release-scl arrow-devel-$ARROW_VER libpqxx-devel python27
    do
        yum install --assumeyes $pkg
    done

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

    codename=`lsb_release --codename --short`
    if [ "$codename" = "stretch" ]
    then
        cat > /etc/apt/sources.list.d/backports.list <<EOF
deb http://deb.debian.org/debian $codename-backports main
EOF
    fi
    wget https://apache.bintray.com/arrow/$(
        echo $id | tr 'A-Z' 'a-z'
        )/apache-arrow-archive-keyring-latest-$codename.deb
    apt install --assume-yes ./apache-arrow-archive-keyring-latest-$codename.deb

    apt-get update
    apt-get install                              \
            --assume-yes --no-install-recommends \
            libarrow-dev=$ARROW_VER-1            \
            libpqxx-dev
fi


wget --no-verbose https://bootstrap.pypa.io/get-pip.py
python get-pip.py
pip install --upgrade scidb-py


# Reset SciDB instance count to 4
scidbctl.py stop $SCIDB_NAME
rm --recursive $SCIDB_INSTALL_PATH/DB-$SCIDB_NAME
sed --in-place s/server-0=127.0.0.1,1/server-0=127.0.0.1,3/ \
    $SCIDB_INSTALL_PATH/etc/config.ini
scidbctl.py init-cluster --force $SCIDB_NAME
scidbctl.py start $SCIDB_NAME
iquery --afl --query "list('instances')"


# Turn on aio in Shim
echo aio=1 > /var/lib/shim/conf
if [ "$id" = "CentOS" ]
then
    systemctl restart shimsvc
else
    service shimsvc restart
fi


# Compile and install plugin
scidbctl.py stop $SCIDB_NAME
make --directory /accelerated_io_tools
cp /accelerated_io_tools/libaccelerated_io_tools.so $SCIDB_INSTALL_PATH/lib/scidb/plugins/
scidbctl.py start $SCIDB_NAME
iquery --afl --query "load_library('accelerated_io_tools')"
