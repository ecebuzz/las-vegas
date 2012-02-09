#!/bin/bash

NUM_PARTS=$1
INSTALL_DIR=$2
RESOURCE_DIR=$3

RACK_MAPPER="/home/adf/_hadoop-install/cslab-rackmap/rack-mapper.sh"

CONF_TEMPLATE="$RESOURCE_DIR/lvfs_conf_sunlab.xml"
STDOUT_LOG="$INSTALL_DIR/stdout.log"

##

if [ "$RESOURCE_DIR" == "" ]; then
	echo "three arguments required: NUM_PARTS INSTALL_DIR RESOURCE_DIR"
	exit
fi

RACKNAME=`$RACK_MAPPER $HOSTNAME | tr -d /\ `
HOSTS_FILE=hosts-$NUM_PARTS.txt
HOST_INDEX=`grep -n $HOSTNAME $RESOURCE_DIR/$HOSTS_FILE | cut -d: -f1`

rm -rf $INSTALL_DIR
mkdir $INSTALL_DIR
cd $INSTALL_DIR

# Get latest code and compile

git clone git://github.com/hkimura/las-vegas.git >> $STDOUT_LOG 2>&1

cd las-vegas/lvfs
ant compile >> $STDOUT_LOG

# Generate StarSchema Benchmark

cd ../ssb-dbgen/
make >> $STDOUT_LOG 2>&1
./dbgen -T l -s $NUM_PARTS -S $HOST_INDEX -C $NUM_PARTS >> $STDOUT_LOG 2>&1  # $HOST_INDEX is 1-based
mv lineorder.tbl.$HOST_INDEX $INSTALL_DIR

# Can have a race condition. Keep a hard-coded file for now.
# echo -e "$HOSTNAME\t$INSTALL_DIR/lineorder.tbl.$HOST_INDEX" >> $RESOURCE_DIR/ssb-sunlab.txt

# Generate config file 

cd $INSTALL_DIR
cat $CONF_TEMPLATE | sed s/HOSTNAME/$HOSTNAME/g | sed s/RACKNAME/$RACKNAME/g > lvfs_conf.xml

cp lvfs_conf.xml las-vegas/lvfs/bin/
