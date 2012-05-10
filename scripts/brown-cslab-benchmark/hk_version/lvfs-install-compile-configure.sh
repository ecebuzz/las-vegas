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

cd $INSTALL_DIR

# Get latest code and compile

cp -a $RESOURCE_DIR/las-vegas.git las-vegas

cd las-vegas/lvfs
ant compile >> $STDOUT_LOG

# Generate config file 

cd $INSTALL_DIR
cat $CONF_TEMPLATE | sed s/HOSTNAME/$HOSTNAME/g | sed s/RACKNAME/$RACKNAME/g > lvfs_conf.xml

cp lvfs_conf.xml las-vegas/lvfs/bin/
