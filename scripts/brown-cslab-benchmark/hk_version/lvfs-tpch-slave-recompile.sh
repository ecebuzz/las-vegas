#!/bin/bash
INSTALL_DIR=/ltmp/hk-lasvegas
RESOURCE_DIR=/home/hkimura/adflvfs

SCALE_SIZE=$1
NUM_PARTS=$2

RACK_MAPPER="/home/adf/_hadoop-install/cslab-rackmap/rack-mapper.sh"
CONF_TEMPLATE="$RESOURCE_DIR/lvfs_conf_sunlab.xml"
STDOUT_LOG="$INSTALL_DIR/stdout.log"

HOSTS_FILE=hosts-$NUM_PARTS.txt
LVFS_DIR=$INSTALL_DIR/las-vegas/lvfs/

if [ "$NUM_PARTS" == "" ]; then
        echo "SCALE_SIZE and NUM_PARTS argument required"
        exit
fi


RACKNAME=`$RACK_MAPPER $HOSTNAME | tr -d /\ `
HOST_INDEX=`grep -n $HOSTNAME $RESOURCE_DIR/$HOSTS_FILE | cut -d: -f1`

rm -rf $INSTALL_DIR/las-vegas/lvfs/src
rm -rf $INSTALL_DIR/las-vegas/lvfs/lvfs.log*
cp -a $RESOURCE_DIR/las-vegas.git/lvfs/src $INSTALL_DIR/las-vegas/lvfs
cd $INSTALL_DIR/las-vegas/lvfs
ant clean >> $STDOUT_LOG
ant compile >> $STDOUT_LOG

# Generate config file 
cd $INSTALL_DIR
cat $CONF_TEMPLATE | sed s/HOSTNAME/$HOSTNAME/g | sed s/RACKNAME/$RACKNAME/g > lvfs_conf.xml
cp lvfs_conf.xml las-vegas/lvfs/bin/

