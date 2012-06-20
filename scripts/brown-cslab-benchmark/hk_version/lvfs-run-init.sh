#!/bin/bash

INSTALL_DIR=/ltmp/hk-lasvegas
RESOURCE_DIR=/home/hkimura/adflvfs
DATE_FORMAT="%Y-%m-%d-%H.%M.%S-%Z"

SCALE_SIZE=$1
NUM_PARTS=$2

HOSTS_FILE=hosts-$NUM_PARTS.txt
LVFS_DIR=$INSTALL_DIR/las-vegas/lvfs/

if [ "$NUM_PARTS" == "" ]; then
        echo "SCALE_SIZE and NUM_PARTS argument required"
        exit
fi

cd $RESOURCE_DIR

if [ ! -f "$HOSTS_FILE" ]; then
	echo "$HOSTS_FILE does not exist!"
	exit
fi

CENTRAL_NODE=`head -1 $HOSTS_FILE`

echo "$CENTRAL_NODE" > central.txt

# Get latest code once
rm -rf las-vegas.git
git clone git://github.com/hkimura/las-vegas.git las-vegas.git
rm -rf las-vegas.git/lvfs/exp_results

./pusher --hosts=$HOSTS_FILE "$RESOURCE_DIR/lvfs-tpch-slave.sh $SCALE_SIZE $NUM_PARTS $INSTALL_DIR $RESOURCE_DIR"

