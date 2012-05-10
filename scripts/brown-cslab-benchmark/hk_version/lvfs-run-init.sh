#!/bin/bash

INSTALL_DIR=/ltmp/hk-lasvegas
RESOURCE_DIR=/home/hkimura/adflvfs
DATE_FORMAT="%Y-%m-%d-%H.%M.%S-%Z"

SCALE_SIZE=$1
NUM_PARTS=$2
NUM_REPEATS=$3

LINEITEM_INPUT_FILE=lineitem-$NUM_PARTS.txt
PART_INPUT_FILE=part-$NUM_PARTS.txt
ORDERS_INPUT_FILE=orders-$NUM_PARTS.txt
CUSTOMER_INPUT_FILE=customer-$NUM_PARTS.txt
HOSTS_FILE=hosts-$NUM_PARTS.txt
LVFS_DIR=$INSTALL_DIR/las-vegas/lvfs/

if [ "$NUM_PARTS" == "" ]; then
        echo "SCALE_SIZE and NUM_PARTS argument required"
        exit
fi

cd $RESOURCE_DIR

if [ ! -f "$LINEITEM_INPUT_FILE" ]; then
	echo "$LINEITEM_INPUT_FILE does not exist!"
	exit
fi

if [ ! -f "$PART_INPUT_FILE" ]; then
	echo "$PART_INPUT_FILE does not exist!"
	exit
fi

if [ ! -f "$ORDERS_INPUT_FILE" ]; then
	echo "$ORDERS_INPUT_FILE does not exist!"
	exit
fi

if [ ! -f "$CUSTOMER_INPUT_FILE" ]; then
	echo "$CUSTOMER_INPUT_FILE does not exist!"
	exit
fi

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

