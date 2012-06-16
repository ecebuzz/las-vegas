#!/bin/bash

INSTALL_DIR=/ltmp/hk-lasvegas
RESOURCE_DIR=/home/hkimura/adflvfs
DATE_FORMAT="%Y-%m-%d-%H.%M.%S-%Z"

SCALE_SIZE=$1
NUM_PARTS=$2
NUM_REPEATS=$3
FRACTURES=$4

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

if [ "$FRACTURES" == "" ]; then
        echo "num of fractures not specified. using the default value."
        FRACTURES=1
fi
echo "num of fractures = $FRACTURES"

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

./pusher --hosts=central.txt "cd $LVFS_DIR; ant -Dconfxml=lvfs_conf.xml -Dformat=true sa-central > /dev/null &"

sleep 15

./pusher --hosts=central.txt "cd $LVFS_DIR; ant -Dconfxml=lvfs_conf.xml -Dlistfile=/home/hkimura/adflvfs/datanodes-60.txt preregister-datanodes > /dev/null &"

sleep 15

./pusher --hosts=$HOSTS_FILE "cd $LVFS_DIR; ant -Dconfxml=lvfs_conf.xml -Dformat=true sa-data > /dev/null &"

sleep 60

scp $LINEITEM_INPUT_FILE $CENTRAL_NODE:$LVFS_DIR > /dev/null
scp $PART_INPUT_FILE $CENTRAL_NODE:$LVFS_DIR > /dev/null
scp $ORDERS_INPUT_FILE $CENTRAL_NODE:$LVFS_DIR > /dev/null
scp $CUSTOMER_INPUT_FILE $CENTRAL_NODE:$LVFS_DIR > /dev/null

sleep 5

./pusher --hosts=central.txt "cd $LVFS_DIR; ant -Dpartitions=$SCALE_SIZE -Daddress=$CENTRAL_NODE.cs.brown.edu:28710 -Dinputfile_lineitem=$LINEITEM_INPUT_FILE -Dinputfile_part=$PART_INPUT_FILE -Dinputfile_customer=$CUSTOMER_INPUT_FILE -Dinputfile_orders=$ORDERS_INPUT_FILE -Dfractures=$FRACTURES import-bench-tpch > /dev/null"


