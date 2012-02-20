#!/bin/bash

INSTALL_DIR=/ltmp/adf-lasvegas
RESOURCE_DIR=/home/adf/_lvfs-install
DATE_FORMAT="%Y-%m-%d-%H.%M.%S-%Z"

SCALE_SIZE=$1
NUM_PARTS=$2

LINEITEM_INPUT_FILE=lineitem-$NUM_PARTS.txt
PART_INPUT_FILE=part-$NUM_PARTS.txt
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

if [ ! -f "$HOSTS_FILE" ]; then
	echo "$HOSTS_FILE does not exist!"
	exit
fi

CENTRAL_NODE=`head -1 $HOSTS_FILE`

echo "$CENTRAL_NODE" > central.txt

# Get latest code once
rm -rf las-vegas.git
git clone git://github.com/hkimura/las-vegas.git las-vegas.git

pusher --hosts=$HOSTS_FILE "$RESOURCE_DIR/lvfs-tpch-slave.sh $SCALE_SIZE $NUM_PARTS $INSTALL_DIR $RESOURCE_DIR"

pusher --hosts=central.txt "cd $LVFS_DIR; ant -Dconfxml=lvfs_conf.xml -Dformat=true sa-central > /dev/null &"

sleep 15

pusher --hosts=$HOSTS_FILE "cd $LVFS_DIR; ant -Dconfxml=lvfs_conf.xml -Dformat=true sa-data > /dev/null &"

sleep 60

scp $LINEITEM_INPUT_FILE $CENTRAL_NODE:$LVFS_DIR > /dev/null
scp $PART_INPUT_FILE $CENTRAL_NODE:$LVFS_DIR > /dev/null

sleep 5

pusher --hosts=central.txt "cd $LVFS_DIR; ant -Dpartitions=$SCALE_SIZE -Daddress=$CENTRAL_NODE.cs.brown.edu:28710 -Dinputfile_lineitem=$LINEITEM_INPUT_FILE -Dinputfile_part=$PART_INPUT_FILE import-bench-tpch > /dev/null"

sleep 10

pusher --hosts=central.txt "cd $LVFS_DIR; ant -Daddress=$CENTRAL_NODE.cs.brown.edu:28710 -Dbrand=Brand#34 -Dcontainer='MED DRUM' tpch-bench-q17 > /dev/null"

RUN_NAME=`date +"$DATE_FORMAT"`
scp $CENTRAL_NODE:$LVFS_DIR/lvfs.log logs/lvfs-central-tpch-scale-$SCALE_SIZE-$RUN_NAME.log
gzip logs/lvfs-central-tpch-$RUN_NAME.log

pusher --hosts=$HOSTS_FILE "killall java"
