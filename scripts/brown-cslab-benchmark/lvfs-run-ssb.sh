#!/bin/bash

INSTALL_DIR=/ltmp/adf-lasvegas
RESOURCE_DIR=/home/adf/_lvfs-install
DATE_FORMAT="%Y-%m-%d-%H.%M.%S-%Z"

NUM_PARTS=$1

INPUT_FILE=ssb-$NUM_PARTS.txt
HOSTS_FILE=hosts-$NUM_PARTS.txt
LVFS_DIR=$INSTALL_DIR/las-vegas/lvfs/

if [ "$NUM_PARTS" == "" ]; then
        echo "NUM_PARTS argument required"
        exit
fi

cd $RESOURCE_DIR

if [ ! -f "$INPUT_FILE" ]; then
	echo "$INPUT_FILE does not exist!"
	exit
fi

if [ ! -f "$HOSTS_FILE" ]; then
	echo "$HOSTS_FILE does not exist!"
	exit
fi

CENTRAL_NODE=`head -1 $HOSTS_FILE`

echo "$CENTRAL_NODE" > central.txt

pusher --hosts=$HOSTS_FILE --fork-limit=8 "$RESOURCE_DIR/lvfs-ssb-slave.sh $NUM_PARTS $INSTALL_DIR $RESOURCE_DIR"

pusher --hosts=central.txt "cd $LVFS_DIR; ant -Dconfxml=lvfs_conf.xml -Dformat=true sa-central > /dev/null &"

sleep 15

pusher --hosts=$HOSTS_FILE "cd $LVFS_DIR; ant -Dconfxml=lvfs_conf.xml -Dformat=true sa-data > /dev/null &"

sleep 60

scp $INPUT_FILE $CENTRAL_NODE:$LVFS_DIR > /dev/null

sleep 5

pusher --hosts=central.txt "cd $LVFS_DIR; ant -Dpartitions=$NUM_PARTS -Daddress=$CENTRAL_NODE.cs.brown.edu:28710 -Dinputfile=$INPUT_FILE import-bench > /dev/null"

RUN_NAME=`date +"$DATE_FORMAT"`
scp $CENTRAL_NODE:$LVFS_DIR/lvfs.log logs/lvfs-central-ssb-$RUN_NAME.log
gzip logs/lvfs-central-ssb-$RUN_NAME.log

pusher --hosts=$HOSTS_FILE "killall java"
