#!/bin/bash
INSTALL_DIR=/ltmp/hk-lasvegas
RESOURCE_DIR=/home/hkimura/adflvfs

SCALE_SIZE=$1
NUM_PARTS=$2

HOSTS_FILE=hosts-$NUM_PARTS.txt
LVFS_DIR=$INSTALL_DIR/las-vegas/lvfs/

if [ "$NUM_PARTS" == "" ]; then
        echo "SCALE_SIZE and NUM_PARTS argument required"
        exit
fi

cd $RESOURCE_DIR/las-vegas.git
git pull

cd $RESOURCE_DIR

./pusher --hosts=$HOSTS_FILE "killall java"
sleep 15

./pusher --hosts=$HOSTS_FILE "$RESOURCE_DIR/lvfs-tpch-slave-recompile.sh $1 $2"

sleep 60

./pusher --hosts=central.txt "cd $LVFS_DIR; ant -Dconfxml=lvfs_conf.xml -Dformat=false sa-central > /dev/null &"

sleep 60

./pusher --hosts=$HOSTS_FILE "cd $LVFS_DIR; ant -Dconfxml=lvfs_conf.xml -Dformat=false sa-data > /dev/null &"
