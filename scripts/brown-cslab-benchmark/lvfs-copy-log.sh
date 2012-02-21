#!/bin/bash

LVFS_DIR=$1
DEST_DIR=$2

if [ "$DEST_DIR" == "" ]; then
	echo "two arguments required: LVFS_DIR RUN_NAME"
	exit
fi

if [ ! -d "$DEST_DIR" ]; then
	echo "ahh! $DEST_DIR is not a directory"
	exit
fi

if [ ! -d "$LVFS_DIR" ]; then
	echo "ahh! $LVFS_DIR is not a directry"
	exit
fi

if [ ! -f "$LVFS_DIR/lvfs.log" ]; then
	echo "ahh! $LVFS_DIR/lvfs.log does not exist"
	exit
fi

gzip $LVFS_DIR/lvfs.log
cp $LVFS_DIR/lvfs.log.gz $DEST_DIR/lvfs-`hostname`.log.gz
