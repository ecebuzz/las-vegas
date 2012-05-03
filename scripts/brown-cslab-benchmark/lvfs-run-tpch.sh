#!/bin/bash

INSTALL_DIR=/ltmp/adf-lasvegas
RESOURCE_DIR=/home/adf/_lvfs-install
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

pusher --hosts=$HOSTS_FILE "$RESOURCE_DIR/lvfs-tpch-slave.sh $SCALE_SIZE $NUM_PARTS $INSTALL_DIR $RESOURCE_DIR"

pusher --hosts=central.txt "cd $LVFS_DIR; ant -Dconfxml=lvfs_conf.xml -Dformat=true sa-central > /dev/null &"

sleep 15

pusher --hosts=$HOSTS_FILE "cd $LVFS_DIR; ant -Dconfxml=lvfs_conf.xml -Dformat=true sa-data > /dev/null &"

sleep 60

scp $LINEITEM_INPUT_FILE $CENTRAL_NODE:$LVFS_DIR > /dev/null
scp $PART_INPUT_FILE $CENTRAL_NODE:$LVFS_DIR > /dev/null
scp $ORDERS_INPUT_FILE $CENTRAL_NODE:$LVFS_DIR > /dev/null
scp $CUSTOMER_INPUT_FILE $CENTRAL_NODE:$LVFS_DIR > /dev/null

sleep 5

pusher --hosts=central.txt "cd $LVFS_DIR; ant -Dpartitions=$SCALE_SIZE -Daddress=$CENTRAL_NODE.cs.brown.edu:28710 -Dinputfile_lineitem=$LINEITEM_INPUT_FILE -Dinputfile_part=$PART_INPUT_FILE -Dinputfile_customer=$CUSTOMER_INPUT_FILE -Dinputfile_orders=$ORDERS_INPUT_FILE import-bench-tpch > /dev/null"

sleep 10

#
# We're now finished generating and importing data.
#

# First, a utility function:

function sleep_and_flush {
	sleep 5
	pusher --hosts=central.txt "cd $LVFS_DIR; ant -Daddress=$CENTRAL_NODE.cs.brown.edu:28710 -Dinputfile=$LINEITEM_INPUT_FILE cache-flush > /dev/null"
	sleep 5
}

#
# Finally time to start the benchmarks!
#

#Q17 plan A

i=0; while [ $i -lt $NUM_REPEATS ]; do
	sleep_and_flush
	pusher --hosts=central.txt "cd $LVFS_DIR; ant -Daddress=$CENTRAL_NODE.cs.brown.edu:28710 -Dbrand=Brand#34 -Dcontainer='MED DRUM' tpch-bench-q17-plana > /dev/null"
	i=$((i+1))
done

#Q17 plan B

i=0; while [ $i -lt $NUM_REPEATS ]; do
	sleep_and_flush
	pusher --hosts=central.txt "cd $LVFS_DIR; ant -Daddress=$CENTRAL_NODE.cs.brown.edu:28710 -Dbrand=Brand#34 -Dcontainer='MED DRUM' tpch-bench-q17-planb > /dev/null"
	i=$((i+1))
done

#Q18 plan A

i=0; while [ $i -lt $NUM_REPEATS ]; do
	sleep_and_flush
	pusher --hosts=central.txt "cd $LVFS_DIR; ant -Daddress=$CENTRAL_NODE.cs.brown.edu:28710 -Dquantity=312 tpch-bench-q18-plana > /dev/null"
	i=$((i+1))
done

#Q18 plan B

i=0; while [ $i -lt $NUM_REPEATS ]; do
	sleep_and_flush
	pusher --hosts=central.txt "cd $LVFS_DIR; ant -Daddress=$CENTRAL_NODE.cs.brown.edu:28710 -Dquantity=312 tpch-bench-q18-planb > /dev/null"
	i=$((i+1))
done

#
# And we're done!
#

# Collect all the logs

TIMESTAMP=`date +"$DATE_FORMAT"`
RUN_NAME="tpch-scale-$SCALE_SIZE-$TIMESTAMP"
CENTRAL_LOG="lvfs-central-$RUN_NAME.log"

scp $CENTRAL_NODE:$LVFS_DIR/lvfs.log logs/$CENTRAL_LOG
gzip logs/$CENTRAL_LOG

mkdir logs/datanode-$RUN_NAME
pusher --hosts=$HOSTS_FILE "$RESOURCE_DIR/lvfs-copy-log.sh $LVFS_DIR $RESOURCE_DIR/logs/datanode-$RUN_NAME"

# Cleanup unmercifully

pusher --hosts=$HOSTS_FILE "killall java"
