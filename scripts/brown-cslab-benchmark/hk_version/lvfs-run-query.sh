#!/bin/bash

INSTALL_DIR=/ltmp/hk-lasvegas
RESOURCE_DIR=/home/hkimura/adflvfs
DATE_FORMAT="%Y-%m-%d-%H.%M.%S-%Z"

SCALE_SIZE=$1
NUM_PARTS=$2
NUM_REPEATS=$3
FRACTURES=$4

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

if [ ! -f "$HOSTS_FILE" ]; then
	echo "$HOSTS_FILE does not exist!"
	exit
fi

CENTRAL_NODE=`head -1 $HOSTS_FILE`

echo "$CENTRAL_NODE" > central.txt
cd $LVFS_DIR

#
# We're now finished generating and importing data.
#

# First, a utility function:

function sleep_and_flush {
	ant -Daddress=$CENTRAL_NODE.cs.brown.edu:28710 -Dinputfile=$LINEITEM_INPUT_FILE cache-flush
	sleep 5
}

#
# Finally time to start the benchmarks!
#

#Q1 plan A
i=0; while [ $i -lt $NUM_REPEATS ]; do
	sleep_and_flush
	ant -Daddress=$CENTRAL_NODE.cs.brown.edu:28710 -Ddelta=90 tpch-bench-q1-plana
	i=$((i+1))
done

#Q1 plan B
i=0; while [ $i -lt $NUM_REPEATS ]; do
	sleep_and_flush
	ant -Daddress=$CENTRAL_NODE.cs.brown.edu:28710 -Ddelta=90 tpch-bench-q1-planb
	i=$((i+1))
done

#Q15 plan A
i=0; while [ $i -lt $NUM_REPEATS ]; do
	sleep_and_flush
	ant -Daddress=$CENTRAL_NODE.cs.brown.edu:28710 -Ddate=19960101 tpch-bench-q15-plana
	i=$((i+1))
done

#Q15 plan B
i=0; while [ $i -lt $NUM_REPEATS ]; do
	sleep_and_flush
	ant -Daddress=$CENTRAL_NODE.cs.brown.edu:28710 -Ddate=19960101 tpch-bench-q15-planb
	i=$((i+1))
done

#Q15 plan C
i=0; while [ $i -lt $NUM_REPEATS ]; do
	sleep_and_flush
	ant -Daddress=$CENTRAL_NODE.cs.brown.edu:28710 -Ddate=19960101 tpch-bench-q15-planc
	i=$((i+1))
done

#Q17 plan A
i=0; while [ $i -lt $NUM_REPEATS ]; do
	sleep_and_flush
	ant -Daddress=$CENTRAL_NODE.cs.brown.edu:28710 -Dbrand=Brand#34 -Dcontainer='MED DRUM' tpch-bench-q17-plana
	i=$((i+1))
done

#Q17 plan B
i=0; while [ $i -lt $NUM_REPEATS ]; do
	sleep_and_flush
	ant -Daddress=$CENTRAL_NODE.cs.brown.edu:28710 -Dbrand=Brand#34 -Dcontainer='MED DRUM' tpch-bench-q17-planb
	i=$((i+1))
done

#Q18 plan A
i=0; while [ $i -lt $NUM_REPEATS ]; do
	sleep_and_flush
	ant -Daddress=$CENTRAL_NODE.cs.brown.edu:28710 -Dquantity=312 tpch-bench-q18-plana
	i=$((i+1))
done

#Q18 plan B
i=0; while [ $i -lt $NUM_REPEATS ]; do
	sleep_and_flush
	ant -Daddress=$CENTRAL_NODE.cs.brown.edu:28710 -Dquantity=312 tpch-bench-q18-planb
	i=$((i+1))
done


#Recovery from Buddy
# Unlike the foreign recovery below, the lostPartitions is always 1.
# As the fracturing is a partitioning on orderkey, and the recovered partition is also partitioned by orderkey,
# Fracturing doesn't change the #tuples in each partition! So, we must not adjust lostPartitions to $FRACTURES.
# Spent half a day to figure this out...
i=0; while [ $i -lt $NUM_REPEATS ]; do
	sleep_and_flush
	ant -Daddress=$CENTRAL_NODE.cs.brown.edu:28710 -Dforeign=false -DlostPartitions=1 tpch-recovery-bench
	i=$((i+1))
done

#Recovery with repartitioning
# to have the same number of tuples recovered, #lostPartitions=$FRACTURES
# because each partition in each fracture is smaller with more fractures.
i=0; while [ $i -lt $NUM_REPEATS ]; do
	sleep_and_flush
	ant -Daddress=$CENTRAL_NODE.cs.brown.edu:28710 -Dforeign=true -DlostPartitions=$FRACTURES tpch-recovery-bench
	i=$((i+1))
done

#
# And we're done!
#

# Collect all the logs
cd $RESOURCE_DIR
TIMESTAMP=`date +"$DATE_FORMAT"`
RUN_NAME="tpch-scale-$SCALE_SIZE-$TIMESTAMP"
CENTRAL_LOG="lvfs-central-$RUN_NAME.log"

scp $CENTRAL_NODE:$LVFS_DIR/lvfs.log logs/$CENTRAL_LOG
gzip logs/$CENTRAL_LOG

mkdir logs/datanode-$RUN_NAME
./pusher --hosts=$HOSTS_FILE "$RESOURCE_DIR/lvfs-copy-log.sh $LVFS_DIR $RESOURCE_DIR/logs/datanode-$RUN_NAME"

# Cleanup unmercifully

#./pusher --hosts=$HOSTS_FILE "killall java"
