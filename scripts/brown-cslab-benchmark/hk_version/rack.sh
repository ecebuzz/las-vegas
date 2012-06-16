#!/bin/bash
# use this script like: ./rack.sh < hosts-60.txt > datanodes-60.txt
RACK_MAPPER="/home/adf/_hadoop-install/cslab-rackmap/rack-mapper.sh"
while read HOSTNAME
do
    echo "$HOSTNAME	`$RACK_MAPPER $HOSTNAME | tr -d /\ `"
done

