#!/bin/bash
HOSTSFILE=$1
TBLNAME=$2
let HOSTSCOUNT=0
while read HOSTNAME
do
	((HOSTSCOUNT++))
	echo "$HOSTNAME	/ltmp/hk-lasvegas/$TBLNAME.tbl.$HOSTSCOUNT"
done < $HOSTSFILE

