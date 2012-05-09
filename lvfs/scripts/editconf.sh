#!/bin/sh
rm -f ../bin/lvfs_conf_local.xml
rm -f lvfs_conf_local.xml
LOCALHOST=`hostname`
HOSTNAMELEN=`expr length $LOCALHOST`
RACKLINE=`grep $LOCALHOST racks.txt`
RACKNAME=${RACKLINE:$HOSTNAMELEN+1}
sed -e 's/REPLACERACKNAME/'$RACKNAME'/g' -e 's/REPLACEDATANODENAME/'$LOCALHOST'/g' < lvfs_conf_cslab0a.xml > lvfs_conf_local.xml 
mv lvfs_conf_local.xml ../bin/