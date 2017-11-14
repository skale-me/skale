#!/bin/sh

unset CDPATH
export LC_ALL=C IFS=' 	
'
tmp=/tmp/$USER

# Compute canonical path to retrieve all conf/exec files from it
case $0 in (/*) cpath=$0;; (*) cpath=$PWD/$0;; esac
_PWD=$PWD; cd "${cpath%/*}/.."; cpath=$PWD; cd "$_PWD"

[ -f "$cpath/conf/skale-env.sh" ] && . "$cpath/conf/skale-env.sh"

# Stop skale server
host=${SKALE_HOST:-localhost}
printf "Stopping skale server on $host (clients will stop) ... "
ssh $host 'test -f '$tmp'/skale.pid && kill $(cat '$tmp'/skale.pid); rm -f '$tmp'/skale.pid'
echo ok
