#!/bin/sh

unset CDPATH
export LC_ALL=C IFS=' 	
'

die() { echo "$0: fatal: $@" >&2; exit 1; }

# Compute canonical path to retrieve all conf/exec files from it
case $0 in (/*) cpath=$0;; (*) cpath=$PWD/$0;; esac
_PWD=$PWD; cd "${cpath%/*}/.."; cpath=$PWD; cd "$_PWD"

config=$cpath/conf/ugrid-env.sh
slaves=$cpath/conf/slaves

# Command line options
while getopts :c:s: opt
do
	case $opt in
	(c) config=$PWD/$OPTARG; [ -f "$config" ] || die "$config not found" ;;
	(s) slaves=$PWD/$OPTARG; [ -f "$slaves" ] || die "$slaves not found" ;;
	(*) echo "Usage: $0 [-c config_file] [-s slave_file]"; exit 1 ;;
	esac
done
shift $(($OPTIND - 1))

# Read user specific configuration
[ -f "$config" ] && . "$config"
[ -f "$slaves" ] && workers=$(cat "$slaves") || workers=localhost

host=${UGRID_HOST:-localhost} port=${UGRID_PORT:-12346}
node=${NODE:-node} node_opts=${NODE_OPTS}

tmp=/tmp/$USER
[ -d "$tmp" ] || mkdir -p $tmp

# Start ugrid server
printf "Starting ugrid server on $host port $port ... "
ugrid_cmd="PATH=$PATH; [ -f $config ] && . $config; $node $node_opts $cpath/bin/ugrid.js >>$tmp/ugrid.log 2>&1 & echo \$! >$tmp/ugrid.pid"
ssh $host "$ugrid_cmd"

# Wait for ugrid server
while ! $cpath/bin/wait-workers.js 0 2>/dev/null; do sleep 1; done && echo "ok"

# Start ugrid workers
printf "Starting worker on $host ... "
worker_cmd="PATH=$PATH; [ -f $config ] && . $config; $node $node_opts $cpath/bin/worker.js -H $host -P $port >>$tmp/worker.log 2>&1 &"
set -- $workers
for worker; do
	ssh $worker "$worker_cmd"
done

# Wait for ugrid workers
$cpath/bin/wait-workers.js $# && echo "ok"

# Start ugrid controller
printf "Starting controller on $host ... "
controller_cmd="PATH=$PATH; [ -f $config ] && . $config; $node --harmony $node_opts $cpath/bin/controller.js -H $host -P $port >>$tmp/controller.log 2>&1 &"
ssh $host "$controller_cmd"

# Wait for ugrid controller
$cpath/bin/wait-controller.js && echo "ok"
