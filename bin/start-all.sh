#!/bin/bash

# Compute canonical path to retrieve all conf/exec files from it
case $0 in (/*) cpath=$0 ;; (*) cpath=$PWD/$0 ;; esac
cpath=${cpath/.\//}; cpath=${cpath%/*/*}

# Read user specific configuration
[ -f "$cpath/conf/ugrid-env.sh" ] && . "$cpath/conf/ugrid-env.sh"
[ -f "$cpath/conf/slaves" ] && workers=$(<$cpath/conf/slaves) || workers=localhost

host=${UGRID_HOST:-localhost} port=${UGRID_PORT:-12346} wph=${UGRID_WORKER_PER_HOST:-4}

# Start ugrid server
ugrid_cmd="$cpath/bin/ugrid.js >/tmp/ugrid.log 2>&1 </dev/null & echo \$! >/tmp/ugrid.pid"
ssh $host "$ugrid_cmd"

# Wait for ugrid server
while true; do nc -z $host $port >/dev/null && break || sleep 1; done

# Start ugrid workers
worker_cmd="$cpath/bin/worker.js -H $host -P $port -n $wph >/tmp/worker.log 2>&1 &"
set -- $workers
for worker; do
	ssh $worker "$worker_cmd"
done

# Wait ugrid workers
$cpath/bin/wait-workers.js $(($wph * $#))
