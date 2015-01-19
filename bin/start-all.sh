#!/bin/sh

unset CDPATH
export LC_ALL=C IFS=' 	
'
# Compute canonical path to retrieve all conf/exec files from it
case $0 in (/*) cpath=$0;; (*) cpath=$PWD/$0;; esac
_PWD=$PWD; cd "${cpath%/*}/.."; cpath=$PWD; cd "$_PWD"

# Read user specific configuration
[ -f "$cpath/conf/ugrid-env.sh" ] && . "$cpath/conf/ugrid-env.sh"
[ -f "$cpath/conf/slaves" ] && workers=$(cat $cpath/conf/slaves) || workers=localhost

host=${UGRID_HOST:-localhost} port=${UGRID_PORT:-12346} wph=${UGRID_WORKER_PER_HOST:-4}

# Start ugrid server
ugrid_cmd="$cpath/bin/ugrid.js >/tmp/ugrid.log 2>&1 </dev/null & echo \$! >/tmp/ugrid.pid"
ssh $host "$ugrid_cmd"

# Wait for ugrid server
while true; do nc -z $host $port >/dev/null && break || sleep 1; done

# Start ugrid workers
worker_cmd=". $cpath/conf/ugrid-env.sh; $cpath/bin/worker.js -H $host -P $port -n $wph >/tmp/worker.log 2>&1 &"
set -- $workers
for worker; do
	ssh $worker "$worker_cmd"
done

# Wait for ugrid workers
$cpath/bin/wait-workers.js $(($wph * $#))
