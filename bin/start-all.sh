#!/bin/bash
i=0 n=1

cd /gs/ugrid/work/x86_64/ugrid-150105/ugrid/bin
./ugrid.js >/dev/null 2>&1 &
echo $! >/tmp/ugrid.pid
sleep 1

>/tmp/x
./worker.js >>/tmp/x 2>&1 &
./worker.js >>/tmp/x 2>&1 &

while [ $i -le $n ]
do
	read X </tmp/x && echo $X
	i=$(($i + 1))
done
sleep 12
echo UGRID READY
