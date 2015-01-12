#!/bin/bash
i=0 n=$2

cd $1
./ugrid.js >/dev/null 2>&1 &
echo $! >/tmp/ugrid.pid
sleep 1

>/tmp/x
./worker.js -n $n >>/tmp/x 2>&1 &

while [ $i -le $n ]
do
	read X </tmp/x && echo $X
	i=$(($i + 1))
done
sleep 2
echo UGRID READY
