#! /bin/sh

SPARK_HOME=/gs/ugrid/work/x86_64/spark/spark-1.1.1-bin-hadoop1
EXAMPLE_DIR=$SPARK_HOME/examples/src/main/python/mllib/
IP=192.168.1.30
PORT=7077
FILE=logistic_regression_optimise.py
SOURCE_DATA_FILE=/gs/ugrid/work/x86_64/data.txt
LOCAL_DATA_FILE=/tmp/data.txt
N_ITERATIONS=100

echo $IP > $SPARK_HOME/conf/slaves

# Copy data file to /tmp directory only if needed
if [ ! -f $LOCAL_DATA_FILE ];then
	cp $SOURCE_DATA_FILE $LOCAL_DATA_FILE;
fi

# Start Spark master and workers
$SPARK_HOME/sbin/start-all.sh

# Evaluate time to execute FILE
ts=$(date +%s%N)
$SPARK_HOME/bin/spark-submit --master spark://$IP:$PORT $EXAMPLE_DIR/$FILE $LOCAL_DATA_FILE $N_ITERATIONS 2> /dev/null
tt=$((($(date +%s%N) - $ts)/1000000))
echo "Time taken: $tt"

# Stop Spark server
$SPARK_HOME/sbin/stop-all.sh