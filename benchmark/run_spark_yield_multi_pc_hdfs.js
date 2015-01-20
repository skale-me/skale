#! /usr/local/bin/node --harmony

var co = require('co');
var fs = require('fs');
var thunkify = require('thunkify');
var execCbk = require('child_process').exec;

var exec = thunkify(execCbk);

var SPARK_HOME = process.argv[2];
if (process.argv[2] == undefined)
	throw 'You must provide SPARK_HOME directory path'

var EXAMPLE_DIR = SPARK_HOME + '/examples/src/main/python/mllib/';
//~ var IP = '127.0.0.1';
var IP = '192.168.1.29';
var PORT = '7077';
var FILE = 'logistic_regression_optimise.py';
var SOURCE_DATA_FILE = '/gs/ugrid/work/x86_64/data.txt';
//~ var LOCAL_DATA_FILE = '/tmp/data3.txt';
//~ var LOCAL_DATA_FILE = '/tmp/data.txt';
//~ var LOCAL_DATA_FILE = '/tmp/bigdata.txt';
//~ LOCAL_DATA_FILE = '/tmp/hugedata.txt';
var HDFS_MASTER_IP = '192.168.1.30'
var HDFS_PORT = '9000'
var HDFS_FILE = 'hdfs://' + HDFS_MASTER_IP + ':'+ HDFS_PORT +'/user/ugrid/input/4Gdata.txt'
var N_ITERATIONS = process.argv[3] || 1;
var WORKER_CORES = 4;
var WORKER_INSTANCES = 1; 
var MAX_CORES = 4;
var MAX_PC = 2;
var slave_ip= "";
co(function *() {
	for (var NB_PC = 2; NB_PC <= MAX_PC; NB_PC++) {
		//~ console.log('WORKER_CORES = ' + WORKER_CORES);
		//~ console.log('Writing spark env file');
		//~ // Configure spark environment variables
		//~ var spark_env = '#!/usr/bin/env bash\n\n' + 
			//~ 'export SPARK_MASTER_IP=' + IP + '\n' + 
			//~ 'export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk-amd64\n' +
			//~ 'export SPARK_EXECUTOR_MEMORY=2G\n' +			
			//~ 'export SPARK_WORKER_CORES=' + WORKER_CORES + '\n' + 
			//~ 'export SPARK_WORKER_INSTANCES=' + WORKER_INSTANCES + '\n';
//~ 
		//~ fs.writeFileSync(SPARK_HOME + '/conf/spark-env.sh', spark_env, {encoding: 'utf8'}, function(err, res) {
			//~ if (err) throw 'Cannot write spark-env.sh file';
		//~ })
//~ 
		//~ // Edit slaves ip in spark cluster config file
		//~ console.log('Writing spark slaves file');
		//~ slave_ip =  slave_ip + 'pc' + NB_PC + '\n';
		//~ fs.writeFileSync(SPARK_HOME + '/conf/slaves', slave_ip, {encoding: 'utf8'}, function(err, res) {
			//~ if (err) throw 'Cannot write /conf/slaves file';
		//~ })

		// Copy data file to /tmp directory only if needed
		//~ console.log('Looking for input data file');
		//~ var cmd = 'if [ ! -f ' + LOCAL_DATA_FILE + ' ];then cp ' + SOURCE_DATA_FILE + ' ' + LOCAL_DATA_FILE + '; fi';
		//~ yield exec(cmd);
		
		// Copy data file to distant PC only if needed
		//~ if (NB_PC > 1) {
			//~ console.log('SCP input data file to slaves');	
			//~ var cmd = 'scp ' + LOCAL_DATA_FILE + ' ugrid@pc' + NB_PC + ":" + LOCAL_DATA_FILE;
			//~ yield exec(cmd);
		//~ }
		// Start Spark master and workers
		console.log('Starting Spark Cluster');
		yield exec(SPARK_HOME + '/sbin/start-all.sh');

		// Execute test file
		console.log('Executing test file');
		var cmd = SPARK_HOME + '/bin/spark-submit --master spark://' + IP + ':' + PORT + ' ' + EXAMPLE_DIR + FILE + ' ' + HDFS_FILE + ' ' + N_ITERATIONS + ' 2> /dev/null';
		var startTime = new Date();
		var res = yield exec(cmd);
		console.log('Elapsed Time : ' + ((new Date() - startTime) / 1000));
		console.log('Weights: ' + res[0]);

		console.log('Stopping cluster');
		yield exec(SPARK_HOME + '/sbin/stop-all.sh');

		yield exec('sleep 3');
	}
})();
