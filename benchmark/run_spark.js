#! /usr/bin/nodejs

var exec = require('child_process').exec;

var SPARK_HOME = '/gs/ugrid/work/x86_64/spark/spark-1.1.1-bin-hadoop1';
var EXAMPLE_DIR = SPARK_HOME + '/examples/src/main/python/mllib/';
var IP = '192.168.1.30';
var PORT = '7077';
var FILE = 'logistic_regression_optimise.py';
var SOURCE_DATA_FILE = '/gs/ugrid/work/x86_64/data.txt';
var LOCAL_DATA_FILE = '/tmp/data.txt';
var N_ITERATIONS = process.argv[2] || 1;

// Edit slaves ip in spark cluster config file
var cmd = 'echo ' + IP + ' > ' + SPARK_HOME + '/conf/slaves';
exec(cmd, function(error, stdout, stderr) {
	// Copy data file to /tmp directory only if needed
	var cmd = 'if [ ! -f ' + LOCAL_DATA_FILE + ' ];then cp ' + SOURCE_DATA_FILE + ' ' + LOCAL_DATA_FILE + '; fi';
	exec(cmd, function(error, stdout, stderr) {
		// Start Spark master and workers
		var cmd = SPARK_HOME + '/sbin/start-all.sh';
		exec(cmd, function(error, stdout, stderr) {
			var cmd = SPARK_HOME + '/bin/spark-submit --master spark://' + IP + ':' + PORT + ' ' + EXAMPLE_DIR + FILE + ' ' + LOCAL_DATA_FILE + ' ' + N_ITERATIONS + ' 2> /dev/null';
			var startTime = new Date();
			exec(cmd, function(error, stdout, stderr) {
				var elapsedTime = (new Date() - startTime) / 1000;
				console.log('Weights: ' + stdout);
				console.log('Elapsed Time : ' + elapsedTime);
				var cmd = SPARK_HOME + '/sbin/stop-all.sh';
				exec(cmd, function(error, stdout, stderr) {});
			});
		});
	});
});
