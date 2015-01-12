#! /usr/bin/nodejs

var exec = require('child_process').exec;

var UGRID_HOME = '/gs/ugrid/work/x86_64/ugrid-150105/ugrid/';
var EXAMPLE_DIR = UGRID_HOME + '/benchmark/';
var IP = 'localhost';
var PORT = '12346';
var FILE = 'logreg-textFile.js';
var SOURCE_DATA_FILE = '/gs/ugrid/work/x86_64/data.txt';
var LOCAL_DATA_FILE = '/tmp/data.txt';
var N_ITERATIONS = process.argv[2] || 1;

// Edit slaves ip in spark cluster config file
var cmd = 'echo ' + IP + ' > ' + UGRID_HOME + '/conf/slaves';
exec(cmd, function(error, stdout, stderr) {
	// Copy data file to /tmp directory only if needed
	var cmd = 'if [ ! -f ' + LOCAL_DATA_FILE + ' ];then cp ' + SOURCE_DATA_FILE + ' ' + LOCAL_DATA_FILE + '; fi';
	exec(cmd, function(error, stdout, stderr) {
		// Start Spark master and workers
		var cmd = UGRID_HOME + '/bin/start-all.sh';
		exec(cmd, function(error, stdout, stderr) {
			var cmd = EXAMPLE_DIR + FILE + ' ' + LOCAL_DATA_FILE + ' ' + N_ITERATIONS + ' 2> /dev/null';
			var startTime = new Date();
			exec(cmd, function(error, stdout, stderr) {
				var elapsedTime = (new Date() - startTime) / 1000;
				console.log('Weights: ' + stdout);
				console.log('Elapsed Time : ' + elapsedTime);
				var cmd = UGRID_HOME + '/bin/stop-all.sh';
				exec(cmd, function(error, stdout, stderr) {});
			});
		});
	});
});
