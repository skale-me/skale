#! /usr/local/bin/node

var exec = require('child_process').exec;

var UGRID_HOME = process.argv[2];
if (process.argv[2] == undefined)
	throw 'You must provide UGRID_HOME directory path'

var EXAMPLE_DIR = UGRID_HOME + '/benchmark/';
var FILE = 'logreg-textFile.js';
var SOURCE_DATA_FILE = '/gs/ugrid/work/x86_64/data.txt';
var LOCAL_DATA_FILE = '/tmp/data.txt';
var N_ITERATIONS = process.argv[3] || 1;

// Copy data file to /tmp directory only if needed
var cmd = 'if [ ! -f ' + LOCAL_DATA_FILE + ' ];then cp ' + SOURCE_DATA_FILE + ' ' + LOCAL_DATA_FILE + '; fi';
exec(cmd, function(error, stdout, stderr) {
	// Start Spark master and workers
	var cmd = UGRID_HOME + '/bin/start-all.sh ' + UGRID_HOME + '/bin';
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
