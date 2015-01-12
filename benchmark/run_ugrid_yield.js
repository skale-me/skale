#! /usr/local/bin/node --harmony

var co = require('co');
var thunkify = require('thunkify');
var execCbk = require('child_process').exec;

var exec = thunkify(execCbk);

var UGRID_HOME = process.argv[2];
if (process.argv[2] == undefined)
	throw 'You must provide UGRID_HOME directory path'

var EXAMPLE_DIR = UGRID_HOME + '/benchmark/';
var FILE = 'logreg-textFile.js';
var SOURCE_DATA_FILE = '/gs/ugrid/work/x86_64/data.txt';
var LOCAL_DATA_FILE = '/tmp/data.txt';
var N_ITERATIONS = process.argv[3] || 1;

co(function *() {
	// Copy data file to /tmp directory only if needed
	var cmd = 'if [ ! -f ' + LOCAL_DATA_FILE + ' ];then cp ' + SOURCE_DATA_FILE + ' ' + LOCAL_DATA_FILE + '; fi';
	yield exec(cmd);

	// Start Spark master and workers
	var cmd = UGRID_HOME + '/bin/start-all.sh ' + UGRID_HOME + '/bin';
	yield exec(cmd);

	// Execute test file
	var cmd = EXAMPLE_DIR + FILE + ' ' + LOCAL_DATA_FILE + ' ' + N_ITERATIONS + ' 2> /dev/null';
	var startTime = new Date();
	var res = yield exec(cmd);	

	console.log('Elapsed Time : ' + ((new Date() - startTime) / 1000));
	console.log('Weights: ' + res[0]);

	yield exec(UGRID_HOME + '/bin/stop-all.sh');
})();