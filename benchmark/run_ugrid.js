#! /usr/local/bin/node --harmony

var co = require('co');
var thunkify = require('thunkify');
var execCbk = require('child_process').exec;

var exec = thunkify(execCbk);

if (process.argv.length != 5) {
	console.log('Usage: run_ugrid.js ugridHomeDir dataFile nIterations\n\t==> You must provide absolute path')
	process.exit(1);
}

var UGRID_HOME = process.argv[2];
var SOURCE_DATA_FILE = process.argv[3];
var N_ITERATIONS = process.argv[4];

var BIN = 'logreg-textFile.js';
var TMP_FILE = '/tmp/' + require('path').basename(SOURCE_DATA_FILE);
var MAX_CORES = 4;

var cp_cmd = 'if [ ! -f ' + TMP_FILE + ' ];then cp ' + SOURCE_DATA_FILE + ' ' + TMP_FILE + '; fi';
var exec_cmd = UGRID_HOME + '/examples/' + BIN + ' ' + TMP_FILE + ' ' + N_ITERATIONS + ' 2> /dev/null';

co(function *() {
	console.log('Ugrid home : ' + UGRID_HOME);	
	console.log('Binary file : ' + UGRID_HOME + 'examples/' + BIN);
	console.log('Data file : ' + SOURCE_DATA_FILE);
	console.log('Local data file : ' + TMP_FILE);
	console.log('Iterations : ' + N_ITERATIONS);
	// Copy file if needed
	yield exec(cp_cmd);

	// Loop over number of cores
	for (var i = 1; i <= MAX_CORES; i++) {

		var cache_cmd = 'sh -c "echo 3 >/proc/sys/vm/drop_caches"';
		yield exec(cache_cmd);

		console.log('\nNumber of cores : ' + i);
		console.log('Start Ugrid cluster');
		var start_cmd = UGRID_HOME + '/bin/start-all.sh ' + UGRID_HOME + '/bin ' + i;
		yield exec(start_cmd);

		console.log('Run binary');
		var startTime = new Date();
		var res = yield exec(exec_cmd);
		console.log('Elapsed Time : ' + ((new Date() - startTime) / 1000));
		console.log('Output: ' + res[0]);

		console.log('Stop Ugrid Cluster');
		yield exec(UGRID_HOME + '/bin/stop-all.sh');

		yield exec('sleep 3');
	}
})();
