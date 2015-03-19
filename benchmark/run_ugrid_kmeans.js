#! /usr/local/bin/node --harmony

var co = require('co');
var fs = require('fs');
var thenify = require('thenify');
var execCbk = require('child_process').exec;

var exec = thenify(execCbk);

if (process.argv.length != 6) {
	console.log('Usage: run_ugrid_kmeans.js ugridHomeDir dataFile K nIterations\n\t==> You must provide absolute path')
	process.exit(1);
}

var UGRID_HOME = process.argv[2];
var SOURCE_DATA_FILE = process.argv[3];
var K = process.argv[4];
var N_ITERATIONS = process.argv[5];

var BIN = 'kmeans-textFile.js';
var TMP_FILE = '/tmp/' + require('path').basename(SOURCE_DATA_FILE);
var MAX_CORES = 4;

var cp_cmd = 'if [ ! -f ' + TMP_FILE + ' ];then cp ' + SOURCE_DATA_FILE + ' ' + TMP_FILE + '; fi';
var exec_cmd = UGRID_HOME + '/examples/' + BIN + ' ' + TMP_FILE + ' ' + K + ' ' + N_ITERATIONS + ' 2> /dev/null';

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
		console.log('\nNumber of cores : ' + i);
		console.log('Write ugrid-env.sh');
		var spark_env = '#!/usr/bin/env bash\n\n' + 
			'export UGRID_WORKER_PER_HOST=' + i + '\n';
		fs.writeFileSync(UGRID_HOME + '/conf/ugrid-env.sh', spark_env, {encoding: 'utf8'}, function(err, res) {
			if (err) throw 'Cannot write ugrid-env.sh file';
		})
		console.log('Start Ugrid cluster');
		yield exec(UGRID_HOME + '/bin/start-all.sh');

		console.log('Run binary');
		var startTime = new Date();
		var res = yield exec(exec_cmd);
		console.log('Elapsed Time : ' + ((new Date() - startTime) / 1000));
		console.log('Output: ' + res[0]);

		console.log('Stop Ugrid Cluster');
		yield exec(UGRID_HOME + '/bin/stop-all.sh');

		yield exec('sleep 3');
	}
}).catch(function (err) {
	console.error(err.stack);
	process.exit(1);
});
