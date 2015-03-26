#!/usr/local/bin/node --harmony
'use strict'

var co = require('co');
var thenify = require('thenify');
var exec = thenify(require('child_process').exec);

var ugrid = require('../../../ugrid/lib/ugrid-client.js')();

// var args = JSON.parse(process.argv[2]);

// var N = args.n;
// var K = args.k;
// var ITERATIONS = args.it;

co(function *() {
	var ui = yield ugrid.devices({type: 'webugrid'}, 0);

	var file = '/tmp/logreg244MB.data';
	// var file = '/tmp/test.data';	
	var time = [];
	var nIterations = 1;
	var nMaxWorker = 8;

	run(1, function() {
		ugrid.end();
	});

	function run(nWorker, callback) {
		var cmd = 'node --harmony ../../examples/ml/logreg-file.js ' + file + ' ' + nIterations + ' ' + nWorker;
		var startTime = new Date();
		exec(cmd, function (err, stdout, stderr) {
			if (err) throw 'error'
			time.push([nWorker, (new Date() - startTime) / 1000]);
			// Send source data to UI
			ugrid.send(ui[0].uuid, {cmd: 'benchmark', data: {time: time}});
			if (nWorker < nMaxWorker) run(++nWorker, callback);
			else callback();
		});
	}
}).catch(function (err) {
	console.error(err.stack);
	process.exit(1);
});


