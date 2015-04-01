#!/usr/local/bin/node --harmony

var co = require('co');
var ugrid = require('../..');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var N = 5;
	var D = 2;
	var seed = 1;
	var res = yield uc.randomSVMData(N, D, seed).collect();

	console.assert(res.length == N);

	for (var i = 0; i < N; i++)
		console.assert(res[i].length == (D + 1));

	uc.end();
}).catch(ugrid.onError);
