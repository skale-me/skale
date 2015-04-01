#!/usr/local/bin/node --harmony

// parallelize -> takeSample

var co = require('co');
var ugrid = require('../../');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var v = [1, 2, 3, 4, 5];
	var withReplacement = false;
	var seed = 1;
	var dist = yield uc.parallelize(v).takeSample(withReplacement, 3, seed);

	console.log(dist);

	uc.end();
}).catch(ugrid.onError);
