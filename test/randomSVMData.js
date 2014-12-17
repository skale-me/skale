#!/usr/local/bin/node --harmony

var co = require('co');
var ugrid = require('../lib/ugrid-context.js')({host: 'localhost', port: 12346});

co(function *() {
	yield ugrid.init();

	var N = 5;
	var D = 2;
	var seed = 1;
	var P = process.argv[2];
	var res = yield ugrid.randomSVMData(N, D, seed, P).collect();

	if (res.length != N)
		throw test + ' error: bad array length'

	for (var i = 0; i < N; i++) {
		if (res[i].label == undefined)
			throw 'error: no label found'
		if (res[i].features == undefined)
			throw 'error: no features found'
		if ((res[i].label != -1) && (res[i].label != 1))
			throw 'error: bad label value'
		if (res[i].features.length != D)
			throw 'error: bad features length'
	}

	ugrid.end();
})();