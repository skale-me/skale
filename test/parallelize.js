#!/usr/local/bin/node --harmony

var co = require('co');
var fs = require('fs');
var ugrid = require('../lib/ugrid-context.js')({host: 'localhost', port: 12346});
var ml = require('../lib/ugrid-ml.js');

co(function *() {
	yield ugrid.init();

	var N = 5;
	var seed = 1234;
	var rng = new ml.Random(seed);
	var V = rng.randn(N);

	console.error(V);
	
	var res = yield ugrid.parallelize(V).collect();

	console.log(res);

	ugrid.end();
})();