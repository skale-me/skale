#!/usr/local/bin/node --harmony

var co = require('co');
var ugrid = require('../lib/ugrid-context.js')({host: 'localhost', port: 12346});

co(function *() {
	yield ugrid.init();

	var N = 10;

	var local = new Array(N);
	for (var i = 0; i < N; i++)
		local[i] = i;

	var t0 = ugrid.parallelize(local);
	var r0 = yield t0.collect();

	console.log('local array :')
	console.log(local)

	console.log('\ndistributed array :')
	console.log(r0)

	ugrid.end();
})();
