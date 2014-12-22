#!/usr/local/bin/node --harmony

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')({host: 'localhost', port: 12346});
var ml = require('../../lib/ugrid-ml.js');

co(function *() {
	yield ugrid.init();

	var a = [1, 2];
	var b = [3, 4];

	var d1 = ugrid.parallelize(a);
	var d2 = ugrid.parallelize(b);

	// Reflexive union test
	var r1 = yield d1.collect();
	var r2 = yield d1.union(d1).collect();

	if (r1.length != r2.length)
		throw 'error: r1 and r3 have different lengths';

	for (var i = 0; i < r1.length; i++)
		if (r1[i] != r3[i])
			throw 'error: r1 and r3 have different elements';

	// union with other array
	var d3 = d1.union(d2);
	var r3 = d3.union(d2).collect();

	throw 'ADD OUTPUT TEST'

	ugrid.end();
})();
