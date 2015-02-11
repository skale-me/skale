#!/usr/local/bin/node --harmony

var co = require('co');
var assert = require('assert');
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
	assert(r1.length == r2.length);
	for (var i = 0; i < r1.length; i++)
		assert(r1[i] == r2[i]);

	// union with other array
	var d1 = ugrid.parallelize(a);
	var d2 = ugrid.parallelize(b);
	var d3 = d1.union(d2);

	// var r1 = yield d1.collect();
	// console.log(r1);

	// var r2 = yield d2.collect();
	// console.log(r2);

	var r3 = yield d3.collect();
	console.log(r3);

	ugrid.end();
})();
