#!/usr/local/bin/node --harmony

var co = require('co');
var assert = require('assert');
var ugrid = require('../..');
var ml = require('../../lib/ugrid-ml.js');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var a = [1, 2];
	var b = [3, 4];

	var d1 = uc.parallelize(a);
	var d2 = uc.parallelize(b);

	// // Reflexive union test
	var r1 = yield d1.collect();
	var r2 = yield d1.union(d1).collect();
	assert(r1.length == r2.length);
	for (var i = 0; i < r1.length; i++)
		assert(r1[i] == r2[i]);

	// union with other array
	var d1 = uc.parallelize(a);
	var d2 = uc.parallelize(b);
	var d3 = d1.union(d2);
	var r3 = yield d3.collect();

	var c = a.concat(b);
	assert(r3.length == c.length);

	for (var i = 0; i < r3.length; i++)
		assert(c.indexOf(r3[i]) != -1)
	
	uc.end();
}).catch(ugrid.onError);
