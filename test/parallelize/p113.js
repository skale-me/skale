#!/usr/local/bin/node --harmony

// parallelize -> intersection -> collect
// parallelize -> 

var co = require('co');
var ugrid = require('../../');
var intersection = require('../ugrid-test.js').intersection;

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var v1 = [1, 2, 3];
	var v2 = [3, 4, 5];

	var loc = intersection(v1, v2);
	var d1 = uc.parallelize(v1);
	var d2 = uc.parallelize(v2);

	var dist = yield d1.intersection(d2).collect();

	loc = loc.sort();
	dist = dist.sort();

	console.assert(JSON.stringify(loc)== JSON.stringify(dist))

	uc.end();
}).catch(ugrid.onError);
