#!/usr/local/bin/node --harmony

// parallelize -> union -> collect
// parallelize ->

var co = require('co');
var ugrid = require('../../');
var union = require('../ugrid-test.js').union;

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var v1 = [1, 2, 3, 4, 5];
	var v2 = [6, 7, 8, 9, 10];
	var loc = union(v1, v2);

	var d1 = uc.parallelize(v1);
	var d2 = uc.parallelize(v2);

	var dist = yield d1.union(d2).collect();

	loc = loc.sort();
	dist = dist.sort();

	for (var i = 0; i < loc.length; i++)
		console.assert(loc[i] == dist[i]);

	uc.end();
}).catch(ugrid.onError);
