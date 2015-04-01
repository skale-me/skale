#!/usr/local/bin/node --harmony

// parallelize -> crossProduct -> count
// parallelize -> 

var co = require('co');
var ugrid = require('../../');
var crossProduct = require('../ugrid-test.js').crossProduct;

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var v1 = [1, 2, 3, 4, 5];
	var v2 = [6, 7, 8, 9, 10];
	var loc = crossProduct(v1, v2);

	var d1 = uc.parallelize(v1);
	var d2 = uc.parallelize(v2);

	var dist = yield d1.crossProduct(d2).collect();

	loc = loc.sort();
	dist = dist.sort();

	for (var i = 0; i < loc.length; i++) {
		console.assert(loc[i][0] == dist[i][0]);
		for (var j = 0; j < loc[i][1].length; j++)
			console.assert(loc[i][1][j] == dist[i][1][j]);
	}

	uc.end();
}).catch(ugrid.onError);
