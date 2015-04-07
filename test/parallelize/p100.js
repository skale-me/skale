#!/usr/local/bin/node --harmony

// parallelize -> coGroup -> collect
// parallelize -> 

var co = require('co');
var ugrid = require('../../');
var coGroup = require('../ugrid-test.js').coGroup;

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var v1 = [[0, 1], [1, 2], [2, 3], [2, 4], [5, 5]];
	var v2 = [[0, 5], [1, 6], [2, 7], [3, 8], [0, 9]];
	var loc = coGroup(v1, v2);

	var d1 = uc.parallelize(v1);
	var d2 = uc.parallelize(v2);
	var dist = yield d1.coGroup(d2).collect();

	loc = loc.sort();
	dist = dist.sort();

	console.log(loc)
	console.log(dist)
	console.assert(loc.length == dist.length)
	for (var i = 0; i < loc.length; i++) {
		console.assert(loc[i][0] == dist[i][0]);
		for (var j = 0; j < loc[i][1].length; j++) {
			loc[i][1][j] = loc[i][1][j].sort();
			dist[i][1][j] = dist[i][1][j].sort();
			for (var k = 0; k < loc[i][1][j].length; k++)
				console.assert(loc[i][1][j][k] == dist[i][1][j][k]);
		}
	}

	uc.end();
}).catch(ugrid.onError);
