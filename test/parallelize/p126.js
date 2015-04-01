#!/usr/local/bin/node --harmony

// parallelize -> leftOuterJoin -> collect
// parallelize -> 

var co = require('co');
var ugrid = require('../../');
var join = require('../ugrid-test.js').join;

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var v1 = [[1, 2], [3, 4], [3, 6]];
	var v2 = [[3, 9]];
	var loc = join(v1, v2, 'left');

	var d1 = uc.parallelize(v1);
	var d2 = uc.parallelize(v2);

	var dist = yield d1.leftOuterJoin(d2).collect();

	console.log(loc)
	console.log(dist)

	loc = loc.sort();
	dist = dist.sort();
	for (var i = 0; i < loc.length; i++) {
		console.assert(loc[i][0] == dist[i][0]);
		for (var j = 0; j < loc[i][1].length; j++)
			console.assert(loc[i][1][j] == dist[i][1][j]);
	}

	uc.end();
}).catch(ugrid.onError);
