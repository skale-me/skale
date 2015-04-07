#!/usr/local/bin/node --harmony

// parallelize -> values -> collect

var co = require('co');
var ugrid = require('../../');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var v = [[1, 2], [3, 4], [3, 6]];

	var loc = v.map(function(e){return e[1]});

	var dist = yield uc.parallelize(v).values().collect();

	loc = loc.sort();
	dist = dist.sort();

	console.assert(JSON.stringify(loc) == JSON.stringify(dist));

	uc.end();
}).catch(ugrid.onError);
