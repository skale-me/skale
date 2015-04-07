#!/usr/local/bin/node --harmony

// parallelize -> sortByKey -> collect

var co = require('co');
var ugrid = require('../../');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var v = [["z", 2], ["a", 1], ["t", 3]];

	var loc = v.sort();
	var dist = yield uc.parallelize(v).sortByKey().collect();

	console.assert(JSON.stringify(loc) == JSON.stringify(dist))

	uc.end();
}).catch(ugrid.onError);
