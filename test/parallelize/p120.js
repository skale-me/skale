#!/usr/local/bin/node --harmony

// parallelize -> countByValue

var co = require('co');
var ugrid = require('../../');
var countByValue = require('../ugrid-test.js').countByValue;

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var v = [1, 2, 3, 3];
	var loc = countByValue(v);
	var dist = yield uc.parallelize(v).countByValue();

	console.log(loc);
	console.log(dist)

	loc = loc.sort();
	dist = dist.sort();

	// console.assert(JSON.stringify(loc) == JSON.stringify(dist))

	uc.end();
}).catch(ugrid.onError);
