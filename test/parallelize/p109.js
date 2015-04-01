#!/usr/local/bin/node --harmony

// parallelize -> distinct -> collect

var co = require('co');
var ugrid = require('../../');
var distinct = require('../ugrid-test.js').distinct;

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var v = [3, 2, 1, 3];

	var loc = distinct(v);
	var dist = yield uc.parallelize(v).distinct().collect();

	loc = loc.sort();
	dist= dist.sort();

	console.assert(JSON.stringify(loc) == JSON.stringify(dist))

	uc.end();
}).catch(ugrid.onError);
