#!/usr/local/bin/node --harmony

// parallelize -> distinct -> count

var co = require('co');
var ugrid = require('../../');
var distinct = require('../ugrid-test.js').distinct;

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var v = [3, 2, 1, 3];

	var loc = distinct(v);
	var dist = yield uc.parallelize(v).distinct().count();

	console.log(loc)
	console.log(dist)

	console.assert(loc.length == dist)

	uc.end();
}).catch(ugrid.onError);
