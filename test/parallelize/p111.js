#!/usr/local/bin/node --harmony

// parallelize -> distinct -> lookup

var co = require('co');
var ugrid = require('../../');
var distinct = require('../ugrid-test.js').distinct;

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var v = [[3, 1], [2, 1], [1, 4], [3, 1]];
	var key = 3;

	var loc = distinct(v).filter(function(e) {return (e[0] == key);});
	var dist = yield uc.parallelize(v).distinct().lookup(key);

	loc = loc.sort();
	dist = dist.sort();
	console.assert(JSON.stringify(loc) == JSON.stringify(dist))

	uc.end();
}).catch(ugrid.onError);
