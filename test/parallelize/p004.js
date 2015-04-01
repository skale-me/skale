#!/usr/local/bin/node --harmony

// parallelize -> persist -> count

var co = require('co');
var ugrid = require('../../');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var v = [1, 2, 3, 4, 5];
	var loc = v.length;

	var data = uc.parallelize(v).persist();
	yield data.count();
	v.push(6);
	var dist = yield data.count();

	console.assert(loc == dist);

	uc.end();
}).catch(ugrid.onError);
