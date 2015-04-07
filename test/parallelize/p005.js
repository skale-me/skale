#!/usr/local/bin/node --harmony

// parallelize -> persist -> collect

var co = require('co');
var ugrid = require('../../');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var v = [1, 2, 3, 4, 5];
	var loc = JSON.parse(JSON.stringify(v));

	var data = uc.parallelize(v).persist();
	yield data.collect();
	v.push(6);
	var dist = yield data.collect();

	console.assert(JSON.stringify(loc) == JSON.stringify(dist));

	uc.end();
}).catch(ugrid.onError);
