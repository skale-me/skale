#!/usr/local/bin/node --harmony

// paralelize --> lookup

var co = require('co');
var ugrid = require('../../');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var key = 1;
	var v = [[1, 1], [1, 2], [2, 3], [2, 4], [3, 5]];

	var loc = v.filter(function (e) {return (e[0] == key)});
	var dist = yield uc.parallelize(v).lookup(key);

	console.assert(JSON.stringify(loc) == JSON.stringify(dist));

	uc.end();
}).catch(ugrid.onError);
