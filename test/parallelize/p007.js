#!/usr/local/bin/node --harmony

// parallelize -> persist -> lookup

var co = require('co');
var ugrid = require('../../');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var key = 1;
	var value = 2;	
	var v = [[key, value], [3, 4], [5, 6]];
	var loc = v.filter(function(e) {return (e[0] == key)});

	var data = uc.parallelize(v).persist();
	yield data.lookup(key);
	v.push([key, value]);
	var dist = yield data.lookup(key);

	console.assert(JSON.stringify(loc) == JSON.stringify(dist));

	uc.end();
}).catch(ugrid.onError);
