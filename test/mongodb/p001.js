#!/usr/local/bin/node --harmony

// mongo -> collect

var co = require('co');
var ugrid = require('../..');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var dist = yield uc.mongo({v : {$gt: 1}}).collect();

	// var dist = yield uc.mongo().collect();
	console.log(dist);

	uc.end();
}).catch(ugrid.onError);
