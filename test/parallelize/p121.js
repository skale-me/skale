#!/usr/local/bin/node --harmony

// parallelize -> forEach

var co = require('co');
var ugrid = require('../../');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	function each(e) {
		console.log(e);
	}

	var v = [1, 2, 3, 3];
	var dist = yield uc.parallelize(v).forEach(each);

	uc.end();
}).catch(ugrid.onError);
