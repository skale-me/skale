#!/usr/local/bin/node --harmony

var co = require('co');
var ugrid = require('../..');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	process.exit(0);
	//uc.end();
}).catch(ugrid.onError);
