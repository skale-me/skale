#!/usr/local/bin/node --harmony

// parallelize -> forEach

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	function each(e) {
		console.log(e);
	}

	var v = [1, 2, 3, 3];
	var dist = yield ugrid.parallelize(v).forEach(each);

	ugrid.end();
})();
