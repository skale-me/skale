#!/usr/local/bin/node --harmony

// mongo -> collect

var co = require('co');
var fs = require('fs');
var ugrid = require('../../lib/ugrid-context.js')();

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var dist = yield ugrid.mongo({v : {$gt: 1}}).collect();

	// var dist = yield ugrid.mongo().collect();
	console.log(dist);

	ugrid.end();
})();
