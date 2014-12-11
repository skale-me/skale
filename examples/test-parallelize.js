#!/usr/local/bin/node --harmony

var co = require('co');
var UgridClient = require('../lib/ugrid-client.js');
var UgridContext = require('../lib/ugrid-context.js');

var grid = new UgridClient({host: 'localhost', port: 12346, data: {type: 'master'}});

co(function *() {
	yield grid.connect();
	var res = yield grid.send('devices', {type: "worker"});
	var ugrid = new UgridContext(grid, res[0].devices);

	console.log('# Test with ' + res[0].devices.length + ' workers')

	var N = 10;

	var local = new Array(N);
	for (var i = 0; i < N; i++)
		local[i] = i;

	var t0 = ugrid.parallelize(local);
	var r0 = yield t0.collect();

	console.log('local array :')
	console.log(local)

	console.log('\ndistributed array :')
	console.log(r0)

	grid.disconnect();
})();
