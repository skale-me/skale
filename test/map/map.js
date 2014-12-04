#!/usr/local/bin/node --harmony

var co = require('co');
var fs = require('fs');
var UgridClient = require('../../lib/ugrid-client.js');
var UgridContext = require('../../lib/ugrid-context.js');
var tl = require('../test-lib.js');

var grid = new UgridClient({host: 'localhost', port: 12346, data: {type: 'master'}});

var json = JSON.parse(fs.readFileSync('map.json', {encoding: 'utf8'}));	

co(function *() {
	yield grid.connect();
	var res = yield grid.send('devices', {type: "worker"});
	var ugrid = new UgridContext(grid, res.devices);

	var res = yield ugrid.parallelize(json.input).map(tl.doubles, []).collect();

	console.log('distributed map result')
	console.log(res);

	console.log('\nlocal map result')
	console.log(json.output);

	grid.disconnect();
})();

