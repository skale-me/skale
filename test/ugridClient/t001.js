#!/usr/local/bin/node

// Test client and device get

var grid = require('../../lib/ugrid-client.js')({data: {type: 't000'}});

process.on('exit', function () {console.assert(grid.id !== undefined);});

grid.on('connect', function () {
	console.assert(grid.uuid != undefined);
	console.assert(grid.id != undefined);
	grid.end();
});
