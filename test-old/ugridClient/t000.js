#!/usr/local/bin/node

// Test client and device get

var grid = require('../../lib/ugrid-client.js')({data: {type: 't000'}});

process.on('exit', function () {console.assert(grid.id !== undefined);});

grid.devices({type: 't000'}, 0, function (err, res) {
	console.assert(res != undefined);
	console.assert(res[0] != undefined);
	console.assert(res[0].uuid != undefined);
	console.assert(res[0].id != undefined);
	console.assert(res[0].ip != undefined);
	grid.end();
});
