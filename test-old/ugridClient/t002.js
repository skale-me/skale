#!/usr/local/bin/node

// Test send_cb

var grid = require('../../lib/ugrid-client.js')({data: {type: 't000'}});

process.on('exit', function () {console.assert(grid.id !== undefined);});

grid.send(0, {cmd: 'xxx'}, function (err, res) {
	console.assert(err == 'Invalid command: xxx');
	grid.end();
});
