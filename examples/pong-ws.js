#!/usr/local/bin/node --harmony

// Run 'bin/ugrid.js -w 12348' prior to start this script

var grid = require('../lib/ugrid-client.js')({host: 'localhost', port: 12348, data: {type: 'pong'}});

grid.wsConnect_cb(function () {
	grid.on('ping', function (msg) {
		console.log('ping');
		console.log(msg);
	});
});
