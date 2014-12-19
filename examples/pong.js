#!/usr/local/bin/node --harmony

var grid = require('../lib/ugrid-client.js')({host: 'localhost', port: 12346, data: {type: 'pong'}});

grid.connect_cb(function () {
	grid.on('ping', function (msg) {
		console.log('ping');
		console.log(msg);
	});
});
