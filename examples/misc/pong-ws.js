#!/usr/local/bin/node --harmony

// Run 'bin/ugrid.js -w 12348' prior to start this script

var grid = require('../../lib/ugrid-client.js')({
	ws: true,
	host: 'localhost',
	port: 12348,
	data: {type: 'pong'}
}, function (err, res) {
	console.log(res);
});

grid.on('request', function (msg) {
	console.log(msg);
	grid.reply(msg, null, 'pong');
	console.log('pong');
});
