#!/usr/local/bin/node

var util = require('util');
var grid = require('../lib/ugrid-client.js')({data: {type: 'ping'}});

grid.devices({type: 'pong'}, function (err, res) {
	if (!res.length) process.exit(1);
	console.log("request to " + util.inspect(res[0]));
	grid.request(res[0], 'hello', function (err, res) {
		console.log("got " + res);
		process.exit(0);
	});
});
