#!/usr/local/bin/node

// Test request and reply

var grid = require('../../lib/ugrid-client.js')({data: {type: 't000'}});

process.on('exit', function () {console.assert(grid.id !== undefined);});

grid.on("request", function (msg) {
	grid.reply(msg, null, "test ok");
});

grid.devices({type: 't000'}, function (err, res) {
	console.assert(res[0] != undefined);
	grid.request(res[0], "test", function (err, res) {
		console.assert(res == "test ok");
		grid.end();
	});
});
