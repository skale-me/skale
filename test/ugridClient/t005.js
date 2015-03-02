#!/usr/local/bin/node

var grid = require('../../lib/ugrid-client.js')({data: {type: 't005'}});

grid.on('connect', function () {
	var p = grid.createWriteStream("test", {uuid: grid.uuid});
	p.write("Hello");
});

grid.on('test', function (msg) {
	console.assert(msg.data == "Hello");
	grid.end();
});
