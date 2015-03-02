#!/usr/local/bin/node

var grid = require('../../lib/ugrid-client.js')({data: {type: 't005'}});

grid.on('error', function (err) {
	console.log(err);
});

grid.on('connect', function () {
	console.log("hello");
	grid.end();
});
