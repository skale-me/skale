#!/usr/local/bin/node

var grid = require('../../lib/ugrid-client.js')({data: {type: 't006'}});

grid.subscribe("test");
grid.publish("test", "Hello");
grid.on('test', function (msg) {
	console.assert(msg.data == "Hello");
	grid.end();
});
