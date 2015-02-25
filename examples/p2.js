#!/usr/local/bin/node

var grid = require('../lib/ugrid-client.js')({data: {type: 'p2'}});

grid.publish("test 1", "first message\n");
grid.publish("test 1", "hello world\n");

grid.publish("test 2", "hello test2\n");
grid.subscribe("test 2");

grid.on("test 2", function (msg) {
	console.log("test 2: " + msg.data);
});

grid.pipe("test 1", process.stdout);
grid.subscribe("test 1");
