#!/usr/local/bin/node --harmony

var grid = require('../lib/ugrid-client.js')({data: {type: 'pong'}});

grid.subscribe("test 1");
grid.pipe("test 1", process.stdout);
grid.publish("test 1", "first message\n");
grid.publish("test 1", "hello world\n");
