#!/usr/local/bin/node

var grid = require('../lib/ugrid-client.js')();

grid.subscribe('monitoring');
grid.pipe('monitoring', process.stdout);
