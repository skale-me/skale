#!/usr/local/bin/node

var grid = require('../lib/ugrid-client.js')();
grid.subscribe('monitoring');
grid.on('monitoring', console.log);
