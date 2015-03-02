#!/usr/local/bin/node

var grid = require('../lib/ugrid-client.js')({port: 12348, ws: true});

grid.subscribe('monitoring');
//grid.pipe('monitoring', process.stdout);
grid.on('monitoring', console.log);
