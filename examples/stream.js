#!/usr/bin/env node

var sc = require('skale-engine').context();
// var s = sc.range(20).stream({gzip: true});
//var s = sc.range(20).stream();
var s = sc.range(20).stream({end: true});
s.pipe(process.stdout);
//s.on('end', sc.end);
