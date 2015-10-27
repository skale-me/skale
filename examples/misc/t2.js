#!/usr/bin/env node

var ugrid = require('ugrid');

var uc = new ugrid.Context();
var res = uc.parallelize([1]).collect();

res.on('data', console.log);
res.on('end', uc.end);
