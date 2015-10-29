#!/usr/local/bin/node --harmony
'use strict';

// textFile collect
var ugrid = require('ugrid');

var uc = new ugrid.Context();
// var file = uc.textFile("/home/iaranguren/work/test/buffer/buffer/biglog.txt");
var file = uc.textFile("/Users/cedricartigue/Documents/debug/biglog.txt");
var res = file.collect();
res.on('data', console.log);
res.on('end', uc.end);