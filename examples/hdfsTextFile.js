#!/usr/local/bin/node
'use strict';

var url = require('url')
var uc = new require('ugrid').Context();

var file = "hdfs://localhost:9000/svm200MB";
// var file = "hdfs://localhost:9000/v";
// var file = "/Users/cedricartigue/work/ugrid/examples/random";

var res = uc.textFile(file).collect();

res.on('data', console.log);
res.on('end', uc.end);
