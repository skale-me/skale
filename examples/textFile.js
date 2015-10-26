#!/usr/local/bin/node
'use strict';

var uc = new require('ugrid').Context();

// var res = uc.textFile2("/Users/cedricartigue/work/ugrid/examples/less_than_a_line_with_4_workers").collect();
var res = uc.textFile2("/Users/cedricartigue/work/ugrid/examples/less_bytes_than_workers").collect();

res.on('data', console.log);
res.on('end', uc.end);
