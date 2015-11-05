#!/usr/bin/env node

var uc = require('ugrid').context();

//var da1 = uc.parallelize([['hello', 1], ['salut', 2]]);
//var da2 = uc.parallelize([['hello', 'world'], ['arghh', 3]]);
var da1 = uc.parallelize([[10, 1], [20, 2]]);
var da2 = uc.parallelize([[10, 'world'], [30, 3]]);

da1.coGroup(da2).collect().on('data', console.log).on('end', uc.end);
