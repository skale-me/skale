#!/usr/bin/env node

var uc = new require('ugrid').Context();

var da1 = uc.parallelize([[1, 1], [1, 1], [2, 3], [2, 4], [3, 5]]);
var da2 = uc.parallelize([[10, 1], [10, 3], [10, 4], [10, 4]]);

// da1.reduceByKey((a,b) => a+b, 0).collect().on('data', console.log).on('end', uc.end);

da1.subtract(da2).collect().on('data', console.log).on('end', uc.end);