#!/usr/bin/env node

var uc = require('ugrid').context();

var da1 = uc.parallelize([[10, 1], [10, 2], [10, 4]]);
da1.reduceByKey((a,b) => a+b, 0).collect().on('data', console.log).on('end', uc.end);
