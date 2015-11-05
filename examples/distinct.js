#!/usr/bin/env node

var uc = require('ugrid').context();

uc.parallelize([ 1, 2, 3, 1, 4, 3, 5 ]).
  distinct().
  collect().on('data', console.log).on('end', uc.end);
