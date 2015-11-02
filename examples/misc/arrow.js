#!/usr/bin/env node

var uc = require('ugrid').context();

uc.parallelize([1, 2, 3, 4])
  .map(x => 2*x)
  .collect()
  .on('data', console.log).on('end', uc.end)
