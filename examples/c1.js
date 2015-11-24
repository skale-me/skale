#!/usr/bin/env node

var uc = require('ugrid').context();

a = uc.parallelize([0, 1]);
b = uc.parallelize([0, 1]);
a.cartesian(b).collect().on('data', console.log).on('end', uc.end);
