#!/usr/bin/env node

var uc = require('ugrid').context();
var ml = require('../../lib/ml.js');

var N = 4, D  = 3, seed = 0;

ml.randomSVMData(uc, N, D, seed).collect().on('data', console.log).on('end', uc.end);
