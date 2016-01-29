#!/usr/bin/env node

var sc = require('skale').context();
var ml = require('../../lib/ml.js');

var N = 4, D  = 3, seed = 0;

ml.randomSVMData(sc, N, D, seed).collect().on('data', console.log).on('end', sc.end);
