#!/usr/bin/env node
'use strict';

var fs = require('fs');

var skale = fs.readFileSync('skale.res', {encoding: 'utf8'}).split(' ').map(Number);
var spark = fs.readFileSync('spark.res', {encoding: 'utf8'}).split(' ').map(Number);
var mse = [];

for (var i in skale)
	// mse[i] = Math.pow(skale[i] - spark[i], 2);
	mse[i] = skale[i] - spark[i];

// console.log(skale)
// console.log(spark)

console.log('distance = ' + mse)
