#!/usr/bin/env node
'use strict';

var fs = require('fs');

var skale = fs.readFileSync('skale.res', {encoding: 'utf8'}).split(' ').map(Number);
var spark = fs.readFileSync('spark.res', {encoding: 'utf8'}).split(' ').map(Number);
var mse = 0;

for (var i in skale)
	mse += Math.pow(skale[i] - spark[i], 2);

console.log(skale)
console.log(spark)

console.log('Mean square error = ' + mse / skale.length)
