#!/usr/bin/env node
'use strict';

var uc = require('ugrid').context();

var vect = [1, 2, 3, 4, 5];

function mapper(a) {return a * a;}

function sum(a, b) {return a + b;}

function done(err, result) {
	console.log('Summing the squared value of [' + vect + '] gives ' + result);
	uc.end();
}

uc.parallelize(vect).map(mapper).reduce(sum, 0, done);
