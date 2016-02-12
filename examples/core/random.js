#!/usr/bin/env node

var uc = new require('ugrid').Context();
var RandomSVMData = require('../../lib/ml.js').RandomSVMData;

var N = 4;
var D = 2;
var seed = 1;
var P = 2;

var a = new RandomSVMData(uc, N, D, seed, P).persist();

function reducer(a, b) {a.push(b); return a;}
function combiner(a, b) {return a.concat(b);}

a.aggregate(reducer, combiner, [], function(err, res) {
	console.log('First Time !')
	console.log(res);

	a.aggregate(reducer, combiner, [], function(err, res) {
		console.log('\nSecond Time !')
		console.log(res);
		uc.end();
	})
})
