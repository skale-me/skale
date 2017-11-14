#!/usr/bin/env node

var assert = require('assert');
var uc = new require('ugrid').Context();

function by2(a, args) {return a * 2 * args.bias;}
function sum(a, b) {return a + b;}

var a = uc.parallelize([1, 2, 3, 4]).map(by2, {bias: 2}).reduce(sum, 0, function(err, res) {
	assert(res == 40)	
	console.log(res);
	uc.end();
})
