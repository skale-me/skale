#!/usr/bin/env node

var assert = require('assert');
var uc = new require('ugrid').Context();

function reducer(a, b) {a.push(b); return a;}
function combiner(a, b) {return a.concat(b);}

function filter(a) {return a % 2;}

uc.parallelize([1, 2, 3, 4]).filter(filter).aggregate(reducer, combiner, [], function(err, res) {
	assert(JSON.stringify(res) === JSON.stringify([1, 3])); 
	console.log('Success !')
	console.log(res);
	uc.end();
})
