#!/usr/bin/env node

var assert = require('assert');
var uc = new require('ugrid').Context();

function reducer(a, b) {a.push(b); return a;}
function combiner(a, b) {return a.concat(b);}

var a = uc.parallelize([1, 2, 3, 4]);
var b = uc.parallelize([5, 6, 7, 8]);

a.union(b).aggregate(reducer, combiner, [], function(err, res) {
	res.sort();
	assert(JSON.stringify(res) === JSON.stringify([1, 2, 3, 4, 5, 6, 7, 8]));	
	console.log('Success !')
	console.log(res);
	uc.end();
})
