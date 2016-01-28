#!/usr/bin/env node

var assert = require('assert');
var sc = new require('skale').Context();

function reducer(a, b) {a.push(b); return a;}
function combiner(a, b) {return a.concat(b);}

function by2(a) {return a * 2;}

var a = sc.parallelize([['hello', 1], ['world', 2], ['cedric', 3], ['test', 4]]).mapValues(by2).aggregate(reducer, combiner, [], function(err, res) {
	res.sort();
	assert(JSON.stringify(res) === JSON.stringify([['cedric', 6], ['hello', 2], ['test', 8], ['world', 4]]));	
	console.log('Success !')
	console.log(res);
	sc.end();
})
