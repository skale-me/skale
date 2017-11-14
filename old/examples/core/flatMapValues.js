#!/usr/bin/env node

var assert = require('assert');
var uc = new require('ugrid').Context();

function reducer(a, b) {a.push(b); return a;}
function combiner(a, b) {return a.concat(b);}

function dup(a) {return [a, a];}

uc.parallelize([['hello', 1], ['world', 2], ['cedric', 3], ['test', 4]]).flatMapValues(dup).aggregate(reducer, combiner, [], function(err, res) {
	res.sort();
	assert(JSON.stringify(res) === JSON.stringify([ [ 'cedric', 3 ],[ 'cedric', 3 ],[ 'hello', 1 ],[ 'hello', 1 ],[ 'test', 4 ],[ 'test', 4 ],[ 'world', 2 ],[ 'world', 2 ] ])); 	
	console.log('Success !')
	console.log(res);
	uc.end();
})
