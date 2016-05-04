#!/usr/bin/env node

var sc = require('skale-engine').context();

function reducer(a, b) {a.push(b); return a;}
function combiner(a, b) {return a.concat(b);}

function dup(a) {return [a, a];}

sc.parallelize([['hello', 1], ['world', 2], ['cedric', 3], ['test', 4]])
  .flatMapValues(dup)
  .aggregate(reducer, combiner, [], function(err, res) {
	console.log(res);
	res.sort();
	console.assert(JSON.stringify(res) === JSON.stringify([ [ 'cedric', 3 ],[ 'cedric', 3 ],[ 'hello', 1 ],[ 'hello', 1 ],[ 'test', 4 ],[ 'test', 4 ],[ 'world', 2 ],[ 'world', 2 ] ])); 	
	sc.end();
});
