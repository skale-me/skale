#!/usr/bin/env node

var sc = new require('skale').Context();

function reducer(a, b) {a.push(b); return a;}
function combiner(a, b) {return a.concat(b);}

function filter(a) {return a % 2;}

sc.parallelize([1, 2, 3, 4]).filter(filter).aggregate(reducer, combiner, [], function(err, res) {
	console.log('Success !')
	console.log(res);
	sc.end();
})