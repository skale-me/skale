#!/usr/bin/env node

var sc = new require('skale').Context();

function reducer(a, b) {a.push(b); return a;}
function combiner(a, b) {return a.concat(b);}

function dup(a) {return [a, a];}

sc.parallelize([['hello', 1], ['world', 2], ['cedric', 3], ['test', 4]]).flatMapValues(dup).aggregate(reducer, combiner, [], function(err, res) {
	console.log('Success !')
	console.log(res);
	sc.end();
})