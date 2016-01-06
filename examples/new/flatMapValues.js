#!/usr/bin/env node

var uc = new require('ugrid').Context();

function reducer(a, b) {a.push(b); return a;}
function combiner(a, b) {return a.concat(b);}

function dup(a) {return [a, a];}

var a = uc.parallelize([['hello', 1], ['world', 2], ['cedric', 3], ['test', 4]]).flatMapValues(dup).aggregate(reducer, combiner, [], function(err, res) {
	console.log('Success !')
	console.log(res);
	uc.end();
})