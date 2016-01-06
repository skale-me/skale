#!/usr/bin/env node

var uc = new require('ugrid').Context();

function reducer(a, b) {a.push(b); return a;}
function combiner(a, b) {return a.concat(b);}

function dup(a) {return [a, a];}

var a = uc.parallelize([1, 2, 3, 4]).flatMap(dup).aggregate(reducer, combiner, [], function(err, res) {
	console.log('Success !')
	console.log(res);
	uc.end();
})