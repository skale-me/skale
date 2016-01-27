#!/usr/bin/env node

var sc = new require('skale').Context();

function sum(a, b) {return a + b;}

var a = sc.parallelize([1, 2, 3, 4], 2).reduce(sum, 0, function(err, res) {
	console.log('Success !')	
	console.log(res);
	sc.end();
})
