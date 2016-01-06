#!/usr/bin/env node

var uc = new require('ugrid').Context();

function sum(a, b) {return a + b;}

var a = uc.parallelize([1, 2, 3, 4], 2).reduce(sum, 0, function(err, res) {
	console.log('Success !')	
	console.log(res);
	uc.end();
})
