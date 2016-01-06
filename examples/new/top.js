#!/usr/bin/env node

var uc = new require('ugrid').Context();

uc.parallelize([1, 2, 3, 4], 2).top(2).toArray(function(err, res) {
	console.log('Success !')
	console.log(res);
	uc.end();
})
