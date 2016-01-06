#!/usr/bin/env node

var uc = new require('ugrid').Context();

uc.parallelize([1, 2, 3, 4], 1).collect().toArray(function(err, res) {
	console.log('Success !')
	console.log(res);
	uc.end();
})
