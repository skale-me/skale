#!/usr/bin/env node

// var uc = new require('ugrid').Context();

// uc.parallelize([1, 2, 3, 4]).collect(function(err, res) {
// 	console.log('Success !')
// 	console.log(res);
// 	uc.end();
// })

var uc = new require('ugrid').Context();

var a = uc.parallelize([1, 2, 3, 4], 2).collect().toArray(function(err, res) {
	console.log('Success !')
	console.log(res);
	uc.end();
})
