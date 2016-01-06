#!/usr/bin/env node

var uc = new require('ugrid').Context();

var d1 = [[1, 1], [1, 1], [2, 3], [2, 4], [3, 5]];
var d2 = [[1, 1], [1, 1], [2, 3]];


uc.parallelize(d1).subtract(uc.parallelize(d2)).collect().toArray(function(err, res) {
	console.log('Success !')
	console.log(res);
	uc.end();
});
