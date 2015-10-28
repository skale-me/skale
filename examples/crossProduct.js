#!/usr/local/bin/node
'use strict';

var uc = new require('ugrid').Context();

var a = [[1, 1], [1, 1], [2, 3], [2, 4], [3, 5]];
var b = [[0, 5], [1, 6], [2, 7], [3, 9], [0, 9]];

uc.parallelize(b).crossProduct(uc.parallelize(a)).collect().toArray(function(err, res) {
	console.log(res);
	console.log(res.length);	
	uc.end();
})