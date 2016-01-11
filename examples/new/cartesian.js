#!/usr/bin/env node

var uc = new require('ugrid').Context();

var data = [1, 2, 3, 4, 5, 6];
var data2 = [7, 8, 9, 10, 11, 12];
var nPartitions = 2;

var a = uc.parallelize(data, nPartitions);
var b = uc.parallelize(data2, nPartitions);

a.cartesian(b).collect().toArray(function(err, res) {
	console.log('Success !');
	console.log(res);
	uc.end();
});
