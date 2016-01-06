#!/usr/bin/env node

var uc = new require('ugrid').Context();

var data = [['hello', 1], ['world', 2], ['world', 3]];
var data2 = [['cedric', 3], ['world', 4]];
var nPartitions = 4;

var a = uc.parallelize(data, nPartitions);
var b = uc.parallelize(data2, nPartitions);

a.join(b).collect().toArray(function(err, res) {
	console.log('Success !')
	console.log(res)
	uc.end();
});
