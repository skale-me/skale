#!/usr/bin/env node

var uc = new require('ugrid').Context();

var data = [['world', 2], ['cedric', 3], ['hello', 1]];
var nPartitions = 2;

uc.parallelize(data, nPartitions).sortByKey().collect().toArray(function(err, res) {
	console.log('Success !');
	console.log(res);
	uc.end();
});
