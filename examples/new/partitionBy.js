#!/usr/bin/env node

var uc = new require('ugrid').Context();
var HashPartitioner = require('ugrid').HashPartitioner;

var data = [['hello', 1], ['world', 1], ['hello', 2], ['world', 2], ['cedric', 3]];

uc.parallelize(data).partitionBy(new HashPartitioner(3)).collect().toArray(function(err, res) {
	console.log('Success !');
	console.log(res);
	uc.end();
});
