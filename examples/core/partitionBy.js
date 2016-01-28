#!/usr/bin/env node

var assert = require('assert');
var sc = new require('skale').Context();
var HashPartitioner = require('skale').HashPartitioner;

var data = [['hello', 1], ['world', 1], ['hello', 2], ['world', 2], ['cedric', 3]];

sc.parallelize(data).partitionBy(new HashPartitioner(3)).collect().toArray(function(err, res) {
	assert(JSON.stringify(res) === JSON.stringify([['world', 1], ['world', 2],['hello', 1],['hello', 2],['cedric', 3]]));	
	console.log('Success !');
	console.log(res);
	sc.end();
});
