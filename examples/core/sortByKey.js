#!/usr/bin/env node

var assert = require('assert');
var sc = new require('skale').Context();

var data = [['world', 2], ['cedric', 3], ['hello', 1]];
var nPartitions = 2;

sc.parallelize(data, nPartitions).sortByKey().collect().toArray(function(err, res) {
	assert(JSON.stringify(res) === JSON.stringify([['cedric', 3], ['hello', 1], ['world', 2]]));	
	console.log('Success !');
	console.log(res);
	sc.end();
});
