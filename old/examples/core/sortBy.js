#!/usr/bin/env node

var assert = require('assert');
var uc = new require('ugrid').Context();

var data = [4, 6, 10, 5, 1, 2, 9, 7, 3, 0];
var nPartitions = 3;

function keyFunc(data) {return data;}

uc.parallelize(data, nPartitions).sortBy(keyFunc).collect().toArray(function(err, res) {
	assert(JSON.stringify(res) === JSON.stringify([0, 1, 2, 3, 4, 5, 6, 7, 9, 10]));	
	console.log('Success !');
	console.log(res);
	uc.end();
});
