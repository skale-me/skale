#!/usr/bin/env node

var assert = require('assert');
var sc = new require('skale').Context();

sc.parallelize([1, 2, 3, 4], 2).top(2).toArray(function(err, res) {
	assert(JSON.stringify(res) === JSON.stringify([3, 4])); 	
	console.log('Success !')
	console.log(res);
	sc.end();
})
