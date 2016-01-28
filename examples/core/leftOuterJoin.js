#!/usr/bin/env node

var assert = require('assert');
var sc = new require('skale').Context();

var da1 = sc.parallelize([[10, 1], [20, 2]]);
var da2 = sc.parallelize([[10, 'world'], [30, 3]]);
var res = da1.leftOuterJoin(da2).collect().toArray(function(err, res) {
	assert(JSON.stringify(res) === JSON.stringify([[20, [2, null]], [10, [1, 'world']]])); 
	console.log('Success !')
	console.log(res);
	sc.end();
});
