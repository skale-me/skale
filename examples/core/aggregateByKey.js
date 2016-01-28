#!/usr/bin/env node

var assert = require('assert');
var sc = new require('skale').Context();

var data = [['hello', 1], ['hello', 1], ['world', 1]]
var nPartitions = 2;

var init = 0;

function reducer(a, b) {return a + b;}
function combiner(a, b) {return a + b;}

sc.parallelize(data, nPartitions).
	aggregateByKey(combiner, reducer, init).
	collect().toArray(function(err, res) {
		assert(JSON.stringify(res) === JSON.stringify([['hello', 2], ['world', 1]]));
		console.log('Success !')
		console.log(res);
		sc.end();
	});
