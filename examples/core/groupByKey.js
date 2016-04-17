#!/usr/bin/env node

var sc = require('skale-engine').context();

var data = [['hello', 1], ['hello', 1], ['world', 1]];
var nPartitions = 1;

var a = sc.parallelize(data, nPartitions).groupByKey().persist();

a.collect().toArray(function(err, res) {
	console.log(res);
	console.log('First ok!');
	a.collect().toArray(function(err, res) {
		console.log(res);
		console.log('Second ok !');
		sc.end();
	});
});
