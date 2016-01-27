#!/usr/bin/env node

var sc = new require('skale').Context();

var data = [['hello', 1], ['hello', 1], ['world', 1]]
var nPartitions = 1;

var init = 0;

function reducer(a, b) {return a + b;}

sc.parallelize(data, nPartitions).
	reduceByKey(reducer, init).
	collect().toArray(function(err, res) {
		console.log('Success !')
		console.log(res);
		sc.end();
	})
