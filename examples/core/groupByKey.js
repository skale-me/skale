#!/usr/bin/env node

var sc = new require('skale').Context();

var data = [['hello', 1], ['hello', 1], ['world', 1]]
var nPartitions = 1;

var a = sc.parallelize(data, nPartitions).groupByKey().persist();

a.collect().toArray(function(err, res) {
		console.log('First ok!')
		console.log(res);
		a.collect().toArray(function(err, res) {
				console.log('Second ok !')
				console.log(res);
				sc.end();
			});
	})
