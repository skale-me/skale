#!/usr/bin/env node

var sc = new require('skale').Context();

var data = [['world', 2], ['cedric', 3], ['hello', 1]];
var nPartitions = 2;

sc.parallelize(data, nPartitions).sortByKey().collect().toArray(function(err, res) {
	console.log('Success !');
	console.log(res);
	sc.end();
});
