#!/usr/bin/env node

var sc = new require('skale').Context();

var data = [['hello', 1], ['world', 2], ['cedric', 3], ['cedric', 4]];
var data2 = [['cedric', 3], ['world', 4], ['test', 5]];
var nPartitions = 1;

var a = sc.parallelize(data, nPartitions);
var b = sc.parallelize(data2, nPartitions);

a.coGroup(b).collect().toArray(function(err, res) {
	console.log('Success !')
	console.log(res);
	console.log(res[0]);
	console.log(res[1]);
	console.log(res[2]);
	console.log(res[3]);	
	sc.end();
});
