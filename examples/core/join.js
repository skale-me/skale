#!/usr/bin/env node

var sc = require('skale-engine').context();

var data = [['hello', 1], ['world', 2], ['world', 3]];
var data2 = [['cedric', 3], ['world', 4]];
var nPartitions = 4;

var a = sc.parallelize(data, nPartitions);
var b = sc.parallelize(data2, nPartitions);

a.join(b).collect().toArray(function(err, res) {
	console.log(res);
	console.assert(JSON.stringify(res) === JSON.stringify([['world', [2, 4]],['world',[3, 4]]]));	
	sc.end();
});
