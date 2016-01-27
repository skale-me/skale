#!/usr/bin/env node

var sc = new require('skale').Context();

var data = [4, 6, 10, 5, 1, 2, 9, 7, 3, 0];
var nPartitions = 3;

function keyFunc(data) {return data;}

sc.parallelize(data, nPartitions).sortBy(keyFunc).collect().toArray(function(err, res) {
	console.log('Success !');
	console.log(res);
	sc.end();
});
