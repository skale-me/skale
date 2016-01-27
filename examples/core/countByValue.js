#!/usr/bin/env node

var sc = new require('skale').Context();

var data = [[1, 1], [1, 1], [2, 3], [2, 4], [3, 5]];

var nPartitions = 1;

function valueFlatMapper(e) {
	var i, out = [];
	for (i = e; i <= 5; i++) out.push(i);
	return out;
}

sc.parallelize(data, nPartitions).flatMapValues(valueFlatMapper).countByValue().toArray(function(err, res) {
	console.log('Success !')
	console.log(res);
	sc.end();
});
