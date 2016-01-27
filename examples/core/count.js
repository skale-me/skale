#!/usr/bin/env node

var sc = new require('skale').Context();

sc.parallelize([1, 2, 3, 4]).count(function(err, res) {
	console.log('Success !')
	console.log(res);
	sc.end();
});
