#!/usr/local/bin/node --harmony

var ugrid = require('../../');

var uc = ugrid.context(function (err, res) {
	var a = uc.parallelize([1, 2, 3, 4]);
	a.count(function (err, res) {
		console.log('count1: ' + res);
	});
	a.count(function (err, res) {
		console.log('count2: ' + res);
		process.exit(0);
	});
});
