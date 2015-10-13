#!/usr/local/bin/node --harmony
'use strict';

var ugrid = require('ugrid');

ugrid.context(function(err, uc) {
	if (err) {console.log(err); process.exit();}
	
	function reducer(a, b) {
		return a = a + b;
	}

	uc.parallelize([['hello', 1], ['world', 2], ['hello', 3]])
		.reduceByKey(reducer, 0)
		.collect(function(err, res) {
			if (err) {console.log(err); process.exit();}
			console.log(res);
			uc.end();
		});
});