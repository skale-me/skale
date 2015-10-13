#!/usr/local/bin/node --harmony
'use strict';

var ugrid = require('ugrid');

ugrid.context(function(err, uc) {
	if (err) {console.log(err); process.exit();}
	
	function valueFlatMapper(data, obj) {
		var tmp = [];
		for (var i = 0; i < obj.N; i++) 
			tmp.push(data * obj.fact);
		return tmp;
	}

	uc.parallelize([['hello', 1], ['world', 2]])
		.flatMapValues(valueFlatMapper, {N: 2, fact: 2})
		.collect(function(err, res) {
			if (err) {console.log(err); process.exit();}
			console.log(res);
			uc.end();
		});
})