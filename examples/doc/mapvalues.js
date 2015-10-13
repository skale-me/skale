#!/usr/local/bin/node --harmony
'use strict';

var ugrid = require('ugrid');

ugrid.context(function(err, uc) {
	if (err) {console.log(err); process.exit();}
	
	function valueMapper(data, obj) {
		return data * obj.fact;
	}

	uc.parallelize([['hello', 1], ['world', 2]])
		.mapValues(valueMapper, {fact: 2})
		.collect(function(err, res) {
			if (err) {console.log(err); process.exit();}
			console.log(res);
			uc.end();
		});
})