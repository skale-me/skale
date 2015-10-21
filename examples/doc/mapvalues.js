#!/usr/bin/env node
'use strict';

var ugrid = require('ugrid');

ugrid.context(function(err, uc) {
	if (err) {console.log(err); process.exit();}
	
	function valueMapper(data, obj) {
		return data * obj.fact;
	}

	var res = uc.parallelize([['hello', 1], ['world', 2]])
		.mapValues(valueMapper, {fact: 2})
		.collect();

	res.on('data', console.log);
	res.on('end', uc.end);		
})
