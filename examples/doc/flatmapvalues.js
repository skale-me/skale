#!/usr/bin/env node
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

	var res = uc.parallelize([['hello', 1], ['world', 2]])
		.flatMapValues(valueFlatMapper, {N: 2, fact: 2})
		.collect();

	res.on('data', console.log);
	res.on('end', uc.end);		
})
