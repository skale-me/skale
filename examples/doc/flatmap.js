#!/usr/local/bin/node --harmony
'use strict';

var ugrid = require('ugrid');

ugrid.context(function(err, uc) {
	if (err) {console.log(err); process.exit();}
	
	function flatMapper(data, obj) {
		var tmp = [];
		for (var i = 0; i < obj.N; i++) tmp.push(data);
		return tmp;
	}

	uc.parallelize([1, 2, 3, 4])
		.flatMap(flatMapper, {N: 2})
		.collect(function(err, res) {
			if (err) {console.log(err); process.exit();}
			console.log(res);
			uc.end();
		});
})