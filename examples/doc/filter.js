#!/usr/local/bin/node --harmony
'use strict';

var ugrid = require('ugrid');

ugrid.context(function(err, uc) {
	if (err) {console.log(err); process.exit();}
	
	function filter(data, obj) {
		return data % obj.modulo;
	}

	uc.parallelize([1, 2, 3, 4])
		.filter(filter, {modulo: 2})
		.collect(function(err, res) {
			if (err) {console.log(err); process.exit();}
			console.log(res);
			uc.end();
		});
});