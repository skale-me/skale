#!/usr/local/bin/node --harmony
'use strict';

var ugrid = require('ugrid');

ugrid.context(function(err, uc) {
	if (err) {console.log(err); process.exit();}
	
	function mapper(data, obj) {
		return data * obj.scaling;
	}

	uc.parallelize([1, 2, 3, 4])
		.map(mapper, {scaling: 1.2})
		.collect(function(err, res) {
			if (err) {console.log(err); process.exit();}
			console.log(res);
			uc.end();
		});
});