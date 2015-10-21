#!/usr/local/bin/node --harmony
'use strict';

var ugrid = require('ugrid');

ugrid.context(function(err, uc) {
	if (err) {console.log(err); process.exit();}
	
	function mapper(data, obj) {
		return data * obj.scaling;
	}

	var res = uc.parallelize([1, 2, 3, 4]).map(mapper, {scaling: 1.2}).collect();

	res.on('data', console.log);
	res.on('end', uc.end);
});