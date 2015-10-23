#!/usr/local/bin/node --harmony
'use strict';

var ugrid = require('ugrid');

ugrid.context(function(err, uc) {
	if (err) {console.log(err); process.exit();}

	uc.parallelize([[1,1],[1,1],[2,3],[2,4],[3,5]]).collect().toArray(function(err, result) {
		console.log(result)
		uc.end();
	});

	// res.on('data', console.log);
	// res.on('end', uc.end);
});
