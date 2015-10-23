#!/usr/local/bin/node --harmony
'use strict';

var ugrid = require('ugrid');

ugrid.context(function(err, uc) {
	if (err) {console.log(err); process.exit();}
	console.log('# Connected to ugrid');

	var file = uc.textFile("/Users/cedricartigue/Documents/debug/biglog.txt");
	var res = file.collect();

	res.on('data', console.log);
	res.on('end', uc.end);
});
