#!/usr/local/bin/node --harmony
'use strict';

var ugrid = require('ugrid');

ugrid.context(function(err, uc) {
    if (err) {console.log(err); process.exit();}
    console.log('# Connected to ugrid');
    var data = [['one', 'a'], ['one', 'b'], ['two', 'c'], ['two', 'd'], ['two', 'e'], ['three', 'f']];

    var vector = uc.parallelize(data).reduceByKey(function(x,y) { return x + y});

	vector.collect(function(err,res) {
		console.log(res);
		uc.end();
	})
});