#!/usr/local/bin/node --harmony
'use strict';

var uc = new  require('ugrid').Context();

var data = [1, 2];

var out = uc.parallelize(data).map(function() {;}).collect();

out.on('data', function(data) {
	console.log(data)
});

out.on('end', uc.end);
