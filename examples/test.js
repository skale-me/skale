#!/usr/local/bin/node
'use strict';

var uc = new require('ugrid').Context();

uc.parallelize([[1,1],[1,1],[2,3],[2,4],[3,5]]).collect().toArray(function(err, result) {
	console.log(result)
	uc.end();
});
