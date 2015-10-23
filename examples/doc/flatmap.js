#!/usr/bin/env node
'use strict';

var ugrid = require('ugrid');

var uc = new ugrid.Context();
	
function flatMapper(data, obj) {
	var tmp = [];
	for (var i = 0; i < obj.N; i++) tmp.push(data);
	return tmp;
}

var res = uc.parallelize([1, 2, 3, 4])
	.flatMap(flatMapper, {N: 2})
	.collect();

res.on('data', console.log);
res.on('end', uc.end);		
