#!/usr/bin/env node
'use strict';

var ugrid = require('ugrid');

var uc = new ugrid.Context();
	
function filter(data, obj) {
	return data % obj.modulo;
}

var res = uc.parallelize([1, 2, 3, 4])
	.filter(filter, {modulo: 2})
	.collect();

res.on('data', console.log);
res.on('end', uc.end);
