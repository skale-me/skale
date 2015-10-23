#!/usr/bin/env node
'use strict';

var ugrid = require('ugrid');

var uc = new ugrid.Context();

function valueMapper(data, obj) {
	return data * obj.fact;
}

var res = uc.parallelize([['hello', 1], ['world', 2]])
	.mapValues(valueMapper, {fact: 2})
	.collect();

res.on('data', console.log);
res.on('end', uc.end);		
