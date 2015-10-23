#!/usr/bin/env node
'use strict';

var ugrid = require('ugrid');

var uc = new ugrid.Context();

function mapper(data, obj) {
	return data * obj.scaling;
}

//var res = uc.parallelize([1, 2, 3, 4]).map(mapper, {scaling: 1.2}).collect({text: true});
var res = uc.parallelize([1, 2, 3, 4]).map(mapper, {scaling: 1.2}).collect();
res.toArray(function (err, data) {
	console.log(data);
	uc.end();
});

//res.pipe(process.stdout);
//res.on('data', console.log);
//res.on('end', uc.end);
