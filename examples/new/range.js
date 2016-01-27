#!/usr/bin/env node

var uc = require('ugrid').context();

uc.range(10).collect().toArray().then(console.log);

uc.range(10, -5, -3).collect().toArray().then(console.log);

uc.range(-4, 3).collect().toArray(function(err, res) {
	console.log(res);
	uc.end();
})
