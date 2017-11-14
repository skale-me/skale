#!/usr/bin/env node

var uc = require('ugrid').context();

uc.range(6).map(a => a*a).reduce((a,b) => a+b, 0).then(function (res) {
	console.log(res);
	console.assert(res == 55);
	uc.end();
});
