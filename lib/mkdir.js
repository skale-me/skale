// Copyright 2016 Luca-SAS, licensed under the Apache License 2.0

'use strict';

var mkdirSync = require('fs').mkdirSync;
var sep = require('path').sep;
var cache = {};

// A synchronous version of recursive mkdir with a cache of previous calls
function mkdir(p) {
	if (p in cache) return;
	var a = p.split(sep), b = [], i, j;
	a.shift();
	for (i = 0; i < a.length; i++) {
		b[i] = '';
		for (j = 0; j <= i; j++)
			b[i] += sep + a[j];
		if (b[i] in cache) continue;
		try { mkdirSync(b[i]); } catch (e) {
			if (e.code != 'EEXIST') console.log(e);
		}
		cache[b[i]] = true;
	}
}
mkdir.cache = cache;
module.exports = mkdir;
