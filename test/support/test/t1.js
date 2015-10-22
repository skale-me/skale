#!/usr/bin/env node

if (process.argv[2] == 'local') {
	var ugrid = require('../local.js');
	console.log('loc')
} else {
	var ugrid = require('ugrid');
	console.log('noloc')
}
var v = [
	[[4, 1], [1, 1], [2, 3], [2, 4], [3, 5]],
	[[0, 5], [1, 6], [2, 7], [3, 9], [0, 9]],
];

ugrid.context(function (err, uc) {
	var a, b, s;

	a = uc.parallelize(v[0]);
	b = uc.parallelize(v[1]);

	// s = a.collect();
	//s = a.coGroup(b).collect();
	s = a.join(b).collect();

	s.on('data', console.log)
	s.on('end', uc.end)
})
