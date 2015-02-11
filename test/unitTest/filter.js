#!/usr/local/bin/node --harmony

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var ml = require('../../lib/ugrid-ml.js');

co(function *() {
console.log('before init');
	yield ugrid.init();

console.log('after init');
	function positive(n) {return (n > 0) ? true : false;}

	var rng = new ml.Random();
	var N = 10;
	var a = rng.randn(N);
	var b = a.filter(positive);

console.log('before work');
	var dist = yield ugrid.parallelize(a).filter(positive).collect();
console.log('after work');
	if (dist.length != b.length)
		throw 'error: local and distributed array have different lengths';

	for (var i = 0; i < b.length; i++)
		if (b[i] != dist[i])
			throw 'error: local and distributed array have different elements';

	ugrid.end();
})();
