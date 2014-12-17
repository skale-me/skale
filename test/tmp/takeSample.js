#!/usr/local/bin/node --harmony

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')({host: 'localhost', port: 12346});
var ml = require('../../lib/ugrid-ml.js');

co(function *() {
	yield ugrid.init();

	var N = 10;
	var n = 3;
	var V = [];

	for (var i = 0; i < N; i++) 
		V[i] = i;

	var d1 = yield ugrid.parallelize(V).takeSample(n);

	console.error(V);
	console.error();
	console.error(d1);

	if (d1.length == n ) {
		
		for ( var i = 0; i < d1.length; i++) {
			if ( V.indexOf(d1[i]) == -1) {
				console.log("test ko");
				process.exit(1); //test KO
			} else {
				console.log("test ok");
				process.exit(0); //test OK
			} 
		}
	} 
	ugrid.end();
})();

