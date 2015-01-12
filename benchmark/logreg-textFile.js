#!/usr/local/bin/node --harmony
'use strict';

var co = require('co');
var ugrid = require('../lib/ugrid-context.js')({host: 'localhost', port: 12346});
var ml = require('../lib/ugrid-ml.js');

// NB il faut que le nombre de partitions soit par défzaut le nombre de workers
co(function *() {
	yield ugrid.init();

	var N = 800000;					// Number of observations
	//~ var N = 1000;
	var D = 16;							// Number of features
	var P = 2;							// Number of partitions
	var seed = 1;
	var file = process.argv[2];
	var ITERATIONS = process.argv[3];				// Number of iterations
	var time = new Array(ITERATIONS);
	var rng = new ml.Random(seed);
	var w = rng.randn(D);
	
	function parse(e) {
		var tmp = e.split(' ').map(parseFloat);
		return {label: tmp.shift(), features: tmp}
	}

	var points = ugrid.textFile(file, P).map(parse, []).persist();	

	for (var i = 0; i < ITERATIONS; i++) {
		//~ console.log('w = ' + w);
		var startTime = new Date();
		var gradient = yield points.map(ml.logisticLossGradient, [w]).reduce(ml.sum, ml.zeros(D));
		for (var j = 0; j < w.length; j++)
			w[j] -= 1 / (Math.sqrt(i + 1)) * gradient[j] / N;		
		var endTime = new Date();
		time[i] = (endTime - startTime) / 1000;
		startTime = endTime;
		//~ console.log('\nIteration : ' + i + ', Time : ' + time[i]);
	}
	console.log(w.join(' '));
	ugrid.end();
})();
