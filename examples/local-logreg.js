#!/usr/local/bin/node --harmony

var ml = require('../lib/ugrid-ml.js');

var N = 203472;						// Number of observations
var D = 16;							// Number of features
var P = 4;							// Number of partitions
var ITERATIONS = 20;				// Number of iterations
var time = new Array(ITERATIONS);

var rng = new ml.Random();
var w = rng.randn(D);

// Load test data
var points = ml.loadTestData(N, D, P);

// Concatenate partitions to form one vector
var data = [];
for (var p = 0; p < P; p++)
	data = data.concat(points[p]);

// Iterate and compute gradient
for (var i = 0; i < ITERATIONS; i++) {
	var startTime = new Date();
	var gradient = data.map(function (e) {return ml.logisticLossGradient(e, w);}).reduce(ml.sum, ml.zeros(D));
	for (var j = 0; j < w.length; j++)
		w[j] -= gradient[j];
	var endTime = new Date();
	time[i] = (endTime - startTime) / 1000;
	startTime = endTime;
	console.log('\nIteration : ' + i + ', Time : ' + time[i]);
}
console.log(w);
console.log('\nFirst iteration : ' + time[0]);
time.shift();
console.log('Later iterations : ' + time.reduce(function (a, b) {return a + b;}) / (ITERATIONS - 1));

// var resSerial = [ 1132.5804317594593,
//   -1280.219956229454,
//   -547.4631430381041,
//   63289.63872524456,
//   -888.8120480812047,
//   -366.80562384609357,
//   864.6879756980597,
//   48.629033425598664,
//   43.85626654608404,
//   -132.37612636662152,
//   -291.4945553481557,
//   394.2522883735051,
//   -170.14971288800143,
//   -184.96072800367074,
//   -913.6673787210252,
//   549.7047827079477 ];

// var resPar = [ 1132.5804317594593,
//   -1280.219956229454,
//   -547.4631430381041,
//   63289.63872524456,
//   -888.8120480812047,
//   -366.80562384609357,
//   864.6879756980597,
//   48.629033425598664,
//   43.85626654608404,
//   -132.37612636662152,
//   -291.4945553481557,
//   394.2522883735051,
//   -170.14971288800143,
//   -184.96072800367074,
//   -913.6673787210252,
//   549.7047827079477 ];
