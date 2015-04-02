#!/usr/local/bin/node --harmony
'use strict'

var co = require('co');

var ugrid = require('../..');

var args = JSON.parse(process.argv[2]);

var N = args.n;
var K = args.k;
var ITERATIONS = args.it;

co(function *() {
	var uc = yield ugrid.context();

	var ui = yield uc.devices({type: 'webugrid'}, 0);
	var viewer;
	
	if (ui[0]) viewer = ui[0].uuid;

	var data = [];
	var D = 2;

	for (var i = 0; i < N; i++) {
		var t0 = [];
		for (var j = 0; j < D; j++)
			t0.push(Math.random() * 2 - 1);
		data.push(t0);
	}

	var points = uc.parallelize(data).persist();
	var model = new ugrid.ml.KMeans(points, K);

	// Send source data to UI
	if (viewer)
		uc.send(viewer, {cmd: 'kmeans', data: {
			error: [],
			points: yield points.map(function(e) {return [1, e]}).collect()
		}});

	for (var i = 0; i < ITERATIONS; i++) {
		yield model.train(1);

		// Send clusterized data to UI
		if (viewer)
			uc.send(viewer, {cmd: 'kmeans', data: {
				error: model.mse,
				points: yield points.map(model.closestSpectralNorm, [model.means])
					.map(function(e) {return [e[0] + 1, e[1].data]})
					.collect()
			}});
	}

	console.log('KMeans finished')

	uc.end();
}).catch(ugrid.onError);
