#!/usr/local/bin/node --harmony

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var ml = require('../../lib/ugrid-ml.js');

co(function *() {
	yield ugrid.init();

	var v = [[1, 2], [3, 4], [5, 6], [7, 8]];
	var frac = 0.5;
	var seed = 1;
	var key = 1;

	var r1 = yield ugrid.parallelize(v).sample(frac).lookup(key);

	// recreate partitions
	var P = ugrid.worker.length;	
	var part = {};
	for (var p = 0; p < P; p++)
		part[p] = []

	var p = 0;
	for (var i = 0; i < v.length; i++) {
		part[p].push(v[i]);
		p = (p + 1) % P;
	}

	// Reproduce same sampling locally
	var res = {
		v: {},
		len: {},
		rng: new ml.Random(seed)
	};

	for (var p in part) {
		res.v[p] = [];
		res.len[p] = 0;
		for (var i = 0; i < part[p].length; i++) {
			res.len[p]++;
			var current_frac = res.v[p].length / res.len[p];
			if (current_frac < frac)
				res.v[p].push(part[p][i]);
			else {
				var idx = Math.round(Math.abs(res.rng.next()) * res.len[p]);
				if (idx < res.v[p].length)
					res.v[p][idx] = part[p][i];
			}
		}
	}

	var tmp = [];
	for (var p in res.v)
		tmp = tmp.concat(res.v[p]);

	// local lookup
	var tmp2 = [];
	for (var i = 0; i < tmp.length; i++) {
		if (tmp[i][0] == key)
			tmp2.push(tmp[i]);
	}

	console.assert(r1.length == tmp2.length)
	for (var i = 0; i < r1.length; i++) {
		console.assert(r1[i][0] == tmp2[i][0]);
		console.assert(r1[i][1] == tmp2[i][1]);
	}

	ugrid.end();
})();
