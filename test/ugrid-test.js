var ml = require('../lib/ugrid-ml.js');

/** local version of randomSVMData  (for testing) */
function randomSVMData(N, D, seed, nPartitions) {
	var rng = new ml.Random(seed);
	var res = [], tmp = [];
	var P = nPartitions || 1;

	for (var p = 0; p < P; p++)
		res[p] = [];
	p = 0;
	for (var i = 0; i < N; i++) {
		res[p].push(ml.randomSVMLine(rng, D));
		p = (p == (P - 1)) ? 0 : p + 1;
	}
	for (var p in res) 
		tmp = tmp.concat(res[p]);
	return tmp;
}

function sample(v_in, P, frac, seed) {
	var v = JSON.parse(JSON.stringify(v_in));

	// Create partitions
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

	return tmp;
}

function groupByKey(v_in) {
	var v = JSON.parse(JSON.stringify(v_in));

	var keys = [];
	for (var i = 0; i < v.length; i++)
		if (keys.indexOf(v[i][0]) == -1)
			keys.push(v[i][0]);

	var res = [];
	for (var i = 0; i < keys.length; i++) 
		res.push([keys[i], []]);
	for (var i = 0; i < v.length; i++) {
		var idx = keys.indexOf(v[i][0]);
		res[idx][1].push(v[i][1]);
	}
	return res;
}

function reduceByKey(v_in, reducer, init) {
	var v = JSON.parse(JSON.stringify(v_in));

	var keys = [];
	for (var i = 0; i < v.length; i++)
		if (keys.indexOf(v[i][0]) == -1)
			keys.push(v[i][0]);

	var res = [];
	for (var i = 0; i < keys.length; i++)
		res.push([keys[i], init]);
	for (var i = 0; i < v.length; i++) {
		var idx = keys.indexOf(v[i][0]);
		res[idx][1] = reducer(res[idx][1], v[i][1]);
	}
	return res;
}

function union(v1_in, v2_in) {
	var v1 = JSON.parse(JSON.stringify(v1_in));
	var v2 = JSON.parse(JSON.stringify(v2_in));	
	return v1.concat(v2);
}

function join(v1_in, v2_in) {
	var v1 = JSON.parse(JSON.stringify(v1_in));
	var v2 = JSON.parse(JSON.stringify(v2_in));	
	var v3 = [];
	for (var i = 0; i < v1.length; i++)
		for (var j = 0; j < v2.length; j++)
			if (v1[i][0] == v2[j][0])
				v3.push([v1[i][0], [v1[i][1], v2[j][1]]])
	return v3;
}

function coGroup(v1_in, v2_in) {
	var v1 = JSON.parse(JSON.stringify(v1_in));
	var v2 = JSON.parse(JSON.stringify(v2_in));	
	var v3 = [];
	var already_v1 = [];
	var already_v2 = [];	

	for (var i = 0; i < v1.length; i++)
		for (var j = 0; j < v2.length; j++)
			if (v1[i][0] == v2[j][0]) {
				var idx = -1;
				for (var k = 0; k < v3.length; k++) {
					if (v3[k][0] == v1[i][0]) {
						idx = k;
						break;
					}
				}
				if (idx == -1) {
					idx = v3.length;
					v3[v3.length] = [v1[i][0], [[], []]];
				} 
				if (!already_v1[i]) {
					v3[idx][1][0].push(v1[i][1]);
					already_v1[i] = true;
				}
				if (!already_v2[j]) {
					v3[idx][1][1].push(v2[j][1]);
					already_v2[j] = true;
				}
			}
	return v3;
}

function arrayEqual(a1, a2) {
	return JSON.stringify(a1) === JSON.stringify(a2);
}   

module.exports.randomSVMData = randomSVMData;
module.exports.sample = sample;
module.exports.groupByKey = groupByKey;
module.exports.reduceByKey = reduceByKey;
module.exports.union = union;
module.exports.join = join;
module.exports.coGroup = coGroup;
module.exports.arrayEqual = arrayEqual;