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

function sample(v_in, P, withReplacement, frac, seed) {
	var v = JSON.parse(JSON.stringify(v_in));

	function split(a, n) {
		var len = a.length, out = [], i = 0;
		while (i < len) {
			var size = Math.ceil((len - i) / n--);
			out.push(a.slice(i, i += size))
		}
		return out;
	}	
	var map = split(v_in, P);

	var workerMap = [];
	for (var i = 0; i < P; i++) {
		workerMap[i] = {};
		workerMap[i][i] = map[i];
	}

	var v_out = [];
	for (var w = 0; w < P; w++) {
		var p = 0;
		var tmp = [];
		var rng = new ml.Random(seed);
		for (var i in workerMap[w]) {
			var L = workerMap[w][i].length;
			var L = Math.ceil(L * frac);
			tmp[p] = {data: []};
			var idxVect = [];
			while (tmp[p].data.length != L) {
				var idx = Math.round(Math.abs(rng.next()) * (L - 1));
				if ((idxVect.indexOf(idx) != -1) &&  !withReplacement) 
					continue;	// if already picked but no replacement mode
				idxVect.push[idx];
				tmp[p].data.push(workerMap[w][i][idx]);
			}
			v_out = v_out.concat(tmp[p].data)			
			p++;
		}
	}

	return v_out;
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

function crossProduct(v1_in, v2_in) {
	var v1 = JSON.parse(JSON.stringify(v1_in));
	var v2 = JSON.parse(JSON.stringify(v2_in));	
	var v3 = [];
	var already_v1 = [];
	var already_v2 = [];	

	for (var i = 0; i < v1.length; i++)
		for (var j = 0; j < v2.length; j++)
			v3.push([v1[i], v2[j]])
	return v3;
}

function distinct(v_in) {
	var v = JSON.parse(JSON.stringify(v_in));
	var v_out = [], v_ref = [];
	for (var i = 0; i < v.length; i++) {
		if (v_ref.indexOf(JSON.stringify(v[i])) != -1) continue;
		v_ref.push(JSON.stringify(v[i]));
		v_out.push(v[i]);
	}
	return v_out;
}

function intersection(v1_in, v2_in) {
	var v1 = JSON.parse(JSON.stringify(v1_in));
	var v2 = JSON.parse(JSON.stringify(v2_in));

	var v_ref = [];
	for (var i = 0; i < v1.length; i++) {
		var e = JSON.stringify(v1[i]);
		if (v_ref.indexOf(e) != -1) continue;
		for (var j = 0; j < v2.length; j++) {
			if (JSON.stringify(v2[j]) == e) {
				v_ref.push(e);
				break;
			}
		}
	}
	return v_ref.map(JSON.parse)
}

function substract(v1_in, v2_in) {
	var v1 = JSON.parse(JSON.stringify(v1_in));
	var v2 = JSON.parse(JSON.stringify(v2_in));

	var v_ref = [];
	for (var i = 0; i < v1.length; i++) {
		var e = JSON.stringify(v1[i]);
		var found = false;
		for (var j = 0; j < v2.length; j++)
			if (JSON.stringify(v2[j]) == e) {
				found = true;
				break;
			}
		if (!found)
			v_ref.push(e);
	}
	return v_ref.map(JSON.parse)
}

function countByValue(v_in) {
	var v = JSON.parse(JSON.stringify(v_in));

	var tmp = {};
	for (var i = 0; i < v.length; i++) {
		var str = JSON.stringify(v[i]);
		if (tmp[str] == undefined) tmp[str] = [v[i], 0];
		tmp[str][1]++;
	}

	var v_out = [];
	for (var i in tmp)
		v_out.push(tmp[i]);
	return v_out;
}

function arrayEqual(a1, a2) {
	return JSON.stringify(a1) === JSON.stringify(a2);
}   

module.exports.countByValue = countByValue;
module.exports.randomSVMData = randomSVMData;
module.exports.sample = sample;
module.exports.groupByKey = groupByKey;
module.exports.reduceByKey = reduceByKey;
module.exports.union = union;
module.exports.join = join;
module.exports.coGroup = coGroup;
module.exports.crossProduct = crossProduct;
module.exports.distinct = distinct;
module.exports.intersection = intersection;
module.exports.substract = substract;
module.exports.arrayEqual = arrayEqual;