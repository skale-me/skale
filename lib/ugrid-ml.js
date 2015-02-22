'use strict';

function randomSVMLine(rng, D) {
	// return {label: Math.round(Math.abs(rng.next())) * 2 - 1, features: rng.randn(D)};
	var data = rng.randn(D + 1);
	data[0] = Math.round(Math.abs(data[0])) * 2 - 1; 
	return data;
}

/** local version of randomSVMData  (for testing) */
function randomSVMData(N, D, seed) {
	var res = [], rng = new Random(seed);
	for (var i = 0; i < N; i++)
		res[i] = randomSVMLine(rng, D);
	return res;
}

/** local version of sample  (for testing), same algo as distributed */
function sample(v, frac, P) {
	P = P || 1;
	var seed = 1, part = {}, tmp = [], res, p, i, curFrac, idx;
	// partitions
	for (p = 0; p < P; p++)
		part[p] = [];
	p = 0;
	for (i = 0; i < v.length; i++) {
		part[p].push(v[i]);
		p = (p + 1) % P;
	}
	// sampling on each partition
	res = {v: {}, len: {}, rng: new Random(seed)};
	for (p in part) {
		res.v[p] = [];
		res.len[p] = 0;
		for (i = 0; i < part[p].length; i++) {
			res.len[p]++;
			curFrac = res.v[p].length / res.len[p];
			if (curFrac < frac) {
				res.v[p].push(part[p][i]);
			} else {
				idx = Math.round(Math.abs(res.rng.next()) * res.len[p]);
				if (idx < res.v[p].length)
					res.v[p][idx] = part[p][i];
			}
		}
	}
	for (p in res.v)
		tmp = tmp.concat(res.v[p]);
	return tmp;
}

// Generate P partitions of N SVM data vectors containing D features
function loadTestData(N, D, nPartitions, seed) {
	var rng = new Random(seed);
	var res = [];
	var P = nPartitions || 1;
	for (var p = 0; p < P; p++) {
		res[p] = [];
		for (var i = 0; i < N; i++)
			res[p][i] = {label: Math.round(Math.abs(rng.next())) * 2 - 1, features: rng.randn(D)};
	}
	return res;
}

function logisticLossGradient(p, w) {
	var grad = [], dot_prod = 0;

	for (var i = 1; i < p.length; i++)
		dot_prod += p[i] * w[i - 1];

	// var tmp = (1 / (1 + Math.exp(-p.label * dot_prod)) - 1) * p.label;	// Previous version
	var t2 = Math.exp(-p[0] * dot_prod);									// More numerically stable

	var tmp = -t2 / (1 + t2) * p[0];
	var loss = Math.log(1 + t2);

	for (i = 1; i < p.length; i++)
		grad[i - 1] = p[i] * tmp;
	return grad;
}

// function logisticLossGradient(p, w) {
// 	var grad = [0], dot_prod = 0;

// 	for (var i = 1; i < p.length; i++)
// 		dot_prod += p[i] * w[i];

// 	// // var tmp = (1 / (1 + Math.exp(-p.label * dot_prod)) - 1) * p.label;	// Previous version
// 	var t2 = Math.exp(-p[0] * dot_prod);									// More numerically stable

// 	var tmp = -t2 / (1 + t2) * p[0];
// 	var loss = Math.log(1 + t2);

// 	for (i = 1; i < p.length; i++)
// 		grad[i] = p[i] * tmp;
// 	return grad;
// }

// Logistic Regression
// function logisticLossGradient(p, w) {
// 	var grad = [], dot_prod = 0;

// 	for (var i = 0; i < p.features.length; i++)
// 		dot_prod += p.features[i] * w[i];

// 	// var tmp = (1 / (1 + Math.exp(-p.label * dot_prod)) - 1) * p.label;	// Previous version
// 	var t2 = Math.exp(-p.label * dot_prod);									// More numerically stable
// 	var tmp = -t2 / (1 + t2) * p.label;

// 	var loss = Math.log(1 + t2);

// 	for (i = 0; i < p.features.length; i++)
// 		grad[i] = p.features[i] * tmp;
// 	return grad;
// }

//~ function logisticLossGradient(p, w) {
	//~ return [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1];
//~ }

function sum(a, b) {
	for (var i = 0; i < b.length; i++)
		a[i] += b[i];
	return a;
}

// Kmeans
function closestSpectralNorm(element, means) {
	var smallestSn = Infinity;
	var smallestSnIdx = 0;
	for (var i = 0; i < means.length; i++) {
		var sn = 0;
		for (var j = 0; j < element.features.length; j++)
			sn += Math.pow(element.features[j] - means[i][j], 2);
		if (sn < smallestSn) {
			smallestSnIdx = i;
			smallestSn = sn;
		}
	}
	return {label: element.label, features: element.features, cluster: smallestSnIdx, sum: 1};
}

function accumulate(a, b) {
	a.sum += b.sum;
	for (var i = 0; i < b.features.length; i++)
		a.acc[i] += b.features[i];
	return a;
}

// Version pour reproduire resultats de spark localFileLR
function parseSVMlikeSpark(line) {
	var tmp = line[Object.keys(line)[0]].split(" ");
	return {
		label: parseFloat(tmp[0]),
		features: tmp.splice(1, tmp.length).map(parseFloat)
	};
}

function parseSVM(line) {
	var tmp = line[Object.keys(line)[0]].split(" ");
	return {
		label: parseFloat(tmp[0]) * 2 - 1,
		features: tmp.splice(1, tmp.length).map(parseFloat)
	};
}

function randn(N) {
	var w = new Array(N);
	for (var i = 0; i < N; i++)
		w[i] = Math.random() * 2 - 1;
	return w;
}

function zeros(N) {
	var w = new Array(N);
	for (var i = 0; i < N; i++)
		w[i] = 0;
	return w;
}

/*
	Random(initSeed)
		Simple seeded random number generator
	Methods:
		- Random.next(): Generates a number x, so as -1 < x < 1
		- Random.reset(): Reset seed to initial seed value
*/
function Random(initSeed) {
	this.seed = initSeed || 1;

	this.next = function () {
	    var x = Math.sin(this.seed++) * 10000;
	    return (x - Math.floor(x)) * 2 - 1;
	};

	this.reset = function () {
		this.seed = initSeed;
	};

	this.randn = function (N) {
		var w = new Array(N);
		for (var i = 0; i < N; i++)
			w[i] = this.next();
		return w;
	};
}

module.exports.Random = Random;

module.exports.randomSVMLine = randomSVMLine;
module.exports.randomSVMData = randomSVMData;
module.exports.loadTestData = loadTestData;
module.exports.sample = sample;
module.exports.logisticLossGradient = logisticLossGradient;
module.exports.sum = sum;
module.exports.parseSVM = parseSVM;
module.exports.parseSVMlikeSpark = parseSVMlikeSpark;
module.exports.randn = randn;
module.exports.zeros = zeros;
module.exports.closestSpectralNorm = closestSpectralNorm;
module.exports.accumulate = accumulate;
