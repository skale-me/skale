var thunkify = require('thunkify');

// Generate P partitions of N SVM data vectors containing D features 
function loadTestData(N, D, P) {
	var res = [];
	if ((P == undefined) || (P == 0))
		P = 1;
	for (var p = 0; p < P; p++) {
		var output = [];
		for (var i = 0; i < N; i++) {
			var array = [];
			for (var j = 0; j < D; j++)
				array[j] = Math.random();
			output[i] = {label: Math.round(Math.random()) * 2 - 1, features: array};
		}
		res[p] = output;
	}
	return res;
}

function logisticLossGradient(p, w) {
	// var grad = new Array(p.features.length);	// plus lent que ci-dessous
	var grad = [];								// plus rapide que ci-dessus
	var dot_prod = 0;
	for (var i = 0; i < p.features.length; i++)
		dot_prod += p.features[i] * w[i];

	var tmp = (1 / (1 + Math.exp(-p.label * dot_prod)) - 1) * p.label;

	// Plus rapide que ci-dessous
	for (var i = 0; i < p.features.length; i++)
		grad[i] = p.features[i] * tmp;
	return grad;

	// Plus lent que ci-dessus
	// return p.features.map(function (e) {
	// 	return e * tmp;
	// });
}

// function logisticLossGradient(p, w) {
// 	var grad = [];								// plus rapide que ci-dessus

// 	// Plus rapide que ci-dessous
// 	for (var i = 0; i < p.features.length; i++) {
// 		var dot_prod = 0;
// 		for (var j = 0; j < p.features.length; j++)
// 			dot_prod += p.features[j] * w[j];
// 		grad[i] = p.features[i] * (1 / (1 + Math.exp(-p.label * dot_prod)) - 1) * p.label;
// 	}
// 	return grad;
// }

function sum(a, b) {
	for (var i = 0; i < b.length; i++)
		a[i] += b[i];
	return a;
}


// Version pour reproduire resultats de spark localFileLR
function parseSVMlikeSpark(line) {
	var tmp = line[Object.keys(line)[0]].split(" ");
	return {
		label: parseFloat(tmp[0]), 
		features: tmp.splice(1, tmp.length).map(parseFloat)
	}
}

function parseSVM(line) {
	var tmp = line[Object.keys(line)[0]].split(" ");
	return {
		label: parseFloat(tmp[0]) * 2 - 1, 
		features: tmp.splice(1, tmp.length).map(parseFloat)
	}
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
	var seed = initSeed || 1;

	this.next = function() {
	    var x = Math.sin(seed++) * 10000;
	    return (x - Math.floor(x)) * 2 - 1;
	}

	this.reset = function() {
		seed = initSeed;
	}

	this.randn = function(N) {
		var w = new Array(N);
		for (var i = 0; i < N; i++) 
			w[i] = this.next();
		return w;
	}
}
module.exports.Random = Random;


module.exports.loadTestData = loadTestData;
module.exports.logisticLossGradient = logisticLossGradient;
module.exports.sum = sum;
module.exports.parseSVM = parseSVM;
module.exports.parseSVMlikeSpark = parseSVMlikeSpark;
module.exports.randn = randn;
module.exports.zeros = zeros;
