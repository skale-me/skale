#!/usr/bin/env node

var v = [7, 5, 3, 8, 0, 2, 4, 9, 1, 6]
var sampled = [9, 2, 1];
var nPartitions = 5;

var rp = new RangePartitioner(nPartitions, sampled);
var vr = []; 
for (var i = 0; i < v.length; i++)
	vr[i] = rp.getPID(v[i])

console.log(sampled)
console.log(v)
console.log(vr)

function RangePartitioner(nPartitions, sampled) {
	sampled.sort();								// sort sampled dataset
	var nUpperBounds = nPartitions - 1;
	this.upperbounds = [];
	if (sampled.length <= nUpperBounds) {
		this.upperbounds = sampled;	// supprimer les doublons peut-etre ici
	} else {
		var s = Math.floor(sampled.length / nPartitions);
		for (var i = 0; i < nUpperBounds; i++) this.upperbounds.push(sampled[s * (i + 1)]);
	}

	this.getPID = function(data) {
		for (var i = 0; i < this.upperbounds.length; i++)
			if (data < this.upperbounds[i]) break;
		return i;
	}	
}