#!/usr/bin/env node

var sc = require('skale').context();

//sc.range(10).map(a => a * 2).collect().toArray().then(console.log);

//sc.range(10, -5, -3).collect().toArray().then(console.log);

//sc.range(-4, 3).map(function (a) {return a*2}).collect().toArray(function(err, res) {
//sc.range(-4, 3).map(a => a*2).collect().toArray(function(err, res) {
sc.range(4).map(a => a*2).reduce((a,b)=>a+b, 0, function(err, res) {
	console.log(res);
	sc.end();
})
