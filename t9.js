#!/usr/local/bin/node --harmony

var fs = require('fs');
var trace = require('line-trace');
//var ugrid = require('./test/support/local.js');
var ugrid = require('./');

function textParser(s) {return s.split(' ').map(parseFloat);}

var uc = ugrid.context(function () {
	//var s = fs.createReadStream('/tmp/marc/kv.data', {encoding: 'utf8'});
	//var s1 = fs.createReadStream('/tmp/marc/kv.data', {encoding: 'utf8'});

	var a = uc.parallelize([[1,1], [1,1], [2,3], [2,4], [3,5]]);

	//a.collect(function (err, res) {
	//	trace(res);
	//});

	a = a.sample(true, 0.1);

	a.collect(function (err, res) {
		trace(res);
	});

	//a = uc.lineStream(s, {N: 5});

	//a = a.map(textParser);

	//a.collect(function (err, res) {
	//	trace(res);
	//});

	//a.collect(function (err, res) {
	//	trace(res);
	//	process.exit(0);
	//});
});
