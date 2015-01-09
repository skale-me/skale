#!/usr/local/bin/node --harmony
'use strict';

var co = require('co');
var fs = require('fs');
var ugrid = require('../../lib/ugrid-context.js')();
var ml = require('../../lib/ugrid-ml.js');

// Write textFile
var N = 10; 	//observations
var D = 4;  	//features

var rng = new ml.Random();
var file = '/tmp/data.txt';

for (var i = 0; i < N; i++) {
   var line = 2 * Math.round(Math.abs(rng.randn(1))) - 1;
   for (var j = 0; j < D; j++)
		line += ' ' + rng.randn(1).toString();
	line += '\n';
	fs.appendFile(file, line, function (err) {if (err) console.log(err);});
}

co(function *() {
	yield ugrid.init();

	function parse(e) {
		var tmp = e.split(' ').map(parseFloat);
		return {label: tmp.shift(), features: tmp};
	}

	var points = ugrid.textFile(file).map(parse, []).persist();
	var t0 = yield points.collect();
	console.log(t0);

	fs.unlink(file);
	ugrid.end();
})();
