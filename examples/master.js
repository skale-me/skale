#!/usr/local/bin/node --harmony

var co = require('co');
var ugrid = require('../lib/ugrid-context.js')({host: 'localhost', port: 12346});

co(function *() {
	yield ugrid.init();

	// var lines = ugrid.textFile('data.txt');				// Need to reimplement load file
	var lines = ugrid.loadTestData(4, 2).persist();

	var count = yield lines.count();
	console.log('lines.count() : ' + count);

	var collect = yield lines.collect();
	console.log('\nlines.collect() :');
	console.log(collect);

	function reducer(a, b) {
		for (var i = 0; i < b.features.length; i++)
			a.features[i] += b.features[i];
		return a;
	}
	var reduce = yield lines.reduce(reducer, {label: 1, features: [0, 0]});
	console.log('\nlines.reduce(reducer, initVal) :');
	console.log(reduce);

	// Lookup is valid for key/value tuples, need to build a special dataset
	// var lookup = yield lines.lookup(0, true);
	// console.log('\nlines.lookup(0) : ' + lookup);

	function mapper(a) {
		var o = JSON.parse(JSON.stringify(a));
		o.new_field = "mapped";
		return o;
	}
	var mappedLines = yield lines.map(mapper, []).collect();
	console.log('\nlines.map(mapper).collect() :');
	console.log(mappedLines);

	var unionLines = yield lines.union(lines).collect();
	console.log('\nlines.union(lines).collect() :');
	console.log(unionLines);

/*
	function flatMapper(a) {
		var res = [];
		for (var i = 0; i < 2; i++) {
			// WARNING, make a copy of the object here
			var o = JSON.parse(JSON.stringify(a));
			o.newField = "flatMapped " + i;
			res.push(o);
		}
		return res;
	}
	var flatMappedLines = yield lines.flatMap(flatMapper, true);
	console.log('\nlines.flatMap(myFlatMapper).collect() :');
	yield flatMappedLines.print();

	var sampledLines = yield lines.sample(0.5, true);
	console.log('\nlines.sample(0.5).collect() :');
	yield sampledLines.print();

	var unionLinesGroupedByKey = yield unionLines.groupByKey(true);
	console.log('\nunionLines.groupByKey().collect() :');
	yield unionLinesGroupedByKey.print();
*/
	ugrid.end();
})();
