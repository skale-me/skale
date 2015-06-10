#!/usr/local/bin/node --harmony

console.log("\n" + "# stream stream union count");

var co = require('co');
var fs = require('fs');
var ugrid = require('../../');
var ut = require('../ugrid-test.js');

var tmpdir = '/tmp/' + process.env.USER;

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var raw_data = [
		fs.readFileSync(tmpdir + '/kv.data', {encoding: 'utf8'}),
		fs.readFileSync(tmpdir + '/kv2.data', {encoding: 'utf8'})
	];
	fs.writeFileSync(tmpdir + '/v0', raw_data[0]);
	fs.writeFileSync(tmpdir + '/v1', raw_data[1]);

	var s = [
		fs.createReadStream(tmpdir + '/kv.data', {encoding: 'utf8'}),
		fs.createReadStream(tmpdir + '/kv2.data', {encoding: 'utf8'})
	];

	var v = [
		raw_data[0].split('\n').map(textParser),
		raw_data[1].split('\n').map(textParser)
	];

	var v_ref = [
		JSON.parse(JSON.stringify(v[0])),
		JSON.parse(JSON.stringify(v[1]))
	];

	var dsource = [], lsource = [];

	var frac = 0.1;
	var seed = 1;
	var withReplacement = true;
	var key = v_ref[0][0][0];

	function textParser(s) {return s.split(' ').map(parseFloat);}

	function reducer(a, b) {
		if (Array.isArray(b[0]))
			a[0] += b[0].reduce(function (a, b) {return a + b;});
		else
			a[0] += b[0];

		if (Array.isArray(b[1])) {
			a[1] += b[1].reduce(function (a, b) {
				if (Array.isArray(b))
					return a + b.reduce(function(a, b) {return a + b;});
				return a + b;
			}, 0);
		} else
			a[1] += b[1];
		return a;
	}

	function mapper(e) {
		e[1] *= 2;
		return e;
	}

	function filter(e) {return (e[1] % 2 === 0);}

	function flatMapper(e) {return [e, e];}

	function valueMapper(e) {return e * 2;}

	function reducerByKey(a, b) {
		a += b;
		return a;
	}

	function valueFlatMapper(e) {
		var out = [];
		for (var i = e; i <= 5; i++)
			out.push(i);
		return out;
	}

	function sort(v) {
		for (var i = 0; i < v.length; i++) {
			if (Array.isArray(v[i]))
				sort(v[i]);
		}
		v.sort();
	}

	function compareResults(r1, r2) {
		console.log('=> local result: %j', r1);
		console.log('=> distributed result: %j', r2);
		if (Array.isArray(r1)) sort(r1);
		if (Array.isArray(r2)) sort(r2);
		console.assert(JSON.stringify(r1) == JSON.stringify(r2));
		uc.end();
	}

	lsource[0] = v_ref[0];
	lsource[1] = v_ref[1];
	lsource[1] = ut.union(lsource[1], lsource[0]);
	var loc = lsource[1].length;
	
	dsource[0] = uc.lineStream(s[0], {N: 5}).map(textParser);
	dsource[1] = uc.lineStream(s[1], {N: 5}).map(textParser);
	dsource[1] = dsource[1].union(dsource[0]);
	var dist = yield dsource[1].count();
	
	compareResults(loc, dist);

}).catch(ugrid.onError);

