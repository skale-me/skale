var assert = require('assert');
var fs = require('fs');
//var ut = require('../../test/ugrid-test.js');

var tmpdir = '/tmp/' + process.env.USER;

var files = [tmpdir + '/kv.data', tmpdir + '/kv2.data'];

var raw_data = [
	fs.readFileSync(files[0], {encoding: 'utf8'}),
	fs.readFileSync(files[1], {encoding: 'utf8'})
];
fs.writeFileSync(tmpdir + '/v0', raw_data[0]);
fs.writeFileSync(tmpdir + '/v1', raw_data[1]);

function textParser(s) {return s.split(' ').map(parseFloat);}

var v = [
	raw_data[0].split('\n').map(textParser),
	raw_data[1].split('\n').map(textParser)
];

var v_ref = [
	JSON.parse(JSON.stringify(v[0])),
	JSON.parse(JSON.stringify(v[1]))
];

// Helper functions for tests
function compareResults(r1, r2) {
	if (Array.isArray(r1)) sort(r1);
	if (Array.isArray(r2)) sort(r2);
	//console.assert(JSON.stringify(r1) == JSON.stringify(r2));
	assert.deepEqual(r1, r2);
}

function filter(e) {return e[1] % 2 === 0;}

function flatMapper(e) {return [e, e];}

function mapper(e) {e[1] *= 2; return e;}

function reducer(a, b) {
	if (Array.isArray(b[0]))
		a[0] += b[0].reduce(sum);
	else
		a[0] += b[0];

	if (Array.isArray(b[1])) {
		a[1] += b[1].reduce(function (a, b) {
			if (Array.isArray(b))
				return a + b.reduce(sum);
			return a + b;
		}, 0);
	} else
		a[1] += b[1];
	return a;

	function sum(a, b) {return a + b;}
}

function sort(v) {
	for (var i = 0; i < v.length; i++) {
		if (Array.isArray(v[i])) sort(v[i]);
	}
	v.sort();
}

function valueMapper(e) {return e * 2;}

function valueFlatMapper(e) {
	var i, out = [];
	for (i = e; i <= 5; i++) out.push(i);
	return out;
}

module.exports = {
	v: v,
	v_ref: v_ref,
	compareResults: compareResults,
	files: files,
	filter: filter,
	flatMapper: flatMapper,
	mapper: mapper,
	reducer: reducer,
	textParser: textParser,
	valueMapper: valueMapper,
	valueFlatMapper: valueFlatMapper
};
