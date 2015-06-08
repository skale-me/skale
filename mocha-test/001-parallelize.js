var assert = require('assert');
var ugrid = require('../');
var data = require('./support/data.js');
var local = require('./support/local.js');

var uc, ul;

describe('master', function () {
	it('connects to the server', function (done) {
		uc = ugrid.context(done);
	})
	it('gets workers', function () {
		assert(uc.worker.length > 0);
	})
})

describe('local', function () {
	it('starts a local context', function () {
		ul = local.context();
	})
})

var transforms = [
	{name: ''},
	{name: 'distinct', args: [], sort: true},
	{name: 'filter', args: [data.filter]},
	{name: 'flatMap', args: [data.flatMapper]},
	{name: 'flatMapValues',	args: [data.valueFlatMapper]},
	{name: 'groupByKey', args: [], sort: true},
	{name: 'keys', args: []},
	{name: 'map', args: [data.mapper]},
	{name: 'mapValues', args: [data.valueMapper]},
	{name: 'persist', args: []},
	{name: 'reduceByKey', args: [function (a, b) {return a+b;}, 0]},
	{name: 'sample', args: [true, 0.1]},
	{name: 'values', args: []}
];

var actions = [
	{name: 'collect', args: []},
    {name: 'count', args: []},
    {name: 'countByValue', args: []},
	{name: 'lookup', args: [data.v[0][0][0]]},
	{name: 'reduce', args: [data.reducer, [0, 0]]},
];

describe('uc.parallelize()', function() {
	transforms.forEach(function (transform) {
		describe(transform.name ? '.' + transform.name + '()' : '', function () {
			actions.forEach(function (action) {
				describe('.' + action.name + '()', function () {
					var lres, dres, sres;

					it('run local', function (done) {
						var args, loc = ul.parallelize(data.v[0]);
						if (transform.name) loc = loc[transform.name].apply(loc, transform.args);
						args = [].concat(action.args, function (err, res) {lres = res; done();});
						loc[action.name].apply(loc, args);
					})

					it('run distributed', function (done) {
						var args, dist = uc.parallelize(data.v[0]);
						if (transform.name) dist = dist[transform.name].apply(dist, transform.args);
						args = [].concat(action.args, function (err, res) {dres = res; done();});
						dist[action.name].apply(dist, args);
					})

					it('run distributed, stream output', function (done) {
						var args, dist = uc.parallelize(data.v[0]), out;
						if (transform.name) dist = dist[transform.name].apply(dist, transform.args);
						args = [].concat(action.args, {stream: true});
						out = dist[action.name].apply(dist, args);
						sres = [];
						out.on("data", function (d) {sres.push(d);});
						out.on("end", done);
					})

					it('check distributed results', function () {
						if (transform.sort) data.compareResults(lres, dres);
						else assert.deepEqual(lres, dres);
					})

					it('check stream results', function () {
						if (transform.sort) data.compareResults([lres], sres);
						else assert.deepEqual([lres], sres);
					})
				})
			});
		});
	});
})
