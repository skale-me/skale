var fs = require('fs');
var spawn = require('child_process').spawn;
var assert = require('assert');
var ugrid = require('../');
var data = require('./support/data.js');
var local = require('./support/local.js');

var server, workerController, uc, ul;

beforeEach(function (done) {
	var output;
	if (uc === undefined) {
		server = spawn('./bin/ugrid.js');
		server.stdout.on('data', function (d) {
			var output2;
			if (output) return;
			output = true;
			workerController = spawn('./bin/worker.js');
			workerController.stdout.on('data', function (d) {
				if (output2) return;
				output2 = true;
				ul = local.context();
				uc = ugrid.context(done);
			});
		});
	} else done();
});

var sources = [
	[{name: 'parallelize', args: [data.v[0]]}],
	[{name: 'lineStream', args: []}, {name: 'map', args: [data.textParser]}],
];

var sources2 = [
	[{name: 'parallelize', args: [data.v[1]]}],
	[{name: 'lineStream', args: []}, {name: 'map', args: [data.textParser]}],
];

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
	{name: 'reduceByKey', args: [function (a, b) {return a + b;}, 0], sort: true},
	// {name: 'sample', args: [true, 0.1], sort: true},
	{name: 'values', args: []},
];

var transforms2 = [
	{name: 'coGroup', args: [], sort: true},
	{name: 'crossProduct', args: [], sort: true},
	{name: 'intersection', args: [], sort: true},
	{name: 'join', args: [], sort: true},
	{name: 'leftOuterJoin', args: [], sort: true},
	{name: 'rightOuterJoin', args: [], sort: true},
	{name: 'subtract', args: [], sort: true},
	{name: 'union', args: [], sort: true},
];

var actions = [
	{name: 'collect', args: []},
	{name: 'count', args: []},
	{name: 'countByValue', args: []},
	{name: 'lookup', args: [data.v[0][0][0]]},
	{name: 'reduce', args: [data.reducer, [0, 0]]},
// XXXXX TODO:
// take,
// takeOrdered,
// takeSample,
// top,
// foreach,
];

sources.forEach(function (source) {describe('uc.' + source[0].name + '()', function() {
	transforms.forEach(function (transform) {describe(transform.name ? '.' + transform.name + '()' : '/* empty */', function () {
		actions.forEach(function (action) {describe('.' + action.name + '()', function () {
			var lres, dres, sres;

			it('run local', function (done) {
				var args, loc;
				args = (source[0].name != 'lineStream') ? source[0].args :
					[].concat(fs.createReadStream(data.files[0], {encoding: 'utf8'}), {N: 5});
				loc = ul[source[0].name].apply(ul, args);
				if (source.length > 1 ) loc = loc[source[1].name].apply(loc, source[1].args);
				if (transform.name) loc = loc[transform.name].apply(loc, transform.args);
				args = [].concat(action.args, function (err, res) {lres = res; done();});
				loc[action.name].apply(loc, args);
			});

			it('run distributed', function (done) {
				assert(uc.worker.length > 0);
				var args, dist;
				args = (source[0].name != 'lineStream') ? source[0].args :
					[].concat(fs.createReadStream(data.files[0], {encoding: 'utf8'}), {N: 5});
				dist = uc[source[0].name].apply(uc, args);
				if (source.length > 1 ) dist = dist[source[1].name].apply(dist, source[1].args);
				if (transform.name) dist = dist[transform.name].apply(dist, transform.args);
				args = [].concat(action.args, function (err, res) {dres = res; done();});
				dist[action.name].apply(dist, args);
			});

			it('run distributed, stream output', function (done) {
				assert(uc.worker.length > 0);
				var args, out, dist;
				args = (source[0].name != 'lineStream') ? source[0].args :
					[].concat(fs.createReadStream(data.files[0], {encoding: 'utf8'}), {N: 5});
				dist = uc[source[0].name].apply(uc, args);
				if (source.length > 1 ) dist = dist[source[1].name].apply(dist, source[1].args);
				if (transform.name) dist = dist[transform.name].apply(dist, transform.args);
				args = [].concat(action.args, {stream: true});
				out = dist[action.name].apply(dist, args);
				sres = [];
				out.on('data', function (d) {sres.push(d);});
				out.on('end', done);
			});

			it('check distributed results', function () {
				if (transform.sort) data.compareResults(lres, dres);
				else assert.deepEqual(lres, dres);
			});

			it('check stream results', function () {
				if (transform.sort) data.compareResults([lres], sres);
				else assert.deepEqual([lres], sres);
			});
		});});
	});});

	sources2.forEach(function (source2) {
		transforms2.forEach(function (transform2) {describe('.' + transform2.name + '(uc.' + source2[0].name + '())', function () {
			actions.forEach(function (action) {describe('.' + action.name + '()', function () {
				var lres, dres, sres;

				it('run local', function (done) {
					var args, loc, loc2;
					args = (source[0].name != 'lineStream') ? source[0].args :
						[].concat(fs.createReadStream(data.files[0], {encoding: 'utf8'}), {N: 5});
					loc = ul[source[0].name].apply(ul, args);
					if (source.length > 1) loc = loc[source[1].name].apply(loc, source[1].args);
					args = (source2[0].name != 'lineStream') ? source2[0].args :
						[].concat(fs.createReadStream(data.files[1], {encoding: 'utf8'}), {N: 5});
					loc2 = ul[source2[0].name].apply(ul, args);
					if (source2.length > 1) loc2 = loc2[source2[1].name].apply(loc2, source2[1].args);
					args = [].concat(loc, transform2.args);
					loc2 = loc2[transform2.name].apply(loc2, args);
					args = [].concat(action.args, function (err, res) {lres = res; done();});
					loc2[action.name].apply(loc2, args);
				});

				it('run distributed', function (done) {
					assert(uc.worker.length > 0);
					var args, dist, dist2;
					args = (source[0].name != 'lineStream') ? source[0].args :
						[].concat(fs.createReadStream(data.files[0], {encoding: 'utf8'}), {N: 5});
					dist = uc[source[0].name].apply(uc, args);
					if (source.length > 1) dist = dist[source[1].name].apply(dist, source[1].args);
					args = (source2[0].name != 'lineStream') ? source2[0].args :
						[].concat(fs.createReadStream(data.files[1], {encoding: 'utf8'}), {N: 5});
					dist2 = uc[source2[0].name].apply(uc, args);
					if (source2.length > 1) dist2 = dist2[source2[1].name].apply(dist2, source2[1].args);
					args = [].concat(dist, transform2.args);
					dist2 = dist2[transform2.name].apply(dist2, args);
					args = [].concat(action.args, function (err, res) {dres = res; done();});
					dist2[action.name].apply(dist2, args);
				});

				it('run distributed, stream output', function (done) {
					assert(uc.worker.length > 0);
					var args, dist, dist2, out;
					args = (source[0].name != 'lineStream') ? source[0].args :
						[].concat(fs.createReadStream(data.files[0], {encoding: 'utf8'}), {N: 5});
					dist = uc[source[0].name].apply(uc, args);
					if (source.length > 1) dist = dist[source[1].name].apply(dist, source[1].args);
					args = (source2[0].name != 'lineStream') ? source2[0].args :
						[].concat(fs.createReadStream(data.files[1], {encoding: 'utf8'}), {N: 5});
					dist2 = uc[source2[0].name].apply(uc, args);
					if (source2.length > 1) dist2 = dist2[source2[1].name].apply(dist2, source2[1].args);
					args = [].concat(dist, transform2.args);
					dist2 = dist2[transform2.name].apply(dist2, args);
					args = [].concat(action.args, {stream: true});
					out = dist2[action.name].apply(dist2, args);
					sres = [];
					out.on('data', function (d) {sres.push(d);});
					out.on('end', done);
				});

				it('check distributed results', function () {
					if (transform2.sort) data.compareResults(lres, dres);
					else assert.deepEqual(lres, dres);
				});

				it('check stream results', function () {
					if (transform2.sort) data.compareResults([lres], sres);
					else assert.deepEqual([lres], sres);
				});
			});});
		});});
	});
});});
