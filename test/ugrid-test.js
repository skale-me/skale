var fs = require('fs');
var spawn = require('child_process').spawn;
var assert = require('assert');
var trace = require('line-trace');
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
	[{name: 'textFile', args: [data.files[0]]}, {name: 'map', args: [data.textParser]}],
];

var sources2 = [
	[{name: 'parallelize', args: [data.v[1]]}],
	[{name: 'lineStream', args: []}, {name: 'map', args: [data.textParser]}],
	[{name: 'textFile', args: [data.files[1]]}, {name: 'map', args: [data.textParser]}],
];

var transforms = [
	{name: ''},
	{name: 'distinct', args: [], sort: true},
	{name: 'filter', args: [data.filter]},
	{name: 'flatMap', args: [data.flatMapper]},
	{name: 'flatMapValues',	args: [data.valueFlatMapper]},
	{name: 'groupByKey', args: [], sort: true},
//	{name: 'keys', args: []},
	{name: 'map', args: [data.mapper]},
	{name: 'mapValues', args: [data.valueMapper]},
//	{name: 'persist', args: []},
	{name: 'reduceByKey', args: [function (a, b) {return a + b;}, 0], sort: true},
//	{name: 'sample', args: [true, 0.1], sort: true},
//	{name: 'values', args: []},
];

var dualTransforms = [
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
	{name: 'countByValue', args: [], sort: true},
	{name: 'lookup', args: [data.v[0][0][0]]},
	{name: 'reduce', args: [data.reducer, [0, 0]]},
	{name: 'take', args: [2], lengthOnly: true},
	{name: 'takeOrdered', args: [2, function (a, b) {return a < b;}]},
	{name: 'top', args: [2]},
// XXXXX TODO:
// takeSample,
// foreach,
];

sources.forEach(function (source) {describe('uc.' + source[0].name + '()', function() {
	transforms.forEach(function (transform) {describe(transform.name ? '.' + transform.name + '()' : '/* empty */', function () {
		actions.forEach(function (action) {describe('.' + action.name + '()', function () {
			var lres, dres, sres, pres1, pres2, check = {};

			if (transform.sort || action.sort) check.sort = true;
			if (transform.lengthOnly || action.lengthOnly) check.lengthOnly = true;

			it('run local', function (done) {
				var args, rdd;
				args = (source[0].name != 'lineStream') ? source[0].args :
					[].concat(fs.createReadStream(data.files[0], {encoding: 'utf8'}), {N: 5});
				rdd = ul[source[0].name].apply(ul, args);
				if (source.length > 1 ) rdd = rdd[source[1].name].apply(rdd, source[1].args);
				if (transform.name) rdd = rdd[transform.name].apply(rdd, transform.args);
				//args = [].concat(action.args, function (err, res) {trace(res);lres = res; done();});
				args = [].concat(action.args, function (err, res) {lres = res; done();});
				rdd[action.name].apply(rdd, args);
			});

			it('run distributed', function (done) {
				assert(uc.worker.length > 0);
				var args, rdd;
				args = (source[0].name != 'lineStream') ? source[0].args :
					[].concat(fs.createReadStream(data.files[0], {encoding: 'utf8'}), {N: 5});
				rdd = uc[source[0].name].apply(uc, args);
				if (source.length > 1 ) rdd = rdd[source[1].name].apply(rdd, source[1].args);
				if (transform.name) rdd = rdd[transform.name].apply(rdd, transform.args);
				//args = [].concat(action.args, function (err, res) {trace(res);dres = res; done();});
				args = [].concat(action.args, function (err, res) {dres = res; done();});
				rdd[action.name].apply(rdd, args);
			});
//
			it('run distributed, pre-persist', function (done) {
				assert(uc.worker.length > 0);
				var args, rdd;
				args = (source[0].name != 'lineStream') ? source[0].args :
					[].concat(fs.createReadStream(data.files[0], {encoding: 'utf8'}), {N: 5});
				rdd = uc[source[0].name].apply(uc, args);
				if (source.length > 1 ) rdd = rdd[source[1].name].apply(rdd, source[1].args);
				rdd = rdd.persist();
				if (transform.name) rdd = rdd[transform.name].apply(rdd, transform.args);
				args = [].concat(action.args, function (err, res) {pres1 = res; done();});
				rdd[action.name].apply(rdd, args);
			});

			it('run distributed, post-persist', function (done) {
				assert(uc.worker.length > 0);
				var args, rdd;
				args = (source[0].name != 'lineStream') ? source[0].args :
					[].concat(fs.createReadStream(data.files[0], {encoding: 'utf8'}), {N: 5});
				rdd = uc[source[0].name].apply(uc, args);
				if (source.length > 1 ) rdd = rdd[source[1].name].apply(rdd, source[1].args);
				if (transform.name) rdd = rdd[transform.name].apply(rdd, transform.args);
				rdd = rdd.persist();
				args = [].concat(action.args, function (err, res) {pres2 = res; done();});
				rdd[action.name].apply(rdd, args);
			});

			it('run distributed, stream output', function (done) {
				assert(uc.worker.length > 0);
				var args, out, rdd;
				args = (source[0].name != 'lineStream') ? source[0].args :
					[].concat(fs.createReadStream(data.files[0], {encoding: 'utf8'}), {N: 5});
				rdd = uc[source[0].name].apply(uc, args);
				if (source.length > 1 ) rdd = rdd[source[1].name].apply(rdd, source[1].args);
				if (transform.name) rdd = rdd[transform.name].apply(rdd, transform.args);
				args = [].concat(action.args, {stream: true});
				out = rdd[action.name].apply(rdd, args);
				sres = [];
				out.on('data', function (d) {sres.push(d);});
				out.on('end', done);
			});
//
			it('check distributed results', function () {
				data.compareResults(lres, dres, check);
			});
//
			it('check distributed pre-persist results', function () {
				data.compareResults(lres, pres1, check);
			});

			it('check distributed post-persist results', function () {
				data.compareResults(lres, pres2, check);
			});

			it('check stream results', function () {
				data.compareResults([lres], sres, check);
			});
//
		});});
	});});

	sources2.forEach(function (source2) {
		dualTransforms.forEach(function (dualTransform) {describe('.' + dualTransform.name + '(uc.' + source2[0].name + '())', function () {
			actions.forEach(function (action) {describe('.' + action.name + '()', function () {
				var lres, dres, sres, pres1, pres2, check = {};

				if (dualTransform.sort || action.sort) check.sort = true;
				if (dualTransform.lengthOnly || action.lengthOnly) check.lengthOnly = true;

				it('run local', function (done) {
					var args, rdd, other;
					args = (source[0].name != 'lineStream') ? source[0].args :
						[].concat(fs.createReadStream(data.files[0], {encoding: 'utf8'}), {N: 5});
					rdd = ul[source[0].name].apply(ul, args);
					if (source.length > 1) rdd = rdd[source[1].name].apply(rdd, source[1].args);
					args = (source2[0].name != 'lineStream') ? source2[0].args :
						[].concat(fs.createReadStream(data.files[1], {encoding: 'utf8'}), {N: 5});
					other = ul[source2[0].name].apply(ul, args);
					if (source2.length > 1) {
						other = other[source2[1].name].apply(other, source2[1].args);
					}
					args = [].concat(other, dualTransform.args);
					rdd = rdd[dualTransform.name].apply(rdd, args);
					args = [].concat(action.args, function (err, res) {lres = res; done();});
					rdd[action.name].apply(rdd, args);
				});

				it('run distributed', function (done) {
					assert(uc.worker.length > 0);
					var args, rdd, other;
					args = (source[0].name != 'lineStream') ? source[0].args :
						[].concat(fs.createReadStream(data.files[0], {encoding: 'utf8'}), {N: 5});
					rdd = uc[source[0].name].apply(uc, args);
					if (source.length > 1) rdd = rdd[source[1].name].apply(rdd, source[1].args);
					args = (source2[0].name != 'lineStream') ? source2[0].args :
						[].concat(fs.createReadStream(data.files[1], {encoding: 'utf8'}), {N: 5});
					other = uc[source2[0].name].apply(uc, args);
					if (source2.length > 1) other = other[source2[1].name].apply(other, source2[1].args);
					args = [].concat(other, dualTransform.args);
					rdd = rdd[dualTransform.name].apply(rdd, args);
					args = [].concat(action.args, function (err, res) {dres = res; done();});
					rdd[action.name].apply(rdd, args);
				});

				it('run distributed, pre-persist', function (done) {
					assert(uc.worker.length > 0);
					var args, rdd, other;
					args = (source[0].name != 'lineStream') ? source[0].args :
						[].concat(fs.createReadStream(data.files[0], {encoding: 'utf8'}), {N: 5});
					rdd = uc[source[0].name].apply(uc, args);
					if (source.length > 1) rdd = rdd[source[1].name].apply(rdd, source[1].args);
					args = (source2[0].name != 'lineStream') ? source2[0].args :
						[].concat(fs.createReadStream(data.files[1], {encoding: 'utf8'}), {N: 5});
					other = uc[source2[0].name].apply(uc, args);
					if (source2.length > 1) other = other[source2[1].name].apply(other, source2[1].args);
					rdd = rdd.persist();
					other = other.persist();
					args = [].concat(other, dualTransform.args);
					rdd = rdd[dualTransform.name].apply(rdd, args);
					args = [].concat(action.args, function (err, res) {pres1 = res; done();});
					rdd[action.name].apply(rdd, args);
				});

				it('run distributed, post-persist', function (done) {
					assert(uc.worker.length > 0);
					var args, rdd, other;
					args = (source[0].name != 'lineStream') ? source[0].args :
						[].concat(fs.createReadStream(data.files[0], {encoding: 'utf8'}), {N: 5});
					rdd = uc[source[0].name].apply(uc, args);
					if (source.length > 1) rdd = rdd[source[1].name].apply(rdd, source[1].args);
					args = (source2[0].name != 'lineStream') ? source2[0].args :
						[].concat(fs.createReadStream(data.files[1], {encoding: 'utf8'}), {N: 5});
					other = uc[source2[0].name].apply(uc, args);
					if (source2.length > 1) other = other[source2[1].name].apply(other, source2[1].args);
					args = [].concat(other, dualTransform.args);
					rdd = rdd[dualTransform.name].apply(rdd, args);
					rdd = rdd.persist();
					args = [].concat(action.args, function (err, res) {pres2 = res; done();});
					rdd[action.name].apply(rdd, args);
				});

				it('run distributed, stream output', function (done) {
					assert(uc.worker.length > 0);
					var args, rdd, other, out;
					args = (source[0].name != 'lineStream') ? source[0].args :
						[].concat(fs.createReadStream(data.files[0], {encoding: 'utf8'}), {N: 5});
					rdd = uc[source[0].name].apply(uc, args);
					if (source.length > 1) rdd = rdd[source[1].name].apply(rdd, source[1].args);
					args = (source2[0].name != 'lineStream') ? source2[0].args :
						[].concat(fs.createReadStream(data.files[1], {encoding: 'utf8'}), {N: 5});
					other = uc[source2[0].name].apply(uc, args);
					if (source2.length > 1) other = other[source2[1].name].apply(other, source2[1].args);
					args = [].concat(other, dualTransform.args);
					rdd = rdd[dualTransform.name].apply(rdd, args);
					args = [].concat(action.args, {stream: true});
					out = rdd[action.name].apply(rdd, args);
					sres = [];
					out.on('data', function (d) {sres.push(d);});
					out.on('end', done);
				});

				it('check distributed results', function () {
					data.compareResults(lres, dres, check);
				});

				it('check distributed pre-persist results', function () {
					data.compareResults(lres, pres1, check);
				});

				it('check distributed post-persist results', function () {
					data.compareResults(lres, pres2, check);
				});

				it('check stream results', function () {
					data.compareResults([lres], sres, check);
				});
			});});
		});});
	});
});});
