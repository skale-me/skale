// Copyright 2016 Luca-SAS, licensed under the Apache License 2.0

var fs = require('fs');
var net = require('net');
var spawn = require('child_process').spawn;
var trace = require('line-trace');
var skale = require('../');
var data = require('./support/data.js');
var local = require('./support/local.js');
// use a non default port dedicated to tests
var skalePort = 2121;

var server, workerController, sc, sl;

function tryConnect(nTry, timeout, done) {
	const sock = net.connect(skalePort);
	sock.on('connect', function() {
		sock.end();
		done();
	});
	sock.on('error', function (err) {
		if (--nTry <= 0) return done('skale-server not ok');
		setTimeout(function () {tryConnect(nTry, timeout, done);}, timeout);
	});
}

beforeEach(function (done) {
	var output;
	if (sc === undefined) {
		this.timeout(10000);
		server = spawn('./bin/server.js', ['-p', skalePort, '-l', '0']);
		tryConnect(100, 100, function (err) {
			console.log(err);
			sl = local.context();
			sc = skale.context({port: skalePort});
			done();
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
//	{name: 'sample', args: [true, 0.1], sort: true, lengthOnly: true},
//	{name: 'values', args: []},
];

var dualTransforms = [
	{name: 'coGroup', args: [], sort: true},
	{name: 'cartesian', args: [], sort: true},
	{name: 'intersection', args: [], sort: true},
	{name: 'join', args: [], sort: true},
	{name: 'leftOuterJoin', args: [], sort: true},
	{name: 'rightOuterJoin', args: [], sort: true},
	{name: 'subtract', args: [], sort: true},
	{name: 'union', args: [], sort: true},
];

var actions = [
	{name: 'collect', args: [], stream: true},
	{name: 'count', args: [], stream: true},
	{name: 'countByValue', args: [], sort: true, stream: true},
//	{name: 'lookup', args: [data.v[0][0][0]], stream: true},
	{name: 'reduce', args: [data.reducer, [0, 0]], stream: true},
//	{name: 'take', args: [2], lengthOnly: true, stream: true},
//	{name: 'takeOrdered', args: [2, function (a, b) {return a < b;}], stream: true},
//	{name: 'top', args: [2], stream: true},
//	{name: 'takeSample', args: [true, 2, 1], stream: true},
// XXXXX TODO:
// foreach,
];

sources.forEach(function (source) {describe('sc.' + source[0].name + '()', function() {
	transforms.forEach(function (transform) {describe(transform.name ? '.' + transform.name + '()' : '/* empty */', function () {
		actions.forEach(function (action) {describe('.' + action.name + '()', function () {
			var lres, dres, sres, pres1, pres2, check = {};

			if (transform.sort || action.sort) check.sort = true;
			if (transform.lengthOnly || action.lengthOnly) check.lengthOnly = true;
			if (transform.name == 'groupByKey' && action.name == 'reduce') check.lengthOnly = true;

			it('run local', function (done) {
				var src_args, action_args, da;
				switch (source[0].name) {
				case 'lineStream':
					src_args = [fs.createReadStream(data.files[0], {encoding: 'utf8'}), {N: 5}];
					break;
				case 'parallelize':
					src_args = [JSON.parse(JSON.stringify(data.v[0]))];
					break;
				default:
					src_args = source[0].args; break;
				}
				da = sl[source[0].name].apply(sl, src_args);
				if (source.length > 1 ) da = da[source[1].name].apply(da, source[1].args);
				if (transform.name) da = da[transform.name].apply(da, transform.args);
				if (action.stream) {
					action_args = [].concat(action.args);
					da = da[action.name].apply(da, action_args);
					da.toArray(function(err, res) {lres = res; done();});
				} else {
					action_args = [].concat(action.args, function (err, res) {lres = res; done();});
					da[action.name].apply(da, action_args);
				}
			});

			it('run distributed', function (done) {
				var src_args, action_args, da;
				switch (source[0].name) {
				case 'lineStream':
					src_args = [fs.createReadStream(data.files[0], {encoding: 'utf8'}), {N: 5}];
					//src_args = [fs.createReadStream(data.files[0], {encoding: 'utf8'})];
					break;
				case 'parallelize':
					src_args = [JSON.parse(JSON.stringify(data.v[0]))];
					break;
				default:
					src_args = source[0].args; break;
				}
				da = sc[source[0].name].apply(sc, src_args);
				if (source.length > 1 ) da = da[source[1].name].apply(da, source[1].args);
				if (transform.name) da = da[transform.name].apply(da, transform.args);
				if (action.stream) {
					action_args = [].concat(action.args);
					da = da[action.name].apply(da, action_args);
					da.toArray(function(err, res) {dres = res; done();});
				} else {
					action_args = [].concat(action.args, function (err, res) {dres = res; done();});
					da[action.name].apply(da, action_args);
				}
			});

			it('check distributed results', function () {
				data.compareResults(lres, dres, check);
			});

			it('run distributed, pre-persist', function (done) {
				var src_args, action_args, da;
				switch (source[0].name) {
				case 'lineStream':
					src_args = [fs.createReadStream(data.files[0], {encoding: 'utf8'}), {N: 5}];
					break;
				case 'parallelize':
					src_args = [JSON.parse(JSON.stringify(data.v[0]))];
					break;
				default:
					src_args = source[0].args; break;
				}
				da = sc[source[0].name].apply(sc, src_args);
				if (source.length > 1) da = da[source[1].name].apply(da, source[1].args);
				da = da.persist();
				if (transform.name) da = da[transform.name].apply(da, transform.args);
				action_args = [].concat(action.args, function (err, res) {
					switch (source[0].name) {
					case 'parallelize': src_args[0].push([3, 4]); break;
					}
					if (action.stream) {
						da[action.name].apply(da, action_args).toArray(done);
					} else {
						action_args = [].concat(action.args, function (err, res) {pres1 = res; done();});
						da[action.name].apply(da, action_args);
					}
				});
				var da2 = da[action.name].apply(da, action_args);
				if (action.stream) da2.toArray(function (err, res) {pres1 = res; done();});
			});

			it('run distributed, post-persist', function (done) {
				var src_args, action_args, da;
				switch (source[0].name) {
				case 'lineStream':
					src_args = [fs.createReadStream(data.files[0], {encoding: 'utf8'}), {N: 5}];
					break;
				case 'parallelize':
					src_args = [JSON.parse(JSON.stringify(data.v[0]))];
					break;
				default:
					src_args = source[0].args; break;
				}
				da = sc[source[0].name].apply(sc, src_args);
				if (source.length > 1 ) da = da[source[1].name].apply(da, source[1].args);
				if (transform.name) da = da[transform.name].apply(da, transform.args);
				da = da.persist();
				action_args = [].concat(action.args, function (err, res) {
					switch (source[0].name) {
					case 'parallelize': src_args[0].push([3, 4]); break;
					}
					if (action.stream) {
						da[action.name].apply(da, action_args).toArray(done);
					} else {
						action_args = [].concat(action.args, function (err, res) {pres2 = res; done();});
						da[action.name].apply(da, action_args);
					}
				});
				var da2 = da[action.name].apply(da, action_args);
				if (action.stream) da2.toArray(function (err, res) {pres2 = res; done();});
			});

			it('check distributed pre-persist results', function () {
				data.compareResults(lres, pres1, check);
			});

			it('check distributed post-persist results', function () {
				data.compareResults(lres, pres2, check);
			});

		});});
	});});

	sources2.forEach(function (source2) {
		dualTransforms.forEach(function (dualTransform) {describe('.' + dualTransform.name + '(sc.' + source2[0].name + '())', function () {
			actions.forEach(function (action) {describe('.' + action.name + '()', function () {
				var lres, dres, sres, pres1, pres2, check = {};

				if (dualTransform.sort || action.sort) check.sort = true;
				if (dualTransform.lengthOnly || action.lengthOnly) check.lengthOnly = true;
				if (action.name == 'reduce') {
					switch (dualTransform.name) {
					case 'coGroup':
					case 'cartesian':
					case 'join':
					case 'leftOuterJoin':
					case 'rightOuterJoin':
						check.lengthOnly = true;
					}
				}

				it('run local', function (done) {
					var transform_args, src_args, src2_args, action_args, da, other;
					switch (source[0].name) {
					case 'lineStream':
						src_args = [fs.createReadStream(data.files[0], {encoding: 'utf8'}), {N: 5}];
						break;
					case 'parallelize':
						src_args = [JSON.parse(JSON.stringify(data.v[0]))];
						break;
					default:
						src_args = source[0].args; break;
					}
					da = sl[source[0].name].apply(sl, src_args);
					if (source.length > 1) da = da[source[1].name].apply(da, source[1].args);
					switch (source2[0].name) {
					case 'lineStream':
						src2_args = [fs.createReadStream(data.files[1], {encoding: 'utf8'}), {N: 5}];
						break;
					case 'parallelize':
						src2_args = [JSON.parse(JSON.stringify(data.v[1]))];
						break;
					default:
						src2_args = source2[0].args; break;
					}
					other = sl[source2[0].name].apply(sl, src2_args);
					if (source2.length > 1) {
						other = other[source2[1].name].apply(other, source2[1].args);
					}
					transform_args = [].concat(other, dualTransform.args);
					da = da[dualTransform.name].apply(da, transform_args);
					if (action.stream) {
						action_args = [].concat(action.args);
						da = da[action.name].apply(da, action_args);
						da.toArray(function(err, res) {lres = res; done();});
					} else {
						action_args = [].concat(action.args, function (err, res) {lres = res; done();});
						da[action.name].apply(da, action_args);
					}
				});

				it('run distributed', function (done) {
					var src_args, src2_args, transform_args, action_args, da, other;
					switch (source[0].name) {
					case 'lineStream':
						src_args = [fs.createReadStream(data.files[0], {encoding: 'utf8'}), {N: 5}];
						break;
					case 'parallelize':
						src_args = [JSON.parse(JSON.stringify(data.v[0]))];
						break;
					default:
						src_args = source[0].args; break;
					}
					da = sc[source[0].name].apply(sc, src_args);
					if (source.length > 1) da = da[source[1].name].apply(da, source[1].args);
					switch (source2[0].name) {
					case 'lineStream':
						src2_args = [fs.createReadStream(data.files[1], {encoding: 'utf8'}), {N: 5}];
						break;
					case 'parallelize':
						src2_args = [JSON.parse(JSON.stringify(data.v[1]))];
						break;
					default:
						src2_args = source2[0].args; break;
					}
					other = sc[source2[0].name].apply(sc, src2_args);
					if (source2.length > 1) other = other[source2[1].name].apply(other, source2[1].args);
					transform_args = [].concat(other, dualTransform.args);
					da = da[dualTransform.name].apply(da, transform_args);
					if (action.stream) {
						action_args = [].concat(action.args);
						da = da[action.name].apply(da, action_args);
						da.toArray(function(err, res) {dres = res; done();});
					} else {
						action_args = [].concat(action.args, function (err, res) {dres = res; done();});
						da[action.name].apply(da, action_args);
					}
				});

				it('check distributed results', function () {
					data.compareResults(lres, dres, check);
				});

				it('run distributed, pre-persist', function (done) {
					var src_args, src2_args, transform_args, action_args, action2_args, da, other;
					switch (source[0].name) {
					case 'lineStream':
						src_args = [fs.createReadStream(data.files[0], {encoding: 'utf8'}), {N: 5}];
						break;
					case 'parallelize':
						src_args = [JSON.parse(JSON.stringify(data.v[0]))];
						break;
					default:
						src_args = source[0].args; break;
					}
					da = sc[source[0].name].apply(sc, src_args);
					if (source.length > 1) da = da[source[1].name].apply(da, source[1].args);
					switch (source2[0].name) {
					case 'lineStream':
						src2_args = [fs.createReadStream(data.files[1], {encoding: 'utf8'}), {N: 5}];
						break;
					case 'parallelize':
						src2_args = [JSON.parse(JSON.stringify(data.v[1]))];
						break;
					default:
						src2_args = source2[0].args; break;
					}
					other = sc[source2[0].name].apply(sc, src2_args);
					if (source2.length > 1) other = other[source2[1].name].apply(other, source2[1].args);
					da = da.persist();
					other = other.persist();
					transform_args = [].concat(other, dualTransform.args);
					da = da[dualTransform.name].apply(da, transform_args);
					action_args = [].concat(action.args, function (err, res) {
						switch (source[0].name) {
						case 'parallelize': src_args[0].push([3, 4]); break;
						}
						switch (source2[0].name) {
						case 'parallelize': src2_args[0].push([3, 4]); break;
						}
						if (action.stream) {
							da[action.name].apply(da, action_args).toArray(done);
						} else {
							action2_args = [].concat(action.args, function (err, res) {pres1 = res; done();});
							da[action.name].apply(da, action2_args);
						}
					});
					var da2 = da[action.name].apply(da, action_args);
					if (action.stream) da2.toArray(function (err, res) {pres1 = res; done();});
				});

				// it('run distributed, post-persist', function (done) {
				// 	var src_args, src2_args, transform_args, action_args, action2_args, da, other;
				// 	switch (source[0].name) {
				// 	case 'lineStream':
				// 		src_args = [fs.createReadStream(data.files[0], {encoding: 'utf8'}), {N: 5}];
				// 		break;
				// 	case 'parallelize':
				// 		src_args = [JSON.parse(JSON.stringify(data.v[0]))];
				// 		break;
				// 	default:
				// 		src_args = source[0].args; break;
				// 	}
				// 	da = sc[source[0].name].apply(sc, src_args);
				// 	if (source.length > 1) da = da[source[1].name].apply(da, source[1].args);
				// 	switch (source2[0].name) {
				// 	case 'lineStream':
				// 		src2_args = [fs.createReadStream(data.files[1], {encoding: 'utf8'}), {N: 5}];
				// 		break;
				// 	case 'parallelize':
				// 		src2_args = [JSON.parse(JSON.stringify(data.v[1]))];
				// 		break;
				// 	default:
				// 		src2_args = source2[0].args; break;
				// 	}
				// 	other = sc[source2[0].name].apply(sc, src2_args);
				// 	if (source2.length > 1) other = other[source2[1].name].apply(other, source2[1].args);
				// 	transform_args = [].concat(other, dualTransform.args);
				// 	da = da[dualTransform.name].apply(da, transform_args);
				// 	da = da.persist();
				// 	action_args = [].concat(action.args, function (err, res) {
				// 		switch (source2[0].name) {
				// 		case 'parallelize': src2_args[0].push([3, 4]); break;
				// 		}
				// 		if (action.stream) {
				// 			da[action.name].apply(da, action_args).toArray(done);
				// 		} else {
				// 			action2_args = [].concat(action.args, function (err, res) {pres2 = res; done();});
				// 			da[action.name].apply(da, action2_args);
				// 		}
				// 	});
				// 	var da2 = da[action.name].apply(da, action_args);
				// 	if (action.stream) da2.toArray(function (err, res) {pres2 = res; done();});
				// });

				it('check distributed pre-persist results', function () {
					data.compareResults(lres, pres1, check);
				});

				// it('check distributed post-persist results', function () {
				// 	data.compareResults(lres, pres2, check);
				// });
			});});
		});});
	});
});});
