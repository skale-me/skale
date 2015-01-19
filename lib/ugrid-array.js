'use strict';

var thunkify = require('thunkify');
var UgridTask = require('./ugrid-task.js');
var ml = require('./ugrid-ml.js');

function UgridArray(grid, worker, child, transformType, transform, args) {
	if (transformType === undefined)
		throw 'UgridArray() error: Transform type ' + transformType + ' unknown';

	this.id = Math.round(Math.random() * 1e9);			// Unique array id
	this.inMemory = false;
	this.child = [];
	this.persistent = false;
	this.visits = 0;
	this.transform = transform;
	this.transformType = transformType;
	this.args = args;

	for (var i = 0; i < child.length; i++) {
		this.child.push(child[i]);
		child[i].anc = this;
	}

	this.pipelineSource = function () {
		switch (this.transform) {
		case 'map':
			var argStr = '';
			for (var arg = 0; arg < this.args[1].length; arg++)
				argStr += ', node[' + this.num + '].args[1][' + arg + ']';
			return '\t\ttmp = transform[' + this.num + '](tmp' + argStr + ');\n';
		case 'filter':
			return '\t\tvar test = transform[' + this.num + '](tmp);\n' + 'if (!test) continue; ';
		case 'union':
			return '';
		case 'reduceByKey':
			var key = 'node[' + this.num + '].args[0]';
			var tmp = 'if (tmp[' + key + '] == undefined)\n\tthrow "key unknown by worker"\n';
			tmp += 'if (res[tmp[' + key + ']] == undefined)\n' +
				'\tres[tmp[' + key + ']] = [JSON.parse(JSON.stringify(node[' + this.num + '].args[2]))];\n';
			tmp += 'res[tmp[' + key + ']] = [transform[' + this.num + '](res[tmp[' + key + ']][0], tmp)];\n';
			return tmp;
		default:
			throw 'UgridArray().pipeline error: unknown transformation';
		}
	};

	this.transformSource = function () {
		switch (this.transform) {
		case 'map':
			return 'transform[' + this.num + '] = ' + this.args[0].toString() + '\n';
		case 'filter':
			return 'transform[' + this.num + '] = ' + this.args[0].toString() + '\n';
		case 'union':
			return '';
		case 'reduceByKey':
			return 'transform[' + this.num + '] = ' + this.args[1].toString() + '\n';
		default:
			throw 'UgridArray().transformSource error: unknown transformation';
		}
	};

	function rpc(grid, cmd, workerNum, args, callback) {
		grid.request_cb(worker[workerNum], {cmd: cmd, args: args}, callback);
	}

	function sendTask(task, callback) {
		var n = 0;
		for (var i = 0; i < worker.length; i++)
			rpc(grid, 'setTask', i, task[i], function () {
				if (++n == worker.length)
					for (var j = 0; j < worker.length; j++)
						rpc(grid, 'runTask', j, null, callback);
			});
	}

	this.persist = function () {
		this.persistent = true;
		return this;
	};

	this.map = function (mapper, mapperArgs) {
		return new UgridArray(grid, worker, [this], 'narrow', 'map', [mapper.toString(), mapperArgs || []]);
	};

	this.union = function (withArray) {
		if (withArray.id == this.id) return this;
		return new UgridArray(grid, worker, [this, withArray], 'narrow', 'union', [withArray.id]);
	};

	this.flatMap = function (mapper, mapperArgs) {
		throw 'this.flatMap not yet implemented';
		return new UgridArray(grid, worker, [this], 'narrow', 'flatMap', [mapper.toString(), mapperArgs]);
	};

	this.filter = function (filter) {
		return new UgridArray(grid, worker, [this], 'narrow', 'filter', [filter.toString()]);
	};

	this.reduceByKey = function (key, reducer, initVal) {
		return new UgridArray(grid, worker, [this], 'wide', 'reduceByKey', [key, reducer.toString(), initVal]);
	};

	this.reduce_cb = function (reducer, aInit, callback) {
		UgridTask.buildTask(grid, worker, this, {
			fun: 'reduce',
			args: [reducer.toString(), aInit],
			init: 'var reducer = ' + reducer.toString() + ';\nvar res = action.args[1];\n',
			run: 'res = reducer(res, tmp);\n'
		}, function (task) {
			var nAnswer = 0, result = aInit;
			sendTask(task, function (err, res) {
				result = reducer(result, res);
				if (++nAnswer == worker.length)
					callback(null, result);
			});
		});
	};

	this.count_cb = function (callback) {
		UgridTask.buildTask(grid, worker, this, {
			fun: 'count',
			args: [],
			init: 'var res = 0;\n',
			run: 'res++;\n'
		}, function (task) {
			var nAnswer = 0, result = 0;
			sendTask(task, function(err, res) {
				result += res;
				if (++nAnswer == worker.length)
					callback(null, result);
			});
		});
	};

	this.collect_cb = function (callback) {
		UgridTask.buildTask(grid, worker, this, {
			fun: 'collect',
			args: [],
			init: 'var res = {};\n',
			run: 'if (res[p] == undefined) res[p] = []; res[p].push({idx: i, value: tmp});\n'
		}, function (task) {
			var nAnswer = 0, tmp = {};
			sendTask(task, function (err, res) {
				for (var p in res) tmp[p] = res[p];
				if (++nAnswer == worker.length) {
					var result = [];
					var partitionNames = Object.keys(tmp);
					var nPart = partitionNames.length;
					for (p = 0; p < nPart; p++)
						for (var i = 0; i < tmp[partitionNames[p]].length; i++) {
							tmp[partitionNames[p]][i].idx = tmp[partitionNames[p]][i].idx * nPart + p;
							result[tmp[partitionNames[p]][i].idx] = tmp[partitionNames[p]][i].value;
						}

					// remove undefined entries and call result calback
					callback(null, result.filter(function (n) {if (n !== undefined) return true;}));
				}
			});			
		});
	};

	this.sample_cb = function (fraction, seed, callback) {
		function sample(v, frac, seed) {
			var rng = new ml.Random(seed);
			var fres = [];
			while (fres.length != Math.ceil(frac * v.length)) {
				var rnd = Math.floor(Math.abs(rng.next() * v.length));
				if (fres.indexOf(v[rnd]) != -1) continue;
				fres.push(v[rnd]);
			}
			return fres;
		}

		UgridTask.buildTask(grid, worker, this, {
			fun: 'sample',
			args: [fraction, seed],
			init: 'var res = [];\n',
			run: 'res.push(tmp);\n',
			post: sample.toString() + '; res = {N: res.length, data: sample(res, action.args[0], action.args[1])};'
		}, function (task) {
		var nAnswer = 0, result = [], N = 0;
			sendTask(task, function (err, res) {
				N += res.N;
				result = result.concat(res.data);
				if (++nAnswer == worker.length) {
					var n = Math.ceil(fraction * N);
					result.splice(n, result.length - n);
					callback(null, result);
				}
			});
		});
	};

	this.takeSample_cb = function(n, seed, callback) {
		var array = this;
		array.count_cb(function (err, res) {
			if (res < n)
				throw 'takeSample error: Can\'t take ' + n + ' samples, array length is ' + res;
			array.sample_cb(n / res, seed, function (err, res) {
				if (res.length > n) res.splice(n, res.length - n);
				callback(null, res);
			});
		});
	};

	// this.lookup_cb = function(key, callback) {
	// 	throw 'this.lookup not yet implemented'
	// 	var task = UgridTask.buildTask(worker, this, {
	// 		fun: 'lookup',
	// 		args: [key]
	// 	});

	// 	var nAnswer = 0, result = [];
	// 	for (var i = 0; i < worker.length; i++)
	// 		rpc(grid, i, task[i], function(err, res) {
	// 			result = result.concat(res);
	// 			if (++nAnswer == worker.length)
	// 				callback(null, result);
	// 		})
	// }

	this.reduce = thunkify(this.reduce_cb);
	this.count = thunkify(this.count_cb);
	this.collect = thunkify(this.collect_cb);
	this.takeSample = thunkify(this.takeSample_cb);
	this.sample = thunkify(this.sample_cb);
	// this.lookup = thunkify(this.lookup_cb);
}

module.exports = UgridArray;
