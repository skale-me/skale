'use strict';

var thunkify = require('thunkify');
var UgridTask = require('./ugrid-task.js');

function UgridArray(grid, worker, child, transformType, transform, args) {
	if (transformType == undefined)
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

	this.inputSource = function() {
		switch (this.transform) {
		case 'randomSVMData':
			return 'var input = ml.randomSVMData(node[' + this.num +
				'].args[0], ' + 'node[' + this.num + '].args[1], node[' + this.num + '].args[2], node[' + this.num + '].args[3]);\n';
		case 'loadTestData':
			return 'var input = ml.loadTestData(node[' + this.num +
				'].args[0], ' + 'node[' + this.num + '].args[1], node[' +
				this.num + '].args[2]);\n';
		case 'parallelize':
			return 'var input = node[' + this.num + '].args[0];\n';
		default:
			throw 'UgridArray().input error: unknown transformation'
		}
	}

	this.pipelineSource = function () {
		switch (this.transform) {
		case 'map':
			var argStr = '';
			for (var arg = 0; arg < this.args[1].length; arg++)
				argStr += ', node[' + this.num + '].args[1][' + arg + ']';
			return '\t\ttmp = transform[' + this.num + '](tmp' + argStr + ');\n';
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
			throw 'UgridArray().pipeline error: unknown transformation'
		}
	}

	this.transformSource = function () {
		switch (this.transform) {
		case 'map':
			return 'transform[' + this.num + '] = ' + this.args[0].toString() + '\n';
		case 'union':
			return '';
		case 'reduceByKey':
			return 'transform[' + this.num + '] = ' + this.args[1].toString() + '\n';		
		default:
			throw 'UgridArray().transformSource error: unknown transformation'		
		}		
	}

	function rpc(grid, cmd, workerNum, args, callback) {
		grid.request_cb(worker[workerNum], {cmd: cmd, args: args}, function (err, res) {
			callback(err, res, workerNum);
		});
	}

	function sendTask(task, callback) {
		var n = 0;
		for (var i = 0; i < worker.length; i++)
			rpc(grid, 'setTask', i, task[i], function(err, res) {
				if (++n == worker.length)
					for (var j = 0; j < worker.length; j++)
						rpc(grid, 'runTask', j, null, function(err, res, from) {callback(null, res, from);});
			});
	}

	this.persist = function() {
		this.persistent = true;
		return this;
	}

	this.map = function(mapper, mapperArgs) {
		return new UgridArray(grid, worker, [this], 'narrow', 'map', [mapper.toString(), mapperArgs]);
	}

	this.union = function(withArray) {
		if (withArray.id == this.id) return this;
		return new UgridArray(grid, worker, [this, withArray], 'narrow', 'union', [withArray.id]);
	}

	this.flatMap = function(mapper, mapperArgs) {
		throw 'this.flatMap not yet implemented'
		return new UgridArray(grid, worker, [this], 'narrow', 'flatMap', [mapper.toString(), mapperArgs]);
	}

	this.filter = function(filter) {
		throw 'this.filter not yet implemented'
		return new UgridArray(grid, worker, [this], 'narrow', 'filter', [filter.toString()]);
	}

	this.reduceByKey = function(key, reducer, initVal) {
		return new UgridArray(grid, worker, [this], 'wide', 'reduceByKey', [key, reducer.toString(), initVal]);
	}

	this.reduce_cb = function(reducer, aInit, callback) {
		var task = UgridTask.buildTask(worker, this, {
			fun: 'reduce',
			args: [reducer.toString(), aInit],
			init: 'var reducer = ' + reducer.toString() + ';\nvar res = action.args[1];\n',
			run: 'res = reducer(res, tmp);\n'
		});

		var nAnswer = 0, result = aInit;
		sendTask(task, function(err, res, from) {
			result = reducer(result, res);
			if (++nAnswer == worker.length)
				callback(null, result);
		});
	}

	this.count_cb = function(callback) {
		var task = UgridTask.buildTask(worker, this, {
			fun: 'count',
			args: [],
			init: 'var res = 0;\n',
			run: 'res++;\n'
		});

		var nAnswer = 0, result = 0;
		sendTask(task, function(err, res, from) {
			result += res;
			if (++nAnswer == worker.length)
				callback(null, result);
		});
	}

	this.collect_cb = function(callback) {
		var task = UgridTask.buildTask(worker, this, {
			fun: 'collect',
			args: [],
			init: 'var res = {};\n',
			run: 'if (res[p] == undefined) res[p] = []; res[p].push({idx: i, value: tmp});\n'
		});

		var nAnswer = 0, result = [];
		sendTask(task, function(err, res, from) {
			var partName = Object.keys(res);
			var nPart = partName.length;
			for (var p = 0; p < nPart; p++) {
				res[partName[p]].map(function(n) {
					n.idx = (n.idx * nPart + p) * worker.length + from;
				});
				result = result.concat(res[partName[p]]);
			}
			if (++nAnswer == worker.length) {
				var finalRes = new Array(result.length);
				for (var k = 0; k < result.length; k++)
					finalRes[result[k].idx] = result[k].value;
				callback(null, finalRes);
			}
		});
	}

	this.sample_cb = function(fraction, callback) {
		function sample(v, frac) {
			var fres = [];
			while (fres.length != Math.ceil(frac * v.length)) {
				var rnd = Math.floor(Math.random() * v.length);
				if (fres.indexOf(v[rnd]) != -1) continue;
				fres.push(v[rnd]);
			}
			return fres;
		}

		var task = UgridTask.buildTask(worker, this, {
			fun: 'sample',
			args: [fraction],
			init: 'var res = [];\n',
			run: 'res.push(tmp);\n',
			post: sample.toString() + '; res = {N: res.length, data: sample(res, action.args[0])};'
		});

		var nAnswer = 0, result = [], N = 0;
		sendTask(task, function(err, res, from) {
			N += res.N;
			result = result.concat(res.data);
			if (++nAnswer == worker.length) {
				var n = Math.ceil(fraction * N);
				result.splice(n, result.length - n);
				callback(null, result);
			}
		});
	}

	this.takeSample_cb = function(n, callback) {
		var array = this;
		array.count_cb(function(err, res) {
			if (res < n)
				throw 'takeSample error: Can\'t take ' + n + ' samples, array length is ' + res;
			array.sample_cb(n / res, function(err, res) {
				if (res.length > n) res.splice(n, res.length - n);
				callback(null, res);
			})
		});
	}

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
