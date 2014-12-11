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
				'\tres[tmp[' + key + ']] = JSON.parse(JSON.stringify(node[' + this.num + '].args[2]));\n';
			tmp += 'res[tmp[' + key + ']] = transform[' + this.num + '](res[tmp[' + key + ']], tmp);\n';
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

	function rpc(grid, uuid, args, callback) {
		grid.send_cb('request', {uuid: uuid, payload: {cmd: "task", args: args}}, callback);
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

	// Return the result of reduction function
	this.reduce_cb = function(reducer, aInit, callback) {
		var task = UgridTask.buildTask(worker, this, {
			fun: 'reduce',
			args: [reducer.toString(), aInit],
			init: 'var reducer = ' + reducer.toString() + ';\nvar res = action.args[1];\n',
			run: 'res = reducer(res, tmp);\n'
		});

		var nAnswer = 0, result = aInit;
		for (var i in worker)
			rpc(grid, worker[i], task[i], function(err, res) {
				result = reducer(result, res);
				if (++nAnswer == worker.length)
					callback(null, result);
			})
	}

	this.count_cb = function(callback) {
		var task = UgridTask.buildTask(worker, this, {
			fun: 'count',
			args: [],
			init: 'var res = 0;\n',
			run: 'res++;\n'
		});

		var nAnswer = 0, result = 0;
		for (var i in worker)
			rpc(grid, worker[i], task[i], function(err, res) {
				result += res;
				if (++nAnswer == worker.length)
					callback(null, result);
			})
	}

	// Return dataset entries
	this.collect_cb = function(callback) {
		var task = UgridTask.buildTask(worker, this, {
			fun: 'collect',
			args: [],
			init: 'var res = [];\n',
			// Optimise research of partition p index in partition key array
			run: 'res.push({nPart: Object.keys(input).length, pidx: Object.keys(input).indexOf(p), idx: i, value: tmp});\n'
		});

		var nAnswer = 0, result = [];
		for (var i in worker)
			rpc(grid, worker[i], task[i], function(err, res, from) {
				res.map(function(n) {
					n.idx = n.pidx * (n.nPart - 1) + (n.idx * worker.length + worker.indexOf(from));	// NOT SURE ABOUT INDEX CONSTRUCTION HERE !!!!!
				});
				result = result.concat(res);
				if (++nAnswer == worker.length) {
					var finalRes = new Array(result.length);
					for (var j = 0; j < result.length; j++)
						finalRes[result[j].idx] = result[j].value;
					callback(null, finalRes);
				}
			})
	}

	this.takeSample_cb = function(N, callback) {
		var task = UgridTask.buildTask(worker, this, {
			fun: 'takeSample', 
			args: [N],
			init: 'var res = [];\n',
			run: 'res.push(tmp);\n if (res.length == action.args[0]) break;\n'
		});
		
		var nAnswer = 0, result = [];
		for (var i in worker)
			rpc(grid, worker[i], task[i], function(err, res) {
				result = result.concat(res);
				if (++nAnswer == worker.length) {
					if (result.length > N)
						result = result.splice(N, result.length);
					callback(null, result);
				}
			})
	}

	this.sample_cb = function(fraction, callback) {
		throw 'this.sample not yet implemented'
		var task = UgridTask.buildTask(worker, this, {
			fun: 'sample',
			args: [fraction]
		});

		var nAnswer = 0, result = [];
		for (var i in worker)
			rpc(grid, worker[i], task[i], function(err, res) {
				result = result.concat(res);
				if (++nAnswer == worker.length)
					callback(null, result);
			})
	}

	this.lookup_cb = function(key, callback) {
		throw 'this.lookup not yet implemented'
		var task = UgridTask.buildTask(worker, this, {
			fun: 'lookup',
			args: [key]
		});

		var nAnswer = 0, result = [];
		for (var i in worker)
			rpc(grid, worker[i], task[i], function(err, res) {
				result = result.concat(res);
				if (++nAnswer == worker.length)
					callback(null, result);
			})
	}

	this.reduce = thunkify(this.reduce_cb);
	this.count = thunkify(this.count_cb);
	this.collect = thunkify(this.collect_cb);
	this.takeSample = thunkify(this.takeSample_cb);
	this.sample = thunkify(this.sample_cb);
	this.lookup = thunkify(this.lookup_cb);
}

module.exports = UgridArray;