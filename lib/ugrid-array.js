'use strict';

var thunkify = require('thunkify');
var UgridTask = require('./ugrid-task.js');

function UgridArray(grid, worker, child, dependency, transform, args, src) {
	this.id = Math.round(Math.random() * 1e9);			// Unique array id
	this.inMemory = false;
	this.child = child;
	this.persistent = false;
	this.visits = 0;
	this.transform = transform;
	this.dependency = dependency;
	this.args = args || [];

	for (var i = 0; i < child.length; i++)
		child[i].anc = this;	// set child ancestor

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

	this.getArgs = function() {
		var o = this.args;
		var transform = this.transform;
		var child_id = [];
		for (var i = 0; i < child.length; i++)
			child_id.push(child[i].id);
		return {
			args: o,
			src: src,
			child: child_id,
			transform: transform
		};
	}

	this.persist = function () {
		this.persistent = true;
		return this;
	};

	this.map = function (mapper, mapperArgs) {
		return new UgridArray(grid, worker, [this], 'narrow', 'map', mapperArgs, mapper.toString());
	};

	this.keys = function () {
		var mapper = function (e) {return e[0];}
		return new UgridArray(grid, worker, [this], 'narrow', 'map', null, mapper.toString()); 
	};

	this.values = function () {
		var mapper = function (e) {return e[1];}
		return new UgridArray(grid, worker, [this], 'narrow', 'map', null, mapper.toString()); 
	};

	this.filter = function (filter, filterArgs) {
		return new UgridArray(grid, worker, [this], 'narrow', 'filter', filterArgs, filter.toString());
	};

	this.flatMap = function (mapper, mapperArgs) {
		return new UgridArray(grid, worker, [this], 'narrow', 'flatMap', mapperArgs, mapper.toString());
	};

	this.flatMapValues = function (mapper, mapperArgs) {
		return new UgridArray(grid, worker, [this], 'narrow', 'flatMapValues', mapperArgs, mapper.toString());
	};

	this.sample = function (withReplacement, frac, seed) {
		return new UgridArray(grid, worker, [this], 'wide', 'sample', [withReplacement, frac, seed || 1]);
	};

	this.groupByKey = function () {
		return new UgridArray(grid, worker, [this], 'wide', 'groupByKey', []);
	};

	this.reduceByKey = function (reducer, initVal) {
		return new UgridArray(grid, worker, [this], 'wide', 'reduceByKey', [initVal], reducer.toString());
	};

	this.union = function (withArray) {
		if (withArray.id == this.id) return this;
		return new UgridArray(grid, worker, [this, withArray], 'narrow', 'union', [withArray.id]);
	};

	this.join = function (withArray) {
		if (withArray.id == this.id) return this;
		return new UgridArray(grid, worker, [this, withArray], 'wide', 'join', [withArray.id, 'inner']);
	};

	this.leftOuterJoin = function (withArray) {
		if (withArray.id == this.id) return this;
		return new UgridArray(grid, worker, [this, withArray], 'wide', 'join', [withArray.id, 'left']);
	};

	this.rightOuterJoin = function (withArray) {
		if (withArray.id == this.id) return this;
		return new UgridArray(grid, worker, [this, withArray], 'wide', 'join', [withArray.id, 'right']);
	};

	this.coGroup = function (withArray) {
		if (withArray.id == this.id) return this;
		return new UgridArray(grid, worker, [this, withArray], 'wide', 'coGroup', [withArray.id]);
	};

	this.crossProduct = function (withArray) {
		if (withArray.id == this.id) return this;
		return new UgridArray(grid, worker, [this, withArray], 'wide', 'crossProduct', [withArray.id]);
	};

	this.mapValues = function (mapper, mapperArgs) {
		return new UgridArray(grid, worker, [this], 'narrow', 'mapValues', mapperArgs, mapper.toString());
	};

	this.distinct = function () {
		return new UgridArray(grid, worker, [this], 'wide', 'distinct', []);
	};

	this.sortByKey = function () {
		return new UgridArray(grid, worker, [this], 'wide', 'sortByKey', []);
	};

	this.partitionByKey = function () {
		return new UgridArray(grid, worker, [this], 'wide', 'partitionByKey', []);
	};

	this.intersection = function (withArray) {
		return new UgridArray(grid, worker, [this, withArray], 'wide', 'intersection', []);
	};

	this.subtract = function (withArray) {
		return new UgridArray(grid, worker, [this, withArray], 'wide', 'subtract', []);
	};

	this.reduce_cb = function (reducer, aInit, callback) {
		UgridTask.buildTask(grid, worker, this, {
			fun: 'reduce',
			args: [aInit],
			src: reducer.toString(),
			init: aInit
		}, function (task) {
			var nAnswer = 0;
			sendTask(task, function (err, res) {
				aInit = reducer(aInit, res);
				if (++nAnswer == worker.length) callback(null, aInit);
			});
		});
	};

	this.take_cb = function (num, callback) {
		UgridTask.buildTask(grid, worker, this, {
			fun: 'take',
			args: [num],
			init: 0,
		}, function (task) {
			var nAnswer = 0, result = [];
			sendTask(task, function (err, res) {
				for (var i = 0; i < res.length; i++)
					if (result.length < num) result.push(res[i]);
					else break;
				if (++nAnswer == worker.length) callback(null, result);
			});
		});
	};

	this.top_cb = function (num, callback) {
		// By default sort order is on number
		var numsort = function (a, b) {
		    return b - a;
		}

		UgridTask.buildTask(grid, worker, this, {
			fun: 'top',
			args: [num, numsort],
			init: 0,
		}, function (task) {
			var nAnswer = 0, result = [];
			sendTask(task, function (err, res) {
				result = result.concat(res);
				result = result.sort(numsort);
				result = result.slice(0, num);
				if (++nAnswer == worker.length) callback(null, result);
			});
		});
	};

	this.takeOrdered_cb = function (num, callback) {
		// By default sort order is on number
		var numsort = function (a, b) {
		    return a - b;
		}

		UgridTask.buildTask(grid, worker, this, {
			fun: 'takeOrdered',
			args: [num, numsort],
			init: 0,
		}, function (task) {
			var nAnswer = 0, result = [];
			sendTask(task, function (err, res) {
				result = result.concat(res);
				result = result.sort(numsort);
				result = result.slice(0, num);
				if (++nAnswer == worker.length) callback(null, result);
			});
		});
	};

	this.takeSample_cb = function (withReplacement, num, seed, callback) {
		var tmp = this;
		UgridTask.buildTask(grid, worker, this, {
			fun: 'count',
			args: [],
			init: 0,
		}, function (task) {
			var nAnswer = 0, result = 0;
			sendTask(task, function (err, res) {
				result += res;
				if (++nAnswer == worker.length) {
					var a = new UgridArray(grid, worker, [tmp], 'wide', 'sample', [withReplacement, num / result, seed]);
					a.collect_cb(callback);
				}
			});
		});
	};

	this.count_cb = function (callback) {
		UgridTask.buildTask(grid, worker, this, {
			fun: 'count',
			args: [],
			init: 0,
		}, function (task) {
			var nAnswer = 0, result = 0;
			sendTask(task, function (err, res) {
				result += res;
				if (++nAnswer == worker.length) callback(null, result);
			});
		});
	};

	this.collect_cb = function (callback) {
		UgridTask.buildTask(grid, worker, this, {
			fun: 'collect',
			args: [],
			init: {}
		}, function (task) {
			var nAnswer = 0, vect = [];
			sendTask(task, function (err, res) {
				for (var p in res) vect = vect.concat(res[p]);
				if (++nAnswer == worker.length) callback(null, vect);
			});
		});
	};

	this.lookup_cb = function (key, callback) {
		UgridTask.buildTask(grid, worker, this, {
			fun: 'lookup',
			args: [key],
			init: {}
		}, function (task) {
			var nAnswer = 0, vect = [];
			sendTask(task, function (err, res) {
				for (var p in res) vect = vect.concat(res[p]);
				if (++nAnswer == worker.length) callback(null, vect);
			});
		});
	};

	this.countByValue_cb = function (callback) {
		UgridTask.buildTask(grid, worker, this, {
			fun: 'countByValue',
			args: [],
			init: {}
		}, function (task) {
			var nAnswer = 0, tmp = {};
			sendTask(task, function (err, res) {
				for (var i in res) {
					if (tmp[i] == undefined) tmp[i] = res[i];
					else tmp[i][1] += res[i][1];
				}
				if (++nAnswer == worker.length) {
					var vect = [];
					for (var i in tmp) vect.push(tmp[i]);
					callback(null, vect);
				}
			});
		});
	};

	this.forEach_cb = function (eacher, callback) {
		UgridTask.buildTask(grid, worker, this, {
			fun: 'forEach',
			args: [],
			src: eacher.toString(),
			init: {}
		}, function (task) {
			var nAnswer = 0;
			sendTask(task, function (err, res) {
				if (++nAnswer == worker.length)
					callback(null, null);
			});
		});
	};

	this.collect = thunkify(this.collect_cb);
	this.count = thunkify(this.count_cb);
	this.countByValue = thunkify(this.countByValue_cb);
	this.take = thunkify(this.take_cb);
	this.top = thunkify(this.top_cb);
	this.takeOrdered = thunkify(this.takeOrdered_cb);
	this.takeSample = thunkify(this.takeSample_cb);
	this.reduce = thunkify(this.reduce_cb);
	this.forEach = thunkify(this.forEach_cb);

	this.lookup = thunkify(this.lookup_cb);
}

module.exports = UgridArray;
