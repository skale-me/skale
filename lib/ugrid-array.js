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

	this.getArgs = function(i) {
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
		return new UgridArray(grid, worker, [this, withArray], 'wide', 'join', [withArray.id]);
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

	this.intersection = function (withArray) {
		return new UgridArray(grid, worker, [this, withArray], 'wide', 'intersection', []);
	};

	this.substract = function (withArray) {
		return new UgridArray(grid, worker, [this, withArray], 'wide', 'substract', []);
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

	this.reduce = thunkify(this.reduce_cb);
	this.count = thunkify(this.count_cb);
	this.collect = thunkify(this.collect_cb);
	this.lookup = thunkify(this.lookup_cb);
	this.countByValue = thunkify(this.countByValue_cb);	
	this.forEach = thunkify(this.forEach_cb);		
}

module.exports = UgridArray;
