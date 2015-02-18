'use strict';

var thunkify = require('thunkify');
var UgridTask = require('./ugrid-task.js');
var ml = require('./ugrid-ml.js');
var util = require('util');

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
					for (var j = 0; j < worker.length; j++) {
						rpc(grid, 'runTask', j, null, callback);
					}
			});
	}

	this.getArgs = function(i) {
		var o = this.args;
		var child_id = [];
		for (var i = 0; i < child.length; i++)		
			child_id.push(child[i].id);
		return {
			args: o,
			src: src,
			child: child_id
		};
	}

	this.persist = function () {
		this.persistent = true;
		return this;
	};

	this.map = function (mapper, mapperArgs) {
		return new UgridArray(grid, worker, [this], 'narrow', 'map', mapperArgs, mapper.toString());
	};

	this.filter = function (filter, filterArgs) {
		return new UgridArray(grid, worker, [this], 'narrow', 'filter', filterArgs, filter.toString());
	};

	this.flatMap = function (mapper, mapperArgs) {
		return new UgridArray(grid, worker, [this], 'narrow', 'flatMap', mapperArgs, mapper.toString());
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

	// KO
	this.crossProduct = function (withArray) {
		if (withArray.id == this.id) return this;
		return new UgridArray(grid, worker, [this, withArray], 'wide', 'crossProduct', [withArray.id]);
	};	

	this.mapValues = function (mapper, mapperArgs) {
		return new UgridArray(grid, worker, [this], 'narrow', 'mapValues', mapperArgs, mapper.toString());
	};

	// OK
	this.reduce_cb = function (reducer, aInit, callback) {
		UgridTask.buildTask(grid, worker, this, {
			fun: 'reduce',
			args: [aInit],
			src: reducer.toString(),
			init: aInit
		}, function (task) {
			var nAnswer = 0, result = aInit;
			sendTask(task, function (err, res) {
				result = reducer(result, res);
				if (++nAnswer == worker.length)
					callback(null, result);
			});
		});
	};

	// OK
	this.count_cb = function (callback) {
		UgridTask.buildTask(grid, worker, this, {
			fun: 'count',
			args: [],
			init: 0,
		}, function (task) {
			var nAnswer = 0, result = 0;
			sendTask(task, function(err, res) {
				result += res;
				if (++nAnswer == worker.length)
					callback(null, result);
			});
		});
	};

	// OK
	this.collect_cb = function (callback) {
		var self = this;
		UgridTask.buildTask(grid, worker, this, {
			fun: 'collect',
			args: [],
			init: {}
		}, function (task) {
			var nAnswer = 0, tmp = {}, vect = [];
			sendTask(task, function (err, res) {
				for (var p in res) 
					vect = vect.concat(res[p]);
				if (++nAnswer == worker.length)
					callback(null, vect);
			});			
		});
	};

	// OK, deplacer la fonction sample dans worker ?
	this.sample_cb = function (fraction, seed, callback) {
		function sample(v, frac, seed) {
			var rng = new this.ml.Random(seed);
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
			init: [],
			post_src: sample.toString()
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

	// OK
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

	this.reduce = thunkify(this.reduce_cb);
	this.count = thunkify(this.count_cb);
	this.collect = thunkify(this.collect_cb);
	this.takeSample = thunkify(this.takeSample_cb);
	this.sample = thunkify(this.sample_cb);
}

module.exports = UgridArray;
