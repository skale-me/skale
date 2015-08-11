'use strict';

var trace = require('line-trace');

var ugridify  = require('./ugridify.js');
var id = 0;
function UgridArray(uc, child, dependency, transform, args, src) {
	var self = this;

	// this.id = Math.round(Math.random() * 1e9);			// Unique array id
	this.id = id++;											// Unique array id	
	this.inMemory = false;
	this.child = child;
	this.persistent = false;
	this.transform = transform;
	this.dependency = dependency;
	this.args = args || [];
	this.src = src;

	this.getArgs = function(callback) {			// By default duplicate same args for each worker
		callback(uc.worker.map(function(e) {return self.args}));
	}

	this.persist = function () {
		this.persistent = true;
		return this;
	};

	this.map = function (mapper, mapperArgs) {
		return new UgridArray(uc, [this], 'narrow', 'map', mapperArgs, mapper.toString());
	};

	this.keys = function () {
		var mapper = function (e) {return e[0];};
		return new UgridArray(uc, [this], 'narrow', 'map', null, mapper.toString());
	};

	this.values = function () {
		var mapper = function (e) {return e[1];};
		return new UgridArray(uc, [this], 'narrow', 'map', null, mapper.toString());
	};

	this.filter = function (filter, filterArgs) {
		return new UgridArray(uc, [this], 'narrow', 'filter', filterArgs, filter.toString());
	};

	this.flatMap = function (mapper, mapperArgs) {
		return new UgridArray(uc, [this], 'narrow', 'flatMap', mapperArgs, mapper.toString());
	};

	this.flatMapValues = function (mapper, mapperArgs) {
		return new UgridArray(uc, [this], 'narrow', 'flatMapValues', mapperArgs, mapper.toString());
	};

	this.sample = function (withReplacement, frac, seed) {
		return new UgridArray(uc, [this], 'wide', 'sample', [withReplacement, frac, seed || 1]);
	};

	this.groupByKey = function () {
		return new UgridArray(uc, [this], 'wide', 'groupByKey', []);
	};

	this.reduceByKey = function (reducer, initVal) {
		return new UgridArray(uc, [this], 'wide', 'reduceByKey', [initVal], reducer.toString());
	};

	this.union = function (other) {
		if (other.id == this.id) return this;
		return new UgridArray(uc, [this, other], 'narrow', 'union', [other.id]);
	};

	this.join = function (other) {
		if (other.id == this.id) return this;
		return new UgridArray(uc, [this, other], 'wide', 'join', [other.id, 'inner']);
	};

	this.leftOuterJoin = function (other) {
		if (other.id == this.id) return this;
		return new UgridArray(uc, [this, other], 'wide', 'join', [other.id, 'left']);
	};

	this.rightOuterJoin = function (other) {
		if (other.id == this.id) return this;
		return new UgridArray(uc, [this, other], 'wide', 'join', [other.id, 'right']);
	};

	this.coGroup = function (other) {
		if (other.id == this.id) return this;
		return new UgridArray(uc, [this, other], 'wide', 'coGroup', [other.id]);
	};

	this.crossProduct = function (other) {
		if (other.id == this.id) return this;
		return new UgridArray(uc, [this, other], 'wide', 'crossProduct', [other.id]);
	};

	this.mapValues = function (mapper, mapperArgs) {
		return new UgridArray(uc, [this], 'narrow', 'mapValues', mapperArgs, mapper.toString());
	};

	this.distinct = function () {
		return new UgridArray(uc, [this], 'wide', 'distinct', []);
	};

	this.sortByKey = function () {
		return new UgridArray(uc, [this], 'wide', 'sortByKey', []);
	};

	this.partitionByKey = function () {
		return new UgridArray(uc, [this], 'wide', 'partitionByKey', []);
	};

	this.intersection = function (other) {
		return new UgridArray(uc, [this, other], 'wide', 'intersection', []);
	};

	this.subtract = function (other) {
		return new UgridArray(uc, [this, other], 'wide', 'subtract', []);
	};

	this.collect = ugridify(function (opt, callback) {
		opt = opt || {};
		if (arguments.length < 2) callback = opt;
		var action = {fun: 'collect', args: [], init: {}};
		var init = [];
		var reducer = function(a, b) {
			for (var p in b) a = a.concat(b[p]);
			return a;
		}
		return buildJob(uc, opt, this, action, callback, init, reducer);
	});

	this.count = ugridify(function (opt, callback) {
		opt = opt || {};
		if (arguments.length < 2) callback = opt;
		var action = {fun: 'count', args: [], init: 0};
		var init = 0;
		var reducer = function(a, b) {return a + b;}
		return buildJob(uc, opt, this, action, callback, init, reducer);
	});

	this.reduce = ugridify(function (reducer, aInit, opt, callback) {
		opt = opt || {};
		if (arguments.length < 4) callback = opt;
		var action = {fun: 'reduce', args: [aInit], src: reducer.toString(), init: aInit};
		return buildJob(uc, opt, this, action, callback, aInit, reducer);
	});

	this.aggregate = ugridify(function (reducer, merger, aInit, opt, callback) {
		opt = opt || {};
		if (arguments.length < 5) callback = opt;
		var action = {fun: 'reduce', args: [aInit], src: reducer.toString(), init: aInit};
		return buildJob(uc, opt, this, action, callback, aInit, merger);
	});

	this.take = ugridify(function (num, opt, callback) {
		opt = opt || {};
		if (arguments.length < 3) callback = opt;
		var action = {fun: 'take', args: [num], init: 0};
		var init = [];
		var reducer = function(a, b) {
			for (var i = 0; i < b.length; i++)
				if (a.length < num) a.push(b[i]);
				else break;
			return a;
		}
		return buildJob(uc, opt, this, action, callback, init, reducer);
	});

	this.top = ugridify(function (num, opt, callback) {
		opt = opt || {};
		if (arguments.length < 3) callback = opt;
		var ordering = function (a, b) {return b < a;};
		var action = {fun: 'top', args: [num], src: ordering, init: 0};
		var init = [];
		var reducer = function(a, b) {
			return a.concat(b).sort(ordering).slice(0, num);
		}
		return buildJob(uc, opt, this, action, callback, init, reducer);
	});

	this.takeOrdered = ugridify(function (num, ordering, opt, callback) {
		opt = opt || {};
		if (arguments.length < 4) callback = opt;
		var action = {fun: 'takeOrdered', args: [num], src: ordering.toString(), init: 0};
		var init = [];
		var reducer = function(a, b) {
			return a.concat(b).sort(ordering).slice(0, num);
		};
		return buildJob(uc, opt, this, action, callback, init, reducer);
	});

	this.takeSample = ugridify(function (withReplacement, num, seed, opt, callback) {
		opt = opt || {};
		if (arguments.length < 5) callback = opt;
		var self = this;
		var action = {fun: 'count', args: [], init: 0};
		var init = 0;
		var reducer = function(a, b) {
			return a + b;
		};
		return buildJob(uc, opt, this, action, function(err, result) {
			var a = new UgridArray(uc, [self], 'wide', 'sample', [withReplacement, num / result, seed]);
			var stream = uc.jobs[action.jobId].stream;
			a.collect({stream: opt.stream, _stream: stream}, callback);
		}, init, reducer);
	});

	this.lookup = ugridify(function (key, opt, callback) {
		opt = opt || {};
		if (arguments.length < 3) callback = opt;
		return this.filter(function (kv, key) {return (kv[0] === key);}, [key])
			.map(function (kv) {return kv[1]})
			.collect(opt, callback);
	});

	this.countByValue = ugridify(function (opt, callback) {
		opt = opt || {};
		if (arguments.length < 2) callback = opt;
		return this.map(function (e) {return [e, 1]})
			.reduceByKey(function (a, b) {return a + b}, 0)
			.collect(opt, callback);
	});

	this.countByKey = ugridify(function (opt, callback) {
		opt = opt || {};
		if (arguments.length < 2) callback = opt;
		return this.mapValues(function (v) {return 1;})
			.reduceByKey(function (a, b) {return a + b}, 0)
			.collect(opt, callback);
	});

	this.forEach = ugridify(function (eacher, opt, callback) {
		opt = opt || {};
		if (arguments.length < 3) callback = opt;
		var action = {fun: 'forEach', args: [], src: eacher.toString(), init: {}};
		var init = null;
		var reducer = function(a, b) {return null;}
		return buildJob(uc, opt, this, action, callback, init, reducer);
	});
}

module.exports = UgridArray;

function buildJob(uc, opt, array, action, callback, initValue, reducer, postReducer) {
	var num = 1, streamUgridArrays = [];
	var jobId = action.jobId = uc.jobId++;
	var stream = opt._stream || new uc.createReadStream(jobId);
	var stageNode;
	var job = new Array(uc.worker.length);
	var nodeNums = [];

	action.targetPathCount = 1;
	uc.jobs[jobId] = {
		count: 0,
		stream: stream,
		finished: false,
		onlyStreams: true,
		iteration: 0,
		activeInputStreams: []
	};

	for (var i = 0; i < job.length; i++)
		job[i] = {node: [], worker: uc.worker, action: action};

	function cin(a, callback) {
		if (nodeNums.indexOf(a.num) != -1) {
			callback();
			return;
		}
		a.num = num++;
		a.stageNode = stageNode;
		if (a.dependency == 'wide') stageNode = a.num;
		callback();
	}

	function cout(a, callback) {
		if (nodeNums.indexOf(a.num) != -1) {
			callback();
			return;
		};
		nodeNums.push(a.num);
		stageNode = a.stageNode;
		if (a.transform == 'stream') {
			uc.streams[a.streamId].push(jobId);
			uc.jobs[jobId].activeInputStreams.push(a.streamId);
			streamUgridArrays.push(a);
		}

		if (((a.child.length == 0) && (a.transform != 'stream')) || a.inMemory)
			uc.jobs[jobId].onlyStreams = false;

		a.getArgs(function(args) {
			for (var i = 0; i < uc.worker.length; i++)
				job[i].node.push({
					args: args[i],
					id: a.id,
					type: a.transform,
					dependency: a.dependency,
					persistent: a.persistent,
					inMemory: a.inMemory,
					stageNode: a.stageNode,
					num: a.num,
					src: a.src,
					child: a.child.map(function(e) {return e.num;}),
					dependencies: a.child.map(function(e) {return e.id;}),					
					next: []
				});
			a.inMemory = a.persistent;				
			callback();
		});
	}

	asyncTreewalk(array, cin, cout, function() {
		var nAnswer = 0, dones = [];
		var mainResult = JSON.parse(JSON.stringify(initValue));
		uc.removeAllListeners(jobId);
		uc.on(jobId, function(res, done) {
			dones.push(done);
			mainResult = reducer(mainResult, res);
			if (++nAnswer < uc.worker.length) return;
			uc.jobs[jobId].iteration++;
			if (postReducer) mainResult = postReducer(mainResult);
			if (!uc.jobs[jobId].ignore) {
				if (opt.stream) {
					stream.write(mainResult, function () {
						for (var i in dones) dones[i]();
						dones = [];
					});
				} else {
					callback(null, mainResult);
					for (var i in dones) dones[i]();
					dones = [];
				}
			}
			mainResult = JSON.parse(JSON.stringify(initValue));
			nAnswer = 0;
			// unlock toWorkers input streams
			for (var s in streamUgridArrays)
				try {streamUgridArrays[s].dispatch.done();} catch (e) {};			
		});
		var n = 0;
		for (var i = 0; i < uc.worker.length; i++)
			rpc('setJob', i, job[i], jobId, function () {
				if (++n < uc.worker.length) return;
				for (var j = 0; j < uc.worker.length; j++)
					uc.send(uc.worker[j].uuid, {cmd: 'runJob', data: {jobId: jobId}});
				for (var s in streamUgridArrays)
					streamUgridArrays[s].startStream();
			});
	})

	function asyncTreewalk(n, cin, cout, endCallback) {
		cin(n, function() {
			var nChild = 0;
			for (var i = 0; i < n.child.length; i++) {
				asyncTreewalk(n.child[i], cin, cout, function() {
					if (++nChild == n.child.length) cout(n, endCallback);
				});
			}
			if (n.child.length == 0) cout(n, endCallback);
		});
	}

	function rpc(cmd, workerNum, args, jobId, callback) {
		uc.request(uc.worker[workerNum], {cmd: cmd, args: args, master_uuid: uc.uuid, jobId: jobId}, callback);
	}

	return stream;
}
