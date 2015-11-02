'use strict';

var trace = require('line-trace');
var fs = require('fs');

var ugridify  = require('./ugridify.js');
var Lines = require('./lines.js');
var id = 0;

function UgridArray(uc, child, dependency, transform, args, src) {
	var self = this;

	this.id = id++;
	this.inMemory = false;
	this.child = child;
	this.persistent = false;
	this.transform = transform;
	this.dependency = dependency;
	this.args = args || {};
	this.src = src;

	this.getArgs = function(callback) {		// By default duplicate same args for each worker
		callback(uc.worker.map(function(e) {return self.args}));
	}

	this.persist = function () {
		this.persistent = true;
		return this;
	};

	this.map = function (mapper, args) {
		return new UgridArray(uc, [this], 'narrow', 'map', args, mapper.toString());
	};

	this.flatMap = function (mapper, args) {
		return new UgridArray(uc, [this], 'narrow', 'flatMap', args, mapper.toString());
	};

	this.mapValues = function (mapper, args) {
		return new UgridArray(uc, [this], 'narrow', 'mapValues', args, mapper.toString());
	};

	this.flatMapValues = function (mapper, args) {
		return new UgridArray(uc, [this], 'narrow', 'flatMapValues', args, mapper.toString());
	};

	this.filter = function (filter, args) {
		return new UgridArray(uc, [this], 'narrow', 'filter', args, filter.toString());
	};

	this.reduceByKey = function (reducer, initValue, args) {
		if (arguments.length < 2) throw new Error('Missing argument for function reduceByKey().');
		return new UgridArray(uc, [this], 'wide', 'reduceByKey', [initValue, args], reducer.toString());
	};

	this.sample = function (withReplacement, frac, seed) {
		return new UgridArray(uc, [this], 'wide', 'sample', [withReplacement, frac, seed || 1]);
	};

	this.groupByKey = function () {
		return new UgridArray(uc, [this], 'wide', 'groupByKey', []);
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

	this.distinct = function () {
		return new UgridArray(uc, [this], 'wide', 'distinct', []);
	};

	this.intersection = function (other) {
		return new UgridArray(uc, [this, other], 'wide', 'intersection', []);
	};

	this.subtract = function (other) {
		return new UgridArray(uc, [this, other], 'wide', 'subtract', []);
	};

	this.keys = function () {
		return this.map(function(a) {return a[0];});
	};

	this.values = function () {
		return this.map(function(a) {return a[1];});
	};

	this.collect = function (opt) {
		opt = opt || {};
		opt.stream = true;
		return buildJob(uc, opt, this, {fun: 'collect'}, null, null);
	};

	this.lookup = function (key) {
		return this.filter(function (kv, args) {return (kv[0] === args.key);}, {key: key}).map(function (kv) {return kv[1]}).collect();
	};

	this.countByValue = function () {
		return this.map(function (e) {return [e, 1]}).reduceByKey(function (a, b) {return a + b}, 0).collect();
	};

	this.countByKey = function () {
		return this.mapValues(function (v) {return 1;}).reduceByKey(function (a, b) {return a + b}, 0).collect();
	};

	this.aggregate = ugridify(function (reducer, combiner, init, opt, callback) {
		opt = opt || {};
		if (arguments.length < 5) callback = opt;
		var action = {fun: 'aggregate', args: [init], src: reducer.toString(), init: init};
		return buildJob(uc, opt, this, action, callback, init, combiner);
	});

	this.reduce = ugridify(function (reducer, init, opt, callback) {
		opt = opt || {};
		if (arguments.length < 4) callback = opt;
		return this.aggregate(reducer, reducer, init, opt, callback);
	});

	this.count = ugridify(function (opt, callback) {
		opt = opt || {};
		if (arguments.length < 2) callback = opt;
		function reducer(a, b) {return ++a;}
		function combiner(a, b) {return a = a + b;}
		return this.aggregate(reducer, combiner, 0, opt, callback);
	});

	this.forEach = ugridify(function (eacher, opt, callback) {
		opt = opt || {};
		if (arguments.length < 3) callback = opt;
		var combiner = function(a, b) {return null;}
		return this.aggregate(eacher, combiner, null, opt, callback);
	});
}

module.exports = UgridArray;

function buildJob(uc, opt, array, action, callback, initValue, combiner) {
	var num = 1;													// nodes base index for treewalk computation
	var jobId = action.jobId = uc.jobId++;							// index of current job
	var stream = opt._stream || new uc.createReadStream(jobId, opt);		// user redable stream instance
	var stageNode;													// Stage Node temprary var for treewalk
	var job = [];													// Array of job data to be sent to workers
	var nodeNums = [];												// Array of node num to prevent multiple inclusion of same node

	// Store job in context job array
	uc.jobs[jobId] = {
		count: 0,					// number of jobEnd events received from worker, when equal to nWorker emit end event to user stream
		stream: stream,				// Instance of stream to write job results
		activeInputStreams: [],
		streamsToLaunch: []
	};

	// initialize job data destinated to each worker
	for (var i = 0; i < job.length; i++)
		job[i] = {node: [], worker: uc.worker, action: action};

	// Callback executed when entering a node during treewalk
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

	// Callback executed when leaving a node during treewalk
	function cout(a, callback) {
		if (nodeNums.indexOf(a.num) != -1) {
			callback();
			return;
		};
		nodeNums.push(a.num);
		stageNode = a.stageNode;

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
			if (a.transform == 'stream') {
				uc.streams[a.streamId].push(jobId);
				uc.jobs[jobId].activeInputStreams.push(a.streamId);
				uc.jobs[jobId].streamsToLaunch[a.streamId] = a;
			}
			callback();
		});
	}

	// Callback executed when treewalk has been completed
	function postTreewalk() {
		var mainResult = JSON.parse(JSON.stringify(initValue));		
		var nAnswer = 0, files = [];

		uc.on(jobId, function(res) {
			if (res.stream) {
				files.push(res.data);
				if (++nAnswer < uc.worker.length) return;
				var i = 0;
				function streamFile(file) {
					var lines = new Lines();
					fs.createReadStream(files[i]).pipe(lines);
					lines.on('data', function(data) {stream.write(JSON.parse(data));});
					lines.on('end', function() {
						if (++i < files.length) streamFile(files[i]);
						else done();
					});
				}
				streamFile(files[0]);
			} else {
				if (combiner != undefined) mainResult = combiner(mainResult, res.data);
				if (++nAnswer < uc.worker.length) return;

				if (!opt.stream) {
					callback(null, mainResult);
					done();
				} else stream.write(mainResult, done);
			}
			function done() {
				uc.removeAllListeners(jobId);
				stream.end();
			}
		});

		// map job to workers and synchronize job start
		var n = 0;
		for (var i = 0; i < uc.worker.length; i++)
			rpc('setJob', i, job[i], uc.contextId, jobId, function () {
				if (++n < uc.worker.length) return;
				for (var j = 0; j < uc.worker.length; j++)
					uc.send(uc.worker[j].uuid, {cmd: 'runJob', data: {jobId: jobId}});
			});
	}

	uc.getWorkers(function () {
		for (var i = 0; i < uc.worker.length; i++)
			job[i] = {node: [], worker: uc.worker, action: action};
		asyncTreewalk(array, cin, cout, postTreewalk);
	});

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

	function rpc(cmd, workerNum, args, contextId, jobId, callback) {
		uc.request(uc.worker[workerNum], {cmd: cmd, args: args, master_uuid: uc.uuid, jobId: jobId, contextId: contextId}, callback);
	}

	return stream;
}
