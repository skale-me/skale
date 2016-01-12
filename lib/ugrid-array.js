'use strict';

var trace = require('line-trace');
var fs = require('fs');

var ugridify  = require('./ugridify.js');
var Lines = require('./lines.js');
var id = 0;

function UgridArray(uc, dependencies, transform, attr) {
	var self = this;

	this.id = id++;
	this.dependencies = dependencies;
	this.persistent = false;
	this.transform = transform;
	this.attr = attr;

	this.map = function (mapper, args) {
		return new UgridArray(uc, [this], 'Map', {mapper: mapper.toString(), args: args});
	};

	this.flatMap = function (mapper, args) {
		return new UgridArray(uc, [this], 'FlatMap', {mapper: mapper.toString(), args: args});
	};

	this.mapValues = function (mapper, args) {
		return new UgridArray(uc, [this], 'MapValues', {mapper: mapper.toString(), args: args});
	};

	this.flatMapValues = function (mapper, args) {
		return new UgridArray(uc, [this], 'FlatMapValues', {mapper: mapper.toString(), args: args});
	};

	this.filter = function (filter, args) {
		return new UgridArray(uc, [this], 'Filter', {filter: filter.toString(), args: args});
	};

	this.reduceByKey = function (reducer, init, args) {
		if (arguments.length < 2) throw new Error('Missing argument for function reduceByKey().');
		return new UgridArray(uc, [this], 'ReduceByKey', {reducer: reducer.toString(), init: init, args: args});
	};

	this.sample = function (withReplacement, frac, seed) {
		return new UgridArray(uc, [this], 'Sample', {withReplacement: withReplacement, frac: frac, seed: seed || 1});
	};

	this.groupByKey = function () {
		return new UgridArray(uc, [this], 'GroupByKey');
	};

	this.union = function (other) {
		if (other.id == this.id) return this;
		return new UgridArray(uc, [this, other], 'Union');
	};

	this.join = function (other) {
		if (other.id == this.id) return this;
		return new UgridArray(uc, [this, other], 'Join');
	};

	this.leftOuterJoin = function (other) {
		if (other.id == this.id) return this;
		return new UgridArray(uc, [this, other], 'LeftOuterJoin');
	};

	this.rightOuterJoin = function (other) {
		if (other.id == this.id) return this;
		return new UgridArray(uc, [this, other], 'RightOuterJoin');
	};

	this.coGroup = function (other) {
		if (other.id == this.id) return this;
		return new UgridArray(uc, [this, other], 'CoGroup');
	};

	this.cartesian = function (other) {
		if (other.id == this.id) return this;	// FIXME: this is wrong!
		return new UgridArray(uc, [this, other], 'Cartesian');
	};

	this.distinct = function () {
		return new UgridArray(uc, [this], 'Distinct');
	};

	this.intersection = function (other) {
		return new UgridArray(uc, [this, other], 'Intersection');
	};

	this.subtract = function (other) {
		return new UgridArray(uc, [this, other], 'Subtract');
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

	this.persist = function () {
		this.persistent = true;
		return this;
	};	
}

module.exports = UgridArray;

function buildJob(uc, opt, array, action, callback, initValue, combiner) {
	var num = 1;														// nodes base index for treewalk computation
	var jobId = action.jobId = uc.jobId++;								// index of current job
	var stream = opt._stream || new uc.createReadStream(jobId, opt);	// user redable stream instance
	var job = [];														// Array of job data to be sent to workers
	var nodeNums = [];													// Array of node num to prevent multiple inclusion of same node

	// Store job in context job array
	uc.jobs[jobId] = {stream: stream, streamsToLaunch: []};

	// Callback executed when entering a node during treewalk
	function cin(a, done) {
		if (nodeNums.indexOf(a.num) == -1) a.num = num++;
		done();
	}

	// Callback executed when leaving a node during treewalk
	function cout(a, done) {
		if (nodeNums.indexOf(a.num) != -1) return done();	// if already in return
		nodeNums.push(a.num);

		function done2(split) {
			for (var i = 0; i < uc.worker.length; i++) {
				var data = {
					id: a.id,														// id du dataset
					type: a.transform,												// chaine de caractère correspondant à la classe du DA
					persistent: a.persistent,										// si l'on souhaite faire persister la data
					dependencies: a.dependencies.map(function(e) {return e.id;}),	// vecteur de dépendances
					attr: a.attr
				}
				if (split) data.split = split[i];
				job[i].node.push(data);
			}
			if (a.transform == 'Stream') {
				uc.streams[a.streamId].push(jobId);
				uc.jobs[jobId].streamsToLaunch[a.streamId] = a;
			}
			done();
		}

		if (a.getSplits) a.getSplits(done2) 
		else done2();
	}

	// Obtain a stream containing worker data.
	// Either from a local file or a remote if local fails.
	function getWorkerFile(file, callback) {
		var s = fs.createReadStream(file);
		s.once('open', function () {
			callback(s);
		});
		s.once('error', function () {
			var af = file.split('/');
			var anu = af[5].split('_');
			var worker = uc.worker[anu[1]];
			callback(uc.createStreamFrom(worker.uuid, {cmd: 'sendFile', path: file}));
		});
	}

	// Callback executed when treewalk has been completed
	function postTreewalk() {
		var nAnswer = 0, files = [], mainResult = JSON.parse(JSON.stringify(initValue));

		uc.on(jobId, function(res) {
			if (res.stream) {
				files.push(res.data);
				if (++nAnswer < uc.worker.length) return;
				var i = 0;
				function streamFile(file) {
					var lines = new Lines();
					getWorkerFile(file, function (s) {
						s.pipe(lines);
						lines.on('data', function(data) {stream.write(JSON.parse(data));});
						lines.on('end', function() {
							if (++i < files.length) streamFile(files[i]);
							else done();
						});
					});
				}
				streamFile(files[0]);
			} else {
				mainResult = combiner(mainResult, res.data);
				if (++nAnswer < uc.worker.length) return;
				if (!opt.stream) {
					callback(null, mainResult);
					done();
				} else stream.write(mainResult, done);
			}
			function done() {
				uc.removeAllListeners(jobId);
				if (jobId === uc.jobId - 1) uc.sock.unref();
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
			for (var i = 0; i < n.dependencies.length; i++) {
				asyncTreewalk(n.dependencies[i], cin, cout, function() {
					if (++nChild == n.dependencies.length) cout(n, endCallback);
				});
			}
			if (n.dependencies.length == 0) cout(n, endCallback);
		});
	}

	function rpc(cmd, workerNum, args, contextId, jobId, callback) {
		uc.request(uc.worker[workerNum], {cmd: cmd, args: args, master_uuid: uc.uuid, jobId: jobId, contextId: contextId}, callback);
	}

	return stream;
}
