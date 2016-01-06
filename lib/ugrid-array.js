'use strict';

var fs = require('fs'), url = require('url'), path = require('path');
var uuid = require('node-uuid');
var ugridify  = require('./ugridify.js');
var splitLocalFile = require('../utils/readsplit.js').splitLocalFile;
var splitHDFSFile = require('../utils/readsplit.js').splitHDFSFile;
// var ml = require('./ugrid-ml.js');		// for sample we have to break the cyclic dependency on ugrid-ml

function Dataset(uc, dependencies) {
	this.id = uc.datasetIdCounter++;
	this.dependencies = dependencies || [];
	this.persistent = false;

	this.persist = function () {this.persistent = true; return this;};

	this.map = function (mapper, args) {return new Map(uc, this, mapper, args);};

	this.flatMap = function (mapper, args) {return new FlatMap(uc, this, mapper, args);};

	this.mapValues = function (mapper, args) {return new MapValues(uc, this, mapper, args);};

	this.flatMapValues = function (mapper, args) {return new FlatMapValues(uc, this, mapper, args);};

	this.filter = function (filter, args) {return new Filter(uc, this, filter, args);};

	this.sample = function (withReplacement, frac, seed) {return new Sample(uc, this, withReplacement, frac, seed || 1);};

	this.union = function (other) {return (other.id == this.id) ? this : new Union(uc, [this, other]);};

	this.aggregateByKey = function(combiner, reducer, init, args) {
		if (arguments.length < 3) throw new Error('Missing argument for function aggregateByKey().');
		return new AggregateByKey(uc, [this], combiner, reducer, init, args);
	}

	this.reduceByKey = function (reducer, init, args) {
		if (arguments.length < 2) throw new Error('Missing argument for function reduceByKey().');
		return new AggregateByKey(uc, [this], reducer, reducer, init, args);
	};

	this.groupByKey = function () {
		function reducer(a, b) {a.push(b); return a;}
		function combiner(a, b) {return a.concat(b);}
		return new AggregateByKey(uc, [this], combiner, reducer, [], {});
	};

	this.coGroup = function (other) {
		function reducer(a, b) {a.push(b); return a;};
		function combiner(a, b) {
			for (var i = 0; i < b.length; i++) a[i] = a[i].concat(b[i]);
			return a;
		};
		return new AggregateByKey(uc, [this, other], combiner, reducer, [], {});
	};

	this.cartesian = function (other) {return new Cartesian(uc, [this, other]);};

	this.join = function (other) {
		return this.coGroup(other).flatMapValues(function(v) {
			var res = [];
			for (var i in v[0])
				for (var j in v[1])
					res.push([v[0][i], v[1][j]])
			return res;
		});
	};

	this.leftOuterJoin = function (other) {
		return this.coGroup(other).flatMapValues(function(v) {
			var res = [];
			if (v[1].length == 0) {
				for (var i in v[0]) res.push([v[0][i], null]);
			} else {
				for (var i in v[0])
					for (var j in v[1]) res.push([v[0][i], v[1][j]]);
			}
			return res;
		});
	};

	this.rightOuterJoin = function (other) {
		return this.coGroup(other).flatMapValues(function(v) {
			var res = [];
			if (v[0].length == 0) {
				for (var i in v[1]) res.push([null, v[1][i]]);
			} else {
				for (var i in v[0])
					for (var j in v[1]) res.push([v[0][i], v[1][j]]);
			}
			return res;
		});
	};

	this.distinct = function () {
		return this.map(function(e) {return [e, null]}).reduceByKey(function(a, b) {return a;}, null).map(function(a) {return a[0]});
	};

	this.intersection = function (other) {
		var a = this.map(function(e) {return [e, 0]}).reduceByKey(function(a, b) {return ++a;}, 0);
		var b = other.map(function(e) {return [e, 0]}).reduceByKey(function(a, b) {return ++a;}, 0);
		return a.coGroup(b).flatMap(function(a) {return (a[1][0].length && a[1][1].length) ? [a[0]] : [];})
	};

	this.subtract = function (other) {
		var a = this.map(function(e) {return [e, 0]}).reduceByKey(function(a, b) {return ++a;}, 0);
		var b = other.map(function(e) {return [e, 0]}).reduceByKey(function(a, b) {return ++a;}, 0);
		return a.coGroup(b).flatMap(function(a) {
			var res = [];
			if (a[1][0].length && (a[1][1].length == 0))
				for (var i = 0; i < a[1][0][0]; i++) res.push(a[0]);
			return res;
		});
	};

	this.keys = function () {return this.map(function(a) {return a[0];});};

	this.values = function () {return this.map(function(a) {return a[1];});};

	this.lookup = function (key) {
		return this.filter(function (kv, args) {return (kv[0] === args.key);}, {key: key}).map(function (kv) {return kv[1]}).collect();
	};

	this.countByValue = function () {
		return this.map(function (e) {return [e, 1]}).reduceByKey(function (a, b) {return a + b}, 0).collect();
	};

	this.countByKey = function () {
		return this.mapValues(function (v) {return 1;}).reduceByKey(function (a, b) {return a + b}, 0).collect();
	};

	this.collect = function (opt) {
		opt = opt || {};
		opt.stream = true;
		var reducer = function(a, b) {a.push(b); return a;}
		var combiner = function(a, b) {return a.concat(b);}
		var init = [], action = {args: [], src: reducer, init: init};

		return uc.runJob(opt, this, action, function(job, tasks) {
			var mainResult = JSON.parse(JSON.stringify(init)), cnt = 0;
			function taskDone(err, res) {
				mainResult = combiner(mainResult, res.data);
				if (++cnt < tasks.length) uc.runTask(tasks[cnt], taskDone);
				else {
					for (var i = 0; i < mainResult.length; i++) 
						job.stream.write(mainResult[i]);
					job.stream.end();
				}
			}

			uc.runTask(tasks[cnt], taskDone);
		});
	};

	this.first = function(opt) {return this.take(1, opt);}

	this.take = function (N, opt) {
		opt = opt || {};
		opt.stream = true;
		var reducer = function(a, b) {a.push(b); return a;}
		var combiner = function(a, b) {return a.concat(b);}
		var init = [], action = {args: [], src: reducer, init: init};

		return uc.runJob(opt, this, action, function(job, tasks) {
			var mainResult = JSON.parse(JSON.stringify(init)), cnt = 0;

			function taskDone(err, res) {
				mainResult = combiner(mainResult, res.data);
				if ((++cnt < tasks.length) && (mainResult.length < N)) uc.runTask(tasks[cnt], taskDone);
				else {
					for (var i = 0; i < Math.min(N, mainResult.length); i++) job.stream.write(mainResult[i]);
					job.stream.end();
				}
			}

			uc.runTask(tasks[cnt], taskDone);
		});
	};

	this.top = function (N, opt) {
		opt = opt || {};
		opt.stream = true;
		var reducer = function(a, b) {a.push(b); return a;}
		var combiner = function(a, b) {return b.concat(a);}
		var init = [], action = {args: [], src: reducer, init: init};

		return uc.runJob(opt, this, action, function(job, tasks) {
			var mainResult = JSON.parse(JSON.stringify(init)), cnt = tasks.length - 1;

			function taskDone(err, res) {
				mainResult = combiner(mainResult, res.data);
				if (--cnt >= 0 && (mainResult.length < N)) uc.runTask(tasks[cnt], taskDone);
				else {
					for (var i = mainResult.length - N; i < mainResult.length; i++) job.stream.write(mainResult[i]);
					job.stream.end();
				}
			}

			uc.runTask(tasks[cnt], taskDone);
		});
	};

	this.aggregate = ugridify(function (reducer, combiner, init, opt, callback) {
		opt = opt || {};
		if (arguments.length < 5) callback = opt;
		var action = {args: [], src: reducer, init: init};

		return uc.runJob(opt, this, action, function(job, tasks) {
			var mainResult = JSON.parse(JSON.stringify(init)), cnt = 0;

			for (var i = 0; i < tasks.length; i++)
				uc.runTask(tasks[i], function (err, res) {
					mainResult = combiner(mainResult, res.data);
					if (++cnt < tasks.length) return;
					if (!opt.stream) {
						callback(null, mainResult);
						done();
					} else stream.write(mainResult, done);
					function done() {job.stream.end();}
				});
		});
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

	this.getPartitions = function(done) {
		if (this.partitions == undefined) {
			this.partitions = [];
			var cnt = 0;
			for (var i = 0; i < this.dependencies.length; i++)
				for (var j = 0; j < this.dependencies[i].partitions.length; j++) {
					this.partitions[cnt] = new Partition(this.id, cnt, this.dependencies[i].id, this.dependencies[i].partitions[j].partitionIndex);
					cnt++;
				}
		}
		done();
	}

	this.getPreferedLocation = function(pid) {return []};

	this.partitioner = function(data) {	// Key/value shuffle DEFAULT HASH PARTITIONER
		function cksum(o) {
			var i, h = 0, s = o.toString(), len = s.length;
			for (i = 0; i < len; i++) {
				h = ((h << 5) - h) + s.charCodeAt(i);
				h = h & h;	// convert to 32 bit integer
			}
			return Math.abs(h);
		}
		return cksum(data[0]) % this.partitions.length;
	}
}

function Partition(RDDId, partitionIndex, parentRDDId, parentPartitionIndex) {
	this.data = [];
	this.RDDId = RDDId;
	this.partitionIndex = partitionIndex;
	this.parentRDDId = parentRDDId;
	this.parentPartitionIndex = parentPartitionIndex;

	this.transform = function(context, data) {
		for (var i = 0; i < data.length; i++) this.data.push(data[i]);
		return data;
	}

	this.iterate = function(task, p, pipeline, done) {
		var buffer;
		for (var i = 0; i < this.data.length; i++) {
			buffer = [this.data[i]];
			for (var t = 0; t < pipeline.length; t++)
				buffer = pipeline[t].transform(pipeline[t], buffer);
		}
		done();
	}	
}

function Parallelize(uc, localArray, nPartitions) {
	if ((localArray == undefined) || (localArray == null) || !(localArray instanceof Array))
		throw new Error('First argument of function parallelize() must be an instance of class Array.');
	Dataset.call(this, uc);

	this.getPartitions = function(done) {
		var P = nPartitions || uc.worker.length;	// as many partitions as workers by default

		function split(a, n) {
			var len = a.length, out = [], i = 0;
			while (i < len) {
				var size = Math.ceil((len - i) / n--);
				out.push(a.slice(i, i += size));
			}
			return out;
		}
		this.splits = split(localArray, P);
		this.partitions = [];
		for (var i = 0; i < this.splits.length; i++)
			this.partitions[i] = new Partition(this.id, i);
		done();
	};

	this.iterate = function(task, p, pipeline, done) {
		var buffer;
		for (var i = 0; i < this.splits[p].length; i++) {
			buffer = [this.splits[p][i]];
			for (var t = 0; t < pipeline.length; t++)
				buffer = pipeline[t].transform(pipeline[t], buffer);
		}
		done();
	}
}

function Stream(uc, stream, type, config) { // type = 'line' ou 'object'
	var id = uuid.v4();
	var tmpFile = '/tmp/ugrid/tmp/' + id;
	var targetFile = '/tmp/ugrid/stream/' + id;
	var out = fs.createWriteStream(tmpFile);
	var dataset = uc.textFile(targetFile);

	dataset.watched = true;						// notify ugrid to wait for file before launching
	out.on('close', function() {
		fs.renameSync(tmpFile, targetFile);
		dataset.watched = false;
	});
	stream.pipe(out);
	return dataset;
}

function TextFile(uc, file, nPartitions) {
	Dataset.call(this, uc);
	this.file = file;

	this.getPartitions = function(done) {
		var self = this;

		function getSplits() {
			var nSplit = nPartitions || uc.worker.length;		// as many partitions as workers by default
			var u = url.parse(self.file);

			if ((u.protocol == 'hdfs:') && u.slashes && u.hostname && u.port) splitHDFSFile(u.path, nSplit, mapLogicalSplit);
			else splitLocalFile(u.path, nSplit, mapLogicalSplit);

			function mapLogicalSplit(split) {
				self.splits = split;
				self.partitions = [];
				for (var i = 0; i < self.splits.length; i++)
					self.partitions[i] = new Partition(self.id, i);
				done();
			}
		}

		if (this.watched) {
			var watcher = fs.watch('/tmp/ugrid/stream', function (event, filename) {
				if ((event == 'rename') && (filename == path.basename(self.file))) {
					watcher.close();	// stop watching directory
					getSplits();
				}
			});
		} else getSplits();
	};

	this.iterate = function(job, p, pipeline, done) {
		var buffer, readSplit = this.readSplit;

		function processLine(line) {
			if (!line) return;	// skip empty lines
			buffer = [line];
			for (var t = 0; t < pipeline.length; t++)
				buffer = pipeline[t].transform(pipeline[t], buffer);
		}

		readSplit(this.splits, this.splits[p].index, processLine, done);
	}

	this.getPreferedLocation = function(pid) {return this.splits[pid].ip;}
}

function Map(uc, parent, mapper, args) {
	Dataset.call(this, uc, [parent]);
	this.mapper = mapper;
	this.args = args;

	this.transform = function map(context, data) {
		var tmp = [];
		for (var i = 0; i < data.length; i++)
			tmp[i] = this.mapper(data[i], this.args, this.global);
		return tmp;
	}
}

function FlatMap(uc, parent, mapper, args) {
	Dataset.call(this, uc, [parent]);
	this.mapper = mapper;
	this.args = args;

	this.transform = function flatmap(context, data) {
		var tmp = [];
		for (var i = 0; i < data.length; i++) 
			tmp = tmp.concat(this.mapper(data[i], this.args, this.global));
		return tmp;
	}
}

function MapValues(uc, parent, mapper, args) {
	Dataset.call(this, uc, [parent]);
	this.mapper = mapper;
	this.args = args;

	this.transform = function(context, data) {
		var tmp = [];
		for (var i = 0; i < data.length; i++) 
			tmp[i] = [data[i][0], this.mapper(data[i][1], this.args, this.global)];
		return tmp;
	}
}

function FlatMapValues(uc, parent, mapper, args) {
	Dataset.call(this, uc, [parent]);
	this.mapper = mapper;
	this.args = args;

	this.transform = function(context, data) {
		var tmp = [];
		for (var i = 0; i < data.length; i++) {
			var t0 = this.mapper(data[i][1], this.args, this.global);
			tmp = tmp.concat(t0.map(function(e) {return [data[i][0], e];}));
		}
		return tmp;
	}
}

function Filter(uc, parent, filter, args) {
	Dataset.call(this, uc, [parent]);
	this.filter = filter;
	this.args = args;

	this.transform = function(context, data) {
		var tmp = [];
		for (var i = 0; i < data.length; i++)
			if (this.filter(data[i], this.args, this.global)) tmp.push(data[i]);
		return tmp;
	}
}

function Sample(uc, parent, withReplacement, frac, seed) {
	Dataset.call(this, uc, [parent]);
	this.withReplacement = withReplacement;
	this.frac = frac;
	this.seed = seed;
	this.rng = withReplacement ? new ml.Poisson(frac, seed) : new ml.Random(seed);

	this.transform = function(context, data) {
		var tmp = [];
		if (this.withReplacement) {
			for (var i = 0; i < data.length; i++)
				for (var j = 0; j < this.rng.sample(); j++) tmp.push(data[i]);
		} else {
			for (var i = 0; i < data.length; i++)
				if (this.rng.nextDouble() < this.frac) tmp[i] = data[i];
		}
		return tmp;
	}
}

function Union(uc, parents) {
	Dataset.call(this, uc, parents);

	this.transform = function(context, data) {return data;}
}

function AggregateByKey(uc, dependencies, combiner, reducer, init, args) {
	Dataset.call(this, uc, dependencies);
	this.combiner = combiner;
	this.reducer = reducer;
	this.init = init;
	this.args = args;
	this.shuffling = true;
	this.executed = false;
	this.buffer = [];

	this.getPartitions = function(done) {
		if (this.partitions == undefined) {
			var P = 0;
			this.partitions = [];
			for (var i = 0; i < this.dependencies.length; i++)
				P = Math.max(P, this.dependencies[i].partitions.length);
			for (var i = 0; i < P; i++) this.partitions[i] = new Partition(this.id, i);
		}
		done();
	}

	this.transform = function(context, data) {
		for (var i = 0; i < data.length; i++) {
			var key = data[i][0], value = data[i][1], str = JSON.stringify(key), pid = this.partitioner(data[i]);
			if (this.buffer[pid] == undefined) this.buffer[pid] = {};
			if (this.buffer[pid][str] == undefined) this.buffer[pid][str] = JSON.parse(JSON.stringify(this.init));
			this.buffer[pid][str] = this.reducer(this.buffer[pid][str], value, this.args, this.global);
		}
	}

	this.spillToDisk = function(task, done) {
		if (this.dependencies.length > 1) {									// COGROUP
			var isLeft = (this.shufflePartitions[task.pid].parentRDDId == this.dependencies[0].id);
			for (var i = 0; i < this.partitions.length; i++) {
				var str = '', path = '/tmp/ugrid/shuffle/' + this.uuid.v4();
				for (var hash in this.buffer[i]) {
					var data = isLeft ? [JSON.parse(hash), [this.buffer[i][hash], []]] : [JSON.parse(hash), [[], this.buffer[i][hash]]];
					str += JSON.stringify(data) + '\n';
				}
				this.fs.appendFileSync(path, str);
				task.files[i] = {host: 'localhost', path: path};
			}
		} else { 															// AGGREGATE BY KEY			
			for (var i = 0; i < this.partitions.length; i++) {
				var str = '', path = '/tmp/ugrid/shuffle/' + this.uuid.v4();
				for (var hash in this.buffer[i]) {
					var data = [JSON.parse(hash), this.buffer[i][hash]];
					str += JSON.stringify(data) + '\n';
				}
				this.fs.appendFileSync(path, str);
				task.files[i] = {host: 'localhost', path: path};
			}
		}
		done();
	}

	this.iterate = function(job, p, pipeline, done) {
		var self = this, cbuffer = {}, cnt = 0, files = [];

		for (var i = 0; i < self.shufflePartitions.length; i++)
			files.push(self.shufflePartitions[i].files[p].path);

		processShuffleFile(files[cnt], processDone);

		function processShuffleFile(file, done) {
			var lines = new self.Lines();
			self.fs.createReadStream(file).pipe(lines);
			lines.on('data', function(line) {
				var data = JSON.parse(line), key = data[0], value = data[1], hash = JSON.stringify(key);
				if (cbuffer[hash] != undefined) cbuffer[hash] = self.combiner(cbuffer[hash], value, self.args, self.global);
				else cbuffer[hash] = value;
			});
			lines.on('end', done);
		}

		function processDone() {
			if (++cnt == files.length) {
				for (var key in cbuffer) {
					var buffer = [[JSON.parse(key), cbuffer[key]]];
					for (var t = 0; t < pipeline.length; t++)
						buffer = pipeline[t].transform(pipeline[t], buffer);
				}
				done();
			} else processShuffleFile(files[cnt], processDone);
		}
	}
}

module.exports = {Dataset: Dataset, Partition: Partition, Parallelize: Parallelize, TextFile: TextFile, Stream: Stream};