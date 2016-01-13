'use strict';

var fs = require('fs'), url = require('url'), path = require('path');
var uuid = require('node-uuid');
var ugridify  = require('./ugridify.js');
var splitLocalFile = require('../utils/readsplit.js').splitLocalFile;
var splitHDFSFile = require('../utils/readsplit.js').splitHDFSFile;

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

	this.sortBy = function (sorter, ascending, numPartitions) {
		return new SortBy(uc, this, sorter, ascending, numPartitions);		
	}

	this.partitionBy = function (partitioner) {
		return new PartitionBy(uc, this, partitioner);		
	}

	this.sortByKey = function (ascending, numPartitions) {
		return new SortBy(uc, this, function(data) {return data[0];}, ascending, numPartitions);		
	}

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
	var tmpFile = '/tmp/ugrid/' + uc.contextId + '/tmp/' + id;
	var targetFile = '/tmp/ugrid/' + uc.contextId + '/stream/' + id;
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
			var nSplit = nPartitions || uc.worker.length, u = url.parse(self.file);

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
			var watcher = fs.watch('/tmp/ugrid/' + uc.contextId + '/stream', function (event, filename) {
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

	function Random(initSeed) {
		this.seed = initSeed || 1;

		this.next = function () {
		    var x = Math.sin(this.seed++) * 10000;
		    return (x - Math.floor(x)) * 2 - 1;
		};

		this.reset = function () {
			this.seed = initSeed;
		};

		this.randn = function (N) {
			var w = new Array(N);
			for (var i = 0; i < N; i++)
				w[i] = this.next();
			return w;
		};

		this.nextDouble = function () {
			return 0.5 * this.next() + 0.5;			// Must be uniform, not gaussian
		};
	}

	function Poisson(lambda, initSeed) {
		this.seed = initSeed || 1;

		var rng = new Random(initSeed);

		this.sample = function () {
			var L = Math.exp(-lambda), k = 0, p = 1;
			do {
				k++;
				p *= rng.nextDouble();
			} while (p > L);
			return k - 1;
		}
	}

	this.rng = withReplacement ? new Poisson(frac, seed) : new Random(seed);

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
			this.partitioner = new HashPartitioner(P);				
		}
		done();
	}

	this.transform = function(context, data) {
		for (var i = 0; i < data.length; i++) {
			var key = data[i][0], value = data[i][1], str = JSON.stringify(key), pid = this.partitioner.getPartitionIndex(data[i][0]);
			if (this.buffer[pid] == undefined) this.buffer[pid] = {};
			if (this.buffer[pid][str] == undefined) this.buffer[pid][str] = JSON.parse(JSON.stringify(this.init));
			this.buffer[pid][str] = this.reducer(this.buffer[pid][str], value, this.args, this.global);
		}
	}

	this.spillToDisk = function(task, done) {
		if (this.dependencies.length > 1) {									// COGROUP
			var isLeft = (this.shufflePartitions[task.pid].parentRDDId == this.dependencies[0].id);
			for (var i = 0; i < this.partitions.length; i++) {
				var str = '', path = task.basedir + 'shuffle/' + this.uuid.v4();
				for (var hash in this.buffer[i]) {
					var data = isLeft ? [JSON.parse(hash), [this.buffer[i][hash], []]] : [JSON.parse(hash), [[], this.buffer[i][hash]]];
					str += JSON.stringify(data) + '\n';
				}
				this.fs.appendFileSync(path, str);
				task.files[i] = {host: 'localhost', path: path};
			}
		} else { 															// AGGREGATE BY KEY			
			for (var i = 0; i < this.partitions.length; i++) {
				var str = '', path = task.basedir + 'shuffle/' + this.uuid.v4();
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

	this.iterate = function(task, p, pipeline, done) {
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

function Cartesian(uc, dependencies) {
	Dataset.call(this, uc, dependencies);
	this.shuffling = true;
	this.executed = false;
	this.buffer = [];

	this.getPartitions = function(done) {
		if (this.partitions == undefined) {
			var P = this.dependencies[0].partitions.length * this.dependencies[1].partitions.length;
			this.partitions = [];
			for (var i = 0; i < P; i++)
				this.partitions[i] = new Partition(this.id, i);
		}
		done();
	}

	this.transform = function(context, data) {
		for (var i = 0; i < data.length; i++) this.buffer.push(data[i])
	}

	this.spillToDisk = function(task, done) {
		var str = '', path = task.basedir + 'shuffle/' + this.uuid.v4();		
		for (var i = 0; i < this.buffer.length; i++) 
			str += JSON.stringify(this.buffer[i]) + '\n';
		this.fs.appendFileSync(path, str);
		task.files = {host: 'localhost', path: path};
		done();
	}

	this.iterate = function(task, p, pipeline, done) {
		var pleft = this.dependencies[0].partitions.length;
		var pright = this.dependencies[1].partitions.length;		
		var p1 = Math.floor(p / pleft)
		var p2 = p % pright + pleft;
		var a1 = this.fs.readFileSync(this.shufflePartitions[p1].files.path, {encoding: 'utf8'}).split('\n');
		var a2 = this.fs.readFileSync(this.shufflePartitions[p2].files.path, {encoding: 'utf8'}).split('\n');

		for (var i = 0; i < a1.length; i++) {
			if (a1[i] == '') continue;
			for (var j = 0; j < a2.length; j++) {
				if (a2[j] == '') continue;
				var buffer = [[JSON.parse(a1[i]), JSON.parse(a2[j])]];
				for (var t = 0; t < pipeline.length; t++)
					buffer = pipeline[t].transform(pipeline[t], buffer);				
			}
		}
		done();
	}
}

function SortBy(uc, dependencies, keyFunc, ascending, numPartitions) {
	Dataset.call(this, uc, [dependencies]);
	this.shuffling = true;
	this.executed = false;
	this.keyFunc = keyFunc;
	this.ascending = (ascending == undefined) ? true : ascending;
	this.buffer = [];

	this.getPartitions = function(done) {
		if (this.partitions == undefined) {
			var P = Math.max(numPartitions || 1, this.dependencies[0].partitions.length);
			this.partitions = [];
			for (var p = 0; p < P; p++) this.partitions.push(new Partition(this.id, p));
			this.partitioner = new RangePartitioner(P, keyFunc, this.dependencies[0]);
			this.partitioner.init(done);
		} else done();
	}

	this.transform = function(context, data) {
		for (var i = 0; i < data.length; i++) {
			var pid = this.partitioner.getPartitionIndex(this.keyFunc(data[i]));
			if (this.buffer[pid] == undefined) this.buffer[pid] = [];
			this.buffer[pid].push(data[i]);
		}
	}

	this.spillToDisk = function(task, done) {
		for (var i = 0; i < this.partitions.length; i++) {
			var str = '', path = task.basedir + 'shuffle/' + this.uuid.v4();
			if (this.buffer[i] != undefined)
				for (var j = 0; j < this.buffer[i].length; j++)
					str += JSON.stringify(this.buffer[i][j]) + '\n';
			this.fs.appendFileSync(path, str);
			task.files[i] = {host: 'localhost', path: path};
		}
		done();
	}

	this.iterate = function(task, p, pipeline, done) {
		var self = this, cbuffer = [], cnt = 0, files = [];

		for (var i = 0; i < self.shufflePartitions.length; i++)
			files.push(self.shufflePartitions[i].files[p].path);

		processShuffleFile(files[cnt], processDone);

		function processShuffleFile(file, done) {
			var lines = new self.Lines();
			self.fs.createReadStream(file).pipe(lines);
			lines.on('data', function(line) {cbuffer.push(JSON.parse(line));});
			lines.on('end', done);
		}

		function processDone() {
			if (++cnt == files.length) {
				function compare(a, b) {
					if (self.keyFunc(a) < self.keyFunc(b)) return self.ascending ? -1 : 1;
					if (self.keyFunc(a) > self.keyFunc(b)) return self.ascending ? 1 : -1;
					return 0;
				}
				cbuffer.sort(compare);
				for (var i = 0; i < cbuffer.length; i++) {
					var buffer = [cbuffer[i]];
					for (var t = 0; t < pipeline.length; t++)
						buffer = pipeline[t].transform(pipeline[t], buffer);					
				}
				done();
			} else processShuffleFile(files[cnt], processDone);
		}
	}
}

function PartitionBy(uc, dependencies, partitioner) {	// nécessairement sur un clé valeur
	Dataset.call(this, uc, [dependencies]);
	this.shuffling = true;
	this.executed = false;
	this.buffer = [];
	this.partitioner = partitioner;

	this.getPartitions = function(done) {
		if (this.partitions == undefined) {
			var P = this.partitioner.numPartitions;
			this.partitions = [];
			for (var p = 0; p < P; p++) this.partitions.push(new Partition(this.id, p));
			if (this.partitioner.init) this.partitioner.init(done);
			else done();
		} else done();
	}

	this.transform = function(context, data) {
		for (var i = 0; i < data.length; i++) {
			var pid = this.partitioner.getPartitionIndex(data[i][0]);
			if (this.buffer[pid] == undefined) this.buffer[pid] = [];
			this.buffer[pid].push(data[i]);
		}
	}

	this.spillToDisk = function(task, done) {
		for (var i = 0; i < this.partitions.length; i++) {
			var str = '', path = task.basedir + 'shuffle/' + this.uuid.v4();
			if (this.buffer[i] != undefined)
				for (var j = 0; j < this.buffer[i].length; j++)
					str += JSON.stringify(this.buffer[i][j]) + '\n';
			this.fs.appendFileSync(path, str);
			task.files[i] = {host: 'localhost', path: path};
		}
		done();
	}

	this.iterate = function(task, p, pipeline, done) {
		var self = this, cbuffer = [], cnt = 0, files = [];
		for (var i = 0; i < self.shufflePartitions.length; i++)
			files.push(self.shufflePartitions[i].files[p].path);

		processShuffleFile(files[cnt], processDone);

		function processShuffleFile(file, done) {
			var lines = new self.Lines();
			self.fs.createReadStream(file).pipe(lines);
			lines.on('data', function(line) {cbuffer.push(JSON.parse(line));});
			lines.on('end', done);
		}

		function processDone() {
			if (++cnt == files.length) {
				for (var i = 0; i < cbuffer.length; i++) {
					var buffer = [cbuffer[i]];
					for (var t = 0; t < pipeline.length; t++)
						buffer = pipeline[t].transform(pipeline[t], buffer);					
				}
				done();
			} else processShuffleFile(files[cnt], processDone);
		}
	}
}

function RangePartitioner(numPartitions, keyFunc, dataset) {
	this.numPartitions = numPartitions;

	this.init = function(done) {
		var self = this;
		dataset.sample(false, 0.5).collect().toArray(function(err, result) {
			function compare(a, b) {
				if (keyFunc(a) < keyFunc(b)) return -1;
				if (keyFunc(a) > keyFunc(b)) return 1;
				return 0;
			}
			result.sort(compare);
			self.upperbounds = [];
			if (result.length <= numPartitions - 1) {
				self.upperbounds = result;	// supprimer les doublons peut-etre ici
			} else {
				var s = Math.floor(result.length / numPartitions);
				for (var i = 0; i < numPartitions - 1; i++) self.upperbounds.push(result[s * (i + 1)]);
			}
			done();
		})
	}

	this.getPartitionIndex = function(data) {
		for (var i = 0; i < this.upperbounds.length; i++)
			if (data < this.upperbounds[i]) break;
		return i;
	}
}

function HashPartitioner(numPartitions) {
	this.numPartitions = numPartitions;

	this.hash = function (o) {
		var i, h = 0, s = o.toString(), len = s.length;
		for (i = 0; i < len; i++) {
			h = ((h << 5) - h) + s.charCodeAt(i);
			h = h & h;	// convert to 32 bit integer
		}
		return Math.abs(h);
	}

	this.getPartitionIndex = function(data) {
		return this.hash(data) % this.numPartitions;
	}
}

module.exports = {
	Dataset: Dataset, 
	Partition: Partition, 
	Parallelize: Parallelize, 
	TextFile: TextFile, 
	Stream: Stream,
	RangePartitioner: RangePartitioner,
	HashPartitioner: HashPartitioner
};