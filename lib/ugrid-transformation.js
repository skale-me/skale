'use strict';

var fs = require('fs');
var Connection = require('ssh2');

var ml = require('./ugrid-ml.js');
var trace = require('line-trace');
var Lines = require('./lines.js');
var sizeOf = require('../utils/sizeof.js');
var readSplit = require('../utils/readsplit.js').readSplit;

var global = {require: require};

function MemoryManager() {
	var Kb = 1024, Mb = 1024 * Kb, Gb = 1024 * Mb;
	var MAX_MEMORY = 1.0 * Gb;	// To bet set as max_old_space_size value when launching node
	var maxStorageMemory = MAX_MEMORY * 0.4;
	var maxShuffleMemory = MAX_MEMORY * 0.2;
	var maxCollectMemory = MAX_MEMORY * 0.2;

	this.storageMemory = 0;
	this.shuffleMemory = 0;
	this.collectMemory = 0;

	this.storageFull = function() {return (this.storageMemory > maxStorageMemory);}
	this.shuffleFull = function() {return (this.shuffleMemory > maxShuffleMemory);}
	this.collectFull = function() {return (this.collectMemory > maxCollectMemory);}	
}

var mm = new MemoryManager();

module.exports.UgridJob = function(grid, app, param) {
	this.id = param.jobId;
	this.sendResultSemaphore = 0;
	this.da = {};

	var stream = grid.createWriteStream(this.id, app.master_uuid), stageBoundaries = [], stages = [], self = this;

	// Step 1: Spawn DAs from top to bottom
	for (var i = 0; i < param.node.length; i++) {
		if (app.da[param.node[i].id] == undefined) {
			switch (param.node[i].type) {
			case 'Parallelize': this.da[param.node[i].id] = new Parallelize(grid, app, this, param.node[i]); break;
			case 'RandomSVMData': this.da[param.node[i].id] = new RandomSVMData(grid, app, this, param.node[i]); break;
			case 'TextFile': this.da[param.node[i].id] = new TextFile(grid, app, this, param.node[i]); break;
			case 'Stream': this.da[param.node[i].id] = new StreamDA(grid, app, this, param.node[i]); break;
			case 'Map': this.da[param.node[i].id] = new Map(grid, app, this, param.node[i]); break;
			case 'Union': this.da[param.node[i].id] = new Union(grid, app, this, param.node[i]); break;
			case 'Filter': this.da[param.node[i].id] = new Filter(grid, app, this, param.node[i]); break;
			case 'FlatMap': this.da[param.node[i].id] = new FlatMap(grid, app, this, param.node[i]); break;
			case 'FlatMapValues': this.da[param.node[i].id] = new FlatMapValues(grid, app, this, param.node[i]); break;
			case 'MapValues': this.da[param.node[i].id] = new MapValues(grid, app, this, param.node[i]); break;
			case 'Sample': this.da[param.node[i].id] = new Sample(grid, app, this, param.node[i]); break;
			case 'Distinct': this.da[param.node[i].id] = new Distinct(grid, app, this, param.node[i]); break;
			case 'Cartesian': this.da[param.node[i].id] = new Cartesian(grid, app, this, param.node[i]); break;
			case 'Intersection': this.da[param.node[i].id] = new Intersect(grid, app, this, param.node[i]); break;
			case 'Subtract': this.da[param.node[i].id] = new Subtract(grid, app, this, param.node[i]); break;
			case 'ReduceByKey': this.da[param.node[i].id] = new ReduceByKey(grid, app, this, param.node[i]); break;
			case 'GroupByKey': this.da[param.node[i].id] = new GroupByKey(grid, app, this, param.node[i]); break;
			case 'CoGroup': this.da[param.node[i].id] = new CoGroup(grid, app, this, param.node[i]); break;
			case 'Join': this.da[param.node[i].id] = new Join(grid, app, this, param.node[i]); break;
			case 'LeftOuterJoin': this.da[param.node[i].id] = new LeftOuterJoin(grid, app, this, param.node[i]); break;
			case 'RightOuterJoin': this.da[param.node[i].id] = new RightOuterJoin(grid, app, this, param.node[i]); break;
			default: console.error('Not yet implemented');
			}
		} else this.da[param.node[i].id] = app.da[param.node[i].id];
		if (this.da[param.node[i].id].shuffling) stageBoundaries.unshift(this.da[param.node[i].id]);
	}

	// Step 2: Find stages that needs to be executed, NB a stage under an inMemory DArray does not need to be recomputed
	for (var i = 0; i < stageBoundaries.length; i++) {
		if (stageBoundaries[i].partitions) break;
		stages.unshift(stageBoundaries[i]);
	}

	this.sendResult = function(result) {
		if (result != undefined) self.result = result;
		if ((app.wid != 0) && (++self.sendResultSemaphore != 2)) return;
		// if collect notify master that file is ready, else stream to master
		var o = {data: self.result};
		o.stream = (param.action.fun == 'collect');
		stream.write(o);
		// Notify next worker to send its results
		if (app.worker[app.wid + 1]) grid.send(app.worker[app.wid + 1].uuid, {cmd: 'action', jobId: self.id});
	}

	// Method for DAs subject to shuffle
	this.run = function() {
		var root = this.da[param.node[param.node.length - 1].id], s = 0;
		(function nextStage() {
			if (s < stages.length) stages[s++].run(nextStage);
			else root[param.action.fun](self.sendResult, param.action);
		})();
	}
};

function DA(grid, app, job, param) {
	var self = this;

	this.id = param.id;
	this.dependencies = param.dependencies;
	this.persistent = param.persistent;
	this.type = param.type;

	this.ugrid_dir = '/tmp/ugrid/';
	this.context_dir = this.ugrid_dir + app.contextId + '/';
	this.base_dir = this.context_dir + 'da_' + this.id + '/';
	this.worker_dir = this.base_dir + 'worker_' + app.wid + '/';
	this.job_dir = this.worker_dir + 'jobs/';	

	try {fs.mkdirSync(this.ugrid_dir)} catch (e) {};
	try {fs.mkdirSync(this.context_dir)} catch (e) {};
	try {fs.mkdirSync(this.base_dir)} catch (e) {};
	try {fs.mkdirSync(this.worker_dir)} catch (e) {};
	try {fs.mkdirSync(this.job_dir)} catch (e) {};

	this.collect = function(callback, param) {
		this.findPartitions(this);
		var result = '', p = -1;

		(function action() {
			if (++p < self.partitions.length) {
				self.compute(p, function processPartition() {process.nextTick(action)},
					function processElement(context, data) {
						var str = '';
						for (var i = 0; i < data.length; i++)
							str += JSON.stringify(data[i]) + '\n';
						mm.collectMemory += sizeOf(str);
						result += str;
						if (mm.collectFull()) {
							fs.appendFileSync(self.job_dir + job.id, result);
							mm.collectMemory = 0;
							result = '';
						}
					}
				);
			} else {
				fs.appendFileSync(self.job_dir + job.id, result);
				mm.collectMemory = 0;
				result = '';
				callback(self.job_dir + job.id);
			}
		})();
	};

	this.aggregate = function(callback, param) {
		this.findPartitions(this);
		var result = JSON.parse(JSON.stringify(param.args[0])), p = -1;
		var reducer = recompile(param.src);

		(function action() {
			if (++p < self.partitions.length) {
				self.compute(p, function() {process.nextTick(action)}, function (context, data) {
					for (var i = 0; i < data.length; i++) result = reducer(result, data[i]);
				});
			} else callback(result);
		})();
	};

	this.findPartitions = function (n) {
		if (n.partitions == undefined) {
			n.partitions = [];
			for (var i = 0; i < n.dependencies.length; i++) {
				var parentPartitions = self.findPartitions(job.da[n.dependencies[i]]);
				for (var p = 0; p < parentPartitions.length; p++)
					n.partitions.push(new Partition({id: n.dependencies[i], p: p}));	// id is sourcePartitionId
			}
		}
		return n.partitions;
	}

	this.compute = function(p, done, action) {
		var sourceDA = self, sourcePartitionIdx = p, sourcePartition = self.partitions[p];
		var pipeline = action ? [{transform: action, p: sourcePartitionIdx}] : [];
		// comment eviter les race conditions sur les partitions
		// avoir un champs being_written pour eviter l'ecriture multiple
		// avoir un champs available pour indiquer que l'on peut lire la donnée
		// ici il faudrait prendre en compte si la partition est en cours d'écriture ou non e
		while ((sourcePartition.data == undefined) && sourcePartition.source) {
			if (sourceDA.persistent && !sourceDA.shuffling && !mm.storageFull()) {
				sourcePartition.data = [];
				pipeline.unshift({transform: sourcePartition.save, p: sourcePartitionIdx});
			}

			if (sourceDA.transform)			// if dataset as a transformation
				pipeline.unshift({transform: sourceDA.transform, p: sourcePartitionIdx, sourceId: sourcePartition.source.id});

			sourceDA = job.da[sourcePartition.source.id];
			sourcePartitionIdx = sourcePartition.source.p;
			sourcePartition = sourceDA.partitions[sourcePartitionIdx];
		}

		if (sourcePartition.data) sourcePartition.iterate(pipeline, done);
		else {
			if (sourceDA.persistent  && !mm.storageFull()) {
				sourcePartition.data = [];
				pipeline.unshift({transform: sourcePartition.save, p: sourcePartitionIdx});
			}
			sourceDA.iterate(sourcePartitionIdx, pipeline, done);		// Launch computation
		}
	}

	if (this.persistent) app.da[param.id] = this;
}

function ShuffledDA(grid, app, job, param) {
	DA.call(this, grid, app, job, param);
	var self = this;

	this.P = 1;							// default number of partitions
	this.buffer = {};
	this.nShuffle = 0;
	this.shuffling = true;
	this.nextStage;
	this.shuffle_dir = this.worker_dir + 'shuffle/';
	this.post_shuffle_dir = this.worker_dir + 'postshuffle/';
	this.intermediate_dir = this.worker_dir + 'intermediate/';
	this.tmp_dir = this.worker_dir + 'tmp/';

	this.preShuffleFiles = [];
	this.postShuffleFiles = [];
	this.intermediateFiles = [];

	// Create DA directory files
	try {fs.mkdirSync(this.tmp_dir)} catch (e) {};
	try {fs.mkdirSync(this.intermediate_dir)} catch (e) {};
	try {fs.mkdirSync(this.post_shuffle_dir)} catch (e) {};
	try {fs.mkdirSync(this.shuffle_dir)} catch (e) {};

	// Create shuffle files structure (one per worker)
	for (var i = 0; i < app.worker.length; i++) {
		this.preShuffleFiles[i] = {name: self.shuffle_dir + 'to_worker_' + i, buffer: ''};
		// create empty shuffle files
		try {fs.unlinkSync(this.preShuffleFiles[i].name);} catch (e){;}
		fs.appendFileSync(this.preShuffleFiles[i].name, '');
	}

	this.run = function(nextStage) {
		this.findPartitions(this);		// find partitions
		this.nextStage = nextStage;		// nextStage() will be called elsewhere
		var p = -1;
		(function run() {
			if (++p < self.partitions.length) self.compute(p, function() {process.nextTick(run)});
			else {
				self.preShuffle();
				for (var i = 0; i < app.worker.length; i++)
					if (grid.host.uuid == app.worker[i].uuid) self.shuffle();
					else grid.send(app.worker[i].uuid, {cmd: 'shuffle', jobId: job.id, daId: self.id});
			}
		})();
	}

	this.preShuffle = function() {self.spillToDisk('preshuffle', self.preShuffleFiles);}

	this.postShuffle = function() {
		// create intermediate files structure
		// NB: si on partitionByKey alors le nombre de partition n'est pas connu à l'instanciation du DA
		for (var p = 0; p < self.P; p++) {
			self.intermediateFiles[p] = {name: self.intermediate_dir + 'p_' + p, buffer: ''};
			// Create empty intermediate files
			try {fs.unlinkSync(self.intermediateFiles[p].name);} catch (e){;}
			fs.appendFileSync(self.intermediateFiles[p].name, '');
		}

		// Population des fichiers de partitions à partir des fichiers de shuffles
		function populate(from, to, done) {
			var lines = new Lines();
			fs.createReadStream(self.postShuffleFiles[from]).pipe(lines);		// supprimer l'usage de lines ICI
			lines.on('data', function(line) {self.onShuffleData(JSON.parse(line))});
			lines.on('end', function() {
				if (++from < app.worker.length) populate(from, to, done);
				else {self.spillToDisk('postshuffle', self.intermediateFiles); done();}
			});
		}
		populate(0, app.wid, this.nextStage);
	}

	this.shuffle = function() {
		if (++this.nShuffle < app.worker.length) return;

		function fetchShuffleFile(from, done) {
	        var remote = self.base_dir + '/worker_' + from + '/shuffle/to_worker_' + app.wid;
	 		var local = self.base_dir + '/worker_' + app.wid + '/postshuffle/from_worker_' + from;

			//trace("%s %s", grid.workerHost[from], grid.workerHost[app.wid]);
			if (grid.workerHost[from] == grid.workerHost[app.wid]) {
				self.postShuffleFiles[from] = remote;
	            if (++from < app.worker.length) fetchShuffleFile(from, done)
	            else done();
	            return;
			}

			app.transfer(grid.workerHost[from], remote, local, function (err, res) {
					self.postShuffleFiles[from] = local;
	            if (++from < app.worker.length) fetchShuffleFile(from, done)
	            else done(err, res);
			});
		}

		fetchShuffleFile(0, function() {
			self.partitions = [];
			for (var p = 0; p < self.P; p++)
				self.partitions.push(new Partition(null));
			self.postShuffle();
		});
	};

	this.spillToDisk = function(type, files) {
		// step 1: Prepare file buffers, flush global buffer, reset shuffleMemory
		if (type == 'preshuffle') {
			for (var hash in self.buffer) {
				var cksum = self.buffer[hash][0], input = self.buffer[hash][1];
				files[cksum % files.length].buffer += JSON.stringify([cksum, input]) + '\n';
			}
		} else if (type == 'postshuffle') {
			for (var hash in self.buffer) {
				var cksum = self.buffer[hash][0], input = self.buffer[hash][1];
				files[cksum % files.length].buffer += JSON.stringify(input) + '\n';
			}
		}
		self.buffer = {};
		mm.shuffleMemory = 0;
		// step 2: write files and flush file buffers
		for (var i = 0; i < files.length; i++) {
			fs.appendFileSync(files[i].name, files[i].buffer);
			files[i].buffer = '';
		}
	}

	this.iterate = function(p, pipeline, done) {
		var lines = new Lines();
		try {
			fs.createReadStream(self.intermediateFiles[p].name).pipe(lines);
			lines.on('data', function(line) {self.onIteratorData(pipeline, JSON.parse(line));});
			lines.on('end', function() {self.onIteratorEnd && self.onIteratorEnd(pipeline); done()});
		} catch (e) {done();}
	}
}

function recompile(s) {
	if (s.indexOf('=>') >= 0) return eval(s);	// Support arrow functions
	var args = s.match(/\(([^)]*)/)[1];
	var body = s.replace(/^function\s*[^)]*\)\s*{/, '').replace(/}$/, '');
	return new Function(args, body);
}

function Partition(source, data, key) {
	this.source = source;		// {id: 1234, p: 0}
	this.data = data;
	this.key = key;				// peut-etre utile lors du partition by key
	var self = this;			// when class method is called with a different 'this'

	this.save = function(context, data) {
		if (mm.storageFull()) {				// Il faudrait supprimer du pipeline le save pour les partitions suivantes
			if (self.first == undefined) {
				self.first = true;
				console.log('STORAGE MEMORY IS FULL')
				console.log(process.memoryUsage());
				// ici il faudrait retrancher de la storage memory la partition qui vient d'etre supprimée
			}
			self.data = undefined;	// delete partition as it does not fit in storage memory
			return data;
		}
		var size = sizeOf(data);
		mm.storageMemory += size;
		// If storage ok
		for (var i = 0; i < data.length; i++) self.data.push(data[i]);
		return data;
	}

	this.iterate = function(pipeline, done) {
		var buffer;
		for (var i = 0; i < self.data.length; i++) {
			buffer = [self.data[i]];
			for (var t = 0; t < pipeline.length; t++)
				buffer = pipeline[t].transform(pipeline[t], buffer);
		}
		done();
	}
}

// ================================================================================================ //
// Sources
// ================================================================================================ //
function StreamDA(grid, app, job, param) {
	DA.call(this, grid, app, job, param);

 	var self = this;
 	var nPartitions = 1;
 	var streamIdx = param.attr.streamId;

	this.partitions = [];
	for (var p = 0; p < nPartitions; p++)
		this.partitions.push(new Partition(null));

	var onData = function(data, done) {
		var buffer = [data];
		for (var t = 0; t < self.partitionPipeline.length; t++)
			buffer = self.partitionPipeline[t].transform(self.partitionPipeline[t], buffer);
		done();
	}

	var onBlock = function(done) {
	// 	app.dones[streamIdx] = done;
	// 	self.flush(false);
	}

	var onEnd = function(done) {
		self.partitionDone();				// on débloque le done de la partition
		done();								// le done doit etre stocké et traité après traitement complet de la réponse
	}

	grid.on(streamIdx, onData);
	grid.on(streamIdx + '.block', onBlock);
	grid.on(streamIdx + '.end', onEnd);

	this.iterate = function(p, pipeline, done) {
		grid.send(app.master_uuid, {cmd: 'startStream', streamid: streamIdx, jobid: job.id});
		self.partitionDone = done;
		self.partitionPipeline = pipeline;
	};
};

function Parallelize(grid, app, job, param) {
	DA.call(this, grid, app, job, param);

	this.partitions = [];
	var partitions = param.split || [];
	for (var p = 0; p < partitions.length; p++)
		this.partitions.push(new Partition(null, partitions[p]));
}

function RandomSVMData(grid, app, job, param) {
	DA.call(this, grid, app, job, param);

	var D = param.attr.D;
	var partitions = param.split || [];

	this.partitions = [];
	for (var p = 0; p < partitions.length; p++)
		this.partitions.push(new Partition(null));

	this.iterate = function(p, pipeline, done) {
		var rng = new ml.Random(partitions[p].seed), buffer;

		for (var i = 0; i < partitions[p].n; i++) {
			buffer = [ml.randomSVMLine(rng, D)];
			for (var t = 0; t < pipeline.length; t++)
				buffer = pipeline[t].transform(pipeline[t], buffer);
		}
		done();
	}
}

function TextFile(grid, app, job, param) {
	DA.call(this, grid, app, job, param);
	var self = this, blocks = [];

	// Create one partition per split handled by given worker
	var split = param.split;
	this.partitions = [];
	for (var i = 0; i < split.length; i++) {
		if (split[i].wid != app.wid) continue;
		blocks.push(split[i]);
		this.partitions.push(new Partition(null));
	}

	this.iterate = function(p, pipeline, done) {
		var buffer;

		function processLine(line) {
			if (!line) return;	// skip empty lines
			buffer = [line];
			for (var t = 0; t < pipeline.length; t++)
				buffer = pipeline[t].transform(pipeline[t], buffer);
		}

		readSplit(split, blocks[p].index, processLine, done);
	}
}
// ================================================================================================ //
// Transformations
// ================================================================================================ //
function Map(grid, app, job, param) {
	DA.call(this, grid, app, job, param);
	var mapper = recompile(param.attr.mapper);

	this.transform = function(context, data) {
		var tmp = [];
		for (var i = 0; i < data.length; i++)
			tmp[i] = mapper(data[i], param.attr.args, global);
		return tmp;
	}
}

function FlatMap(grid, app, job, param) {
	DA.call(this, grid, app, job, param);
	var mapper = recompile(param.attr.mapper);

	this.transform = function(context, data) {
		var tmp = [];
		for (var i = 0; i < data.length; i++) tmp = tmp.concat(mapper(data[i], param.attr.args, global));
		return tmp;
	}
}

function MapValues(grid, app, job, param) {
	DA.call(this, grid, app, job, param);
	var mapper = recompile(param.attr.mapper);

	this.transform = function(context, data) {
		var tmp = [];
		for (var i = 0; i < data.length; i++) tmp[i] = [data[i][0], mapper(data[i][1], param.attr.args, global)];
		return tmp;
	}
}

function FlatMapValues(grid, app, job, param) {
	DA.call(this, grid, app, job, param);
	var mapper = recompile(param.attr.mapper);

	this.transform = function(context, data) {
		var tmp = [];
		for (var i = 0; i < data.length; i++) {
			var t0 = mapper(data[i][1], param.attr.args, global);
			tmp = tmp.concat(t0.map(function(e) {return [data[i][0], e];}));
		}
		return tmp;
	}
}

function Filter(grid, app, job, param) {
	DA.call(this, grid, app, job, param);
	var filter = recompile(param.attr.filter);

	this.transform = function(context, data) {
		var tmp = [];
		for (var i = 0; i < data.length; i++)
			if (filter(data[i], param.attr.args, global)) tmp.push(data[i]);
		return tmp;
	}
}

function Union(grid, app, job, param) {
	DA.call(this, grid, app, job, param);
}

function Sample(grid, app, job, param) {
	DA.call(this, grid, app, job, param);
	var withReplacement = param.attr.withReplacement;
	var frac = param.attr.frac;
	var seed = param.attr.seed;
	var rng = withReplacement ? new ml.Poisson(frac, seed) : new ml.Random(seed);

	this.transform = function(context, data) {
		var tmp = [];
		if (withReplacement) {
			for (var i = 0; i < data.length; i++)
				for (var j = 0; j < rng.sample(); j++) tmp.push(data[i]);
		} else {
			for (var i = 0; i < data.length; i++)
				if (rng.nextDouble() < frac) tmp[i] = data[i];
		}
		return tmp;
	}
}

function Distinct(grid, app, job, param) {
	ShuffledDA.call(this, grid, app, job, param);
	var self = this;

	this.transform = function(context, data) {
		for (var i = 0; i < data.length; i++) {
			var str = JSON.stringify(data[i]);
			if (self.buffer[str] == undefined) {
				self.buffer[str] = [ml.cksum(str), [data[i]]];
				mm.shuffleMemory += sizeOf(self.buffer[str]) + sizeOf(str);
			}
			if (mm.shuffleFull()) self.spillToDisk('preshuffle', self.preShuffleFiles);
		}
	};

	this.onShuffleData = function(data) {
		var cksum = data[0], input = data[1][0], str = JSON.stringify(input);
		if (self.buffer[str] == undefined) {
			self.buffer[str] = [cksum, [input]];
			mm.shuffleMemory += sizeOf(self.buffer[str]) + sizeOf(str);
		}
		if (mm.shuffleFull()) self.spillToDisk('postshuffle', self.intermediateFiles);
	}

	self.index = [];		// à remplacer par self.buffer

	this.onIteratorData = function(pipeline, data) {
		var input = data[0], str = JSON.stringify(input), value = data[1];
		if (self.index.indexOf(str) != -1) return;
		self.index.push(str);
		var buffer = [input];
		for (var t = 0; t < pipeline.length; t++)
			buffer = pipeline[t].transform(pipeline[t], buffer);
	}
};

function Cartesian(grid, app, job, param) {
	ShuffledDA.call(this, grid, app, job, param);
	var self = this;

	this.secondaryBuffer = {};
	this.tmpFile = {name: self.tmp_dir + 'left', buffer: ''};
	try {fs.unlinkSync(self.tmpFile.name);} catch (e){;}
	fs.appendFileSync(self.tmpFile.name, '');

	this.transform = function(context, data) {
		var sid = (context.sourceId == self.dependencies[0]) ? 0 : 1;

		if (sid == 0) {			// premier dataset
			for (var i = 0; i < data.length; i++) {
				var str = JSON.stringify(data[i]);
				if (self.secondaryBuffer[str] == undefined) {
					self.secondaryBuffer[str] = [ml.cksum(str), [data[i]]];
					mm.shuffleMemory += sizeOf(self.secondaryBuffer[str]) + sizeOf(str);
					if (mm.shuffleFull()) self.spillSecondaryBufferToDisk(self.tmpFile);
				} else {
					self.secondaryBuffer[str][1].push(data[i]);
					mm.shuffleMemory += sizeOf(str);
					if (mm.shuffleFull()) self.spillSecondaryBufferToDisk(self.tmpFile);
				}
			}
		} else {				// second dataset
			for (var i = 0; i < data.length; i++) {
				var str = JSON.stringify(data[i]);
				if (self.buffer[str] == undefined) {
					self.buffer[str] = [ml.cksum(str), [data[i]]];
					mm.shuffleMemory += sizeOf(self.buffer[str]) + sizeOf(str);
					if (mm.shuffleFull()) self.spillToDisk('preshuffle', self.preShuffleFiles);
				} else {
					self.buffer[str][1].push(data[i]);
					mm.shuffleMemory += sizeOf(str);
					if (mm.shuffleFull()) self.spillToDisk('preshuffle', self.preShuffleFiles);
				}
			}
		}
	};

	this.spillToDisk = function(type, files) {
		// step 1: Prepare file buffers, flush global buffer, reset shuffleMemory
		if (type == 'preshuffle') {
			for (var hash in self.buffer) {
				var input = self.buffer[hash][1];
				for (var j = 0; j < input.length; j++) {
					for (var i = 0; i < files.length; i++) {
						files[i].buffer += JSON.stringify([cksum, [input[j]]]) + '\n';
					}
				}
			}
		} else if (type == 'postshuffle') {
			for (var hash in self.buffer) {
				var cksum = self.buffer[hash][0], input = self.buffer[hash][1];
				for (var j = 0; j < input.length; j++)
					files[cksum % files.length].buffer += JSON.stringify([input[j]]) + '\n';
			}
		}
		self.buffer = {};
		mm.shuffleMemory = 0;
		// step 2: write files and flush file buffers
		for (var i = 0; i < files.length; i++) {
			fs.appendFileSync(files[i].name, files[i].buffer);
			files[i].buffer = '';
		}
	}

	this.spillSecondaryBufferToDisk = function(file) {
		// step 1: Prepare file buffers, flush global buffer, reset shuffleMemory
		for (var hash in self.secondaryBuffer) {
			var cksum = self.secondaryBuffer[hash][0], input = self.secondaryBuffer[hash][1];
			for (var j = 0; j < input.length; j++) {
				file.buffer += JSON.stringify([cksum, [input[j]]]) + '\n';
			}
		}
		self.secondaryBuffer = {};
		mm.shuffleMemory = 0;							// ERROR: ici il faut retrancher uniquement la portion du secondary buffer
		// step 2: write files and flush file buffers
		fs.appendFileSync(file.name, file.buffer);
		file.buffer = '';
	}

	this.preShuffle = function() {
		self.spillToDisk('preshuffle', self.preShuffleFiles);
		self.spillSecondaryBufferToDisk(self.tmpFile);
	}

	// lors du shuffle pour chaque entrée on boucle sur le fichier de gauche
	// et in génère les paires de valeur
	this.postShuffle = function() {
		for (var p = 0; p < self.P; p++) {
			self.intermediateFiles[p] = {name: self.intermediate_dir + 'p_' + p, buffer: ''};
			// Create empty intermediate files
			try {fs.unlinkSync(self.intermediateFiles[p].name);} catch (e){;}
			fs.appendFileSync(self.intermediateFiles[p].name, '');
		}

		// Population des fichiers de partitions à partir des fichiers de shuffles
		function populate(from, done) {
			var lines = new Lines();
			fs.createReadStream(self.postShuffleFiles[from]).pipe(lines);		// supprimer l'usage de lines ICI
			var lcount = 0, rcount = 0, rightEnded = false, leftEnded = false;

			lines.on('data', function(line) {
				var right_data = JSON.parse(line);
				var cksum = right_data[0];
				var left_lines = new Lines();
				rcount++;
				leftEnded = false;
				fs.createReadStream(self.tmpFile.name).pipe(left_lines);
				left_lines.on('data', function(line) {
					var left_data = JSON.parse(line);
					var tmp = [left_data[1][0], right_data[1][0]];
					var str = JSON.stringify(tmp);
					if (self.buffer[str] == undefined) {
						self.buffer[str] = [cksum, [tmp]];
						mm.shuffleMemory += sizeOf(self.buffer[str]) + sizeOf(str);
					} else {
						self.buffer[str][1].push(tmp);
						mm.shuffleMemory += sizeOf(str);
					}
					if (mm.shuffleFull()) self.spillToDisk('postshuffle', self.intermediateFiles);
				});
				left_lines.on('end', function () {
					leftEnded = true;
					if (!rightEnded || ++lcount < rcount) return;
					trace('next')
					if (++from < app.worker.length) populate(from, done);
					else {self.spillToDisk('postshuffle', self.intermediateFiles); done();}
				});
			});
			lines.on('end', function() {
				rightEnded = true;
			});
		}
		populate(0, this.nextStage);
	}

	this.onIteratorData = function(pipeline, data) {
		var input = data[0];
		var buffer = [input];
		for (var t = 0; t < pipeline.length; t++)
			buffer = pipeline[t].transform(pipeline[t], buffer);
	}
}

function Intersect(grid, app, job, param) {
	ShuffledDA.call(this, grid, app, job, param);
	var self = this;

	this.transform = function(context, data) {
		var sid = (context.sourceId == self.dependencies[0]) ? 0 : 1;
		for (var i = 0; i < data.length; i++) {
			var str = JSON.stringify(data[i]);
			if (self.buffer[str] == undefined) {
				self.buffer[str] = [ml.cksum(str), [data[i], [0, 0]]];
				mm.shuffleMemory += sizeOf(self.buffer[str]) + sizeOf(str);
			}
			self.buffer[str][1][1][sid]++;
			if (mm.shuffleFull()) self.spillToDisk('preshuffle', self.preShuffleFiles);
		}
	};

	this.onShuffleData = function(data) {
		var cksum = data[0], input = data[1][0], str = JSON.stringify(input), value = data[1][1];
		if (self.buffer[str] == undefined) {
			self.buffer[str] = [cksum, [input, [0, 0]]];
			mm.shuffleMemory += sizeOf(self.buffer[str]) + sizeOf(str);
		}
		self.buffer[str][1][1][0] += value[0];
		self.buffer[str][1][1][1] += value[1];
		if (mm.shuffleFull()) self.spillToDisk('postshuffle', self.intermediateFiles);
	}

	this.onIteratorData = function(pipeline, data) {
		var input = data[0], str = JSON.stringify(input), value = data[1];
		if (self.buffer[str] == undefined) self.buffer[str] = [input, [0, 0]];
		self.buffer[str][1][0] += value[0];
		self.buffer[str][1][1] += value[1];
	}

	this.onIteratorEnd = function(pipeline) {
		for (var str in self.buffer) {
			if (self.buffer[str][1][0] && self.buffer[str][1][1]) {
				var buffer = [self.buffer[str][0]];
				for (var t = 0; t < pipeline.length; t++)
					buffer = pipeline[t].transform(pipeline[t], buffer);
			}
		}
	}
}

function Subtract(grid, app, job, param) {
	Intersect.call(this, grid, app, job, param);
	var self = this;

	this.onIteratorEnd = function(pipeline) {
		for (var str in self.buffer) {
			if (self.buffer[str][1][0] && !self.buffer[str][1][1]) {
				for (var i = 0; i < self.buffer[str][1][0]; i++) {
					var buffer = [self.buffer[str][0]];
					for (var t = 0; t < pipeline.length; t++)
						buffer = pipeline[t].transform(pipeline[t], buffer);
				}
			}
		}
	}
}

// ------------------------------------------------------------------------ //
// Key-Value transformations
// ------------------------------------------------------------------------ //
function CombineByKey(grid, app, job, param) {
	ShuffledDA.call(this, grid, app, job, param);
	var self = this;

	this.transform = function(context, data) {
		for (var i = 0; i < data.length; i++) {
			var key = data[i][0], value = data[i][1], str = JSON.stringify(key);
			if (self.buffer[str] == undefined)
				self.buffer[str] = [ml.cksum(str), [key, JSON.parse(JSON.stringify(self.init))]];
			mm.shuffleMemory -= sizeOf(self.buffer[str][1][1]);
			self.buffer[str][1][1] = self.reducer(self.buffer[str][1][1], value, self.reducerArgs, global);
			mm.shuffleMemory += sizeOf(self.buffer[str][1][1]);
			if (mm.shuffleFull()) self.spillToDisk('preshuffle', self.preShuffleFiles);
		}
	};

	this.onShuffleData = function(data) {
		var cksum = data[0], input = data[1], key = input[0], value = input[1];
		if (self.buffer[key] == undefined) self.buffer[key] = [cksum, input];
		else {
			mm.shuffleMemory -= sizeOf(self.buffer[key][1][1]);
			self.buffer[key][1][1] = self.combiner(self.buffer[key][1][1], value, self.combinerArgs, global);
			mm.shuffleMemory += sizeOf(self.buffer[key][1][1]);
		}
		if (mm.shuffleFull()) self.spillToDisk('postshuffle', self.intermediateFiles);
	}

	this.onIteratorData = function(pipeline, data) {
		var key = data[0], value = data[1];
		if (self.buffer[key] == undefined) self.buffer[key] = data;
		else self.buffer[str][1] = self.combiner(self.buffer[str][1], value, self.combinerArgs, global);
	}

	this.onIteratorEnd = function(pipeline) {
		for (var key in self.buffer) {
			var buffer = [self.buffer[key]];
			for (var t = 0; t < pipeline.length; t++)
				buffer = pipeline[t].transform(pipeline[t], buffer);
		}
	}
}

function ReduceByKey(grid, app, job, param) {
	CombineByKey.call(this, grid, app, job, param);

	// this.init = param.args[0];
	// this.reducerArgs = param.args[1];
	// this.combinerArgs = param.args[1];
	// this.reducer = this.combiner = recompile(param.src);
	this.init = param.attr.init;
	this.reducerArgs = param.attr.args;
	this.combinerArgs = param.attr.args;
	this.reducer = this.combiner = recompile(param.attr.reducer);
}

function GroupByKey(grid, app, job, param) {
	CombineByKey.call(this, grid, app, job, param);

	this.init = [];
	this.reducer = function(a, b) {
		a.push(b);
		return a;
	}
	this.combiner = function(a, b) {
		return a.concat(b);
	}
}

function CoGroup(grid, app, job, param) {
	CombineByKey.call(this, grid, app, job, param);
	var self = this;

	this.init = [[], []];
	this.combiner = function(a, b) {
		a[0] = a[0].concat(b[0]);
		a[1] = a[1].concat(b[1]);
		return a;
	}

	this.transform = function(context, data) {
		var sid = (context.sourceId == self.dependencies[0]) ? 0 : 1;
		for (var i = 0; i < data.length; i++) {
			var key = data[i][0], value = data[i][1], str = JSON.stringify(key);
			if (self.buffer[str] == undefined)
				self.buffer[str] = [ml.cksum(str), [JSON.parse(key), JSON.parse(JSON.stringify(self.init))]];
			self.buffer[str][1][1][sid].push(value);
			try {mm.shuffleMemory += sizeOf(value)}
			catch(err) {console.log(data)}
			if (mm.shuffleFull()) self.spillToDisk('preshuffle', self.preShuffleFiles);
		}
	}
}

function Join(grid, app, job, param) {
	CoGroup.call(this, grid, app, job, param);
	var self = this;

	this.onIteratorEnd = function(pipeline) {
		for (var key in self.buffer) {
			var acc = self.buffer[key][1];
			if (acc[0].length && acc[1].length) {
				for (var i = 0; i < acc[0].length; i++) {
					for (var j = 0; j < acc[1].length; j++) {
						var buffer = [[JSON.parse(key), [acc[0][i], acc[1][j]]]];
						for (var t = 0; t < pipeline.length; t++)
							buffer = pipeline[t].transform(pipeline[t], buffer);
					}
				}
			}
		}
	}
}

function LeftOuterJoin(grid, app, job, param) {
	CoGroup.call(this, grid, app, job, param);
	var self = this;

	this.onIteratorEnd = function(pipeline) {
		for (var key in self.buffer) {
			var acc = self.buffer[key][1];
			for (var i = 0; i < acc[0].length; i++) {
				if (acc[1].length) {
					for (var j = 0; j < acc[1].length; j++) {
						var buffer = [[JSON.parse(key), [acc[0][i], acc[1][j]]]];
						for (var t = 0; t < pipeline.length; t++)
							buffer = pipeline[t].transform(pipeline[t], buffer);
					}
				} else {
					var buffer = [[JSON.parse(key), [acc[0][i], null]]];
					for (var t = 0; t < pipeline.length; t++)
						buffer = pipeline[t].transform(pipeline[t], buffer);
				}
			}
		}
	}
}

function RightOuterJoin(grid, app, job, param) {
	CoGroup.call(this, grid, app, job, param);
	var self = this;

	this.onIteratorEnd = function(pipeline) {
		for (var key in self.buffer) {
			var acc = self.buffer[key][1];
			for (var j = 0; j < acc[1].length; j++) {
				if (acc[0].length) {
					for (var i = 0; i < acc[0].length; i++) {
						var buffer = [[JSON.parse(key), [acc[0][i], acc[1][j]]]];
						for (var t = 0; t < pipeline.length; t++)
							buffer = pipeline[t].transform(pipeline[t], buffer);
					}
				} else {
					var buffer = [[JSON.parse(key), [null, acc[1][j]]]];
					for (var t = 0; t < pipeline.length; t++)
						buffer = pipeline[t].transform(pipeline[t], buffer);
				}
			}
		}
	}
}
