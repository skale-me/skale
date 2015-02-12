'use strict';

var thunkify = require('thunkify');
var UgridTask = require('./ugrid-task.js');
var ml = require('./ugrid-ml.js');

function UgridArray(grid, worker, child, transformType, transform, args) {
	this.id = Math.round(Math.random() * 1e9);			// Unique array id
	this.inMemory = false;
	this.child = child;
	this.persistent = false;
	this.visits = 0;
	this.transform = transform;
	this.transformType = transformType;
	this.args = args;

	for (var i = 0; i < child.length; i++)
		child[i].anc = this;

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

	this.getArgs = function() {
		return {args: args};
	}

	this.persist = function () {
		this.persistent = true;
		return this;
	};

	this.pipelineSource2 = function () {
		if (this.inMemory) return '';
		var tmp = this.pipelineSource ? this.pipelineSource() : '';
		if (this.persistent && (this.transformType == 'narrow')) {
			tmp += 'if (RAM[' + this.id + '] == undefined) RAM[' + this.id + '] = {};\n';
			tmp += 'if (RAM[' + this.id + '][p] == undefined) RAM[' + this.id + '][p] = [];\n';
			tmp += '\t\tRAM[' + this.id + '][p].push(tmp);\n';
		}
		return tmp;
	}

	this.inputFromRam = function () {
		function fromRam(id, sid) {
			var input = RAM[id];
			for (var p in input) {
				for (var i = 0; i < input[p].length; i++) {
					var tmp = input[p][i];
					"PIPELINE_HERE"
				}
			}
			stage_cnt[sid]++;
		}
		return fromRam.toString() + '; fromRam(' + this.id + ', ' + this.stageIdx + ');'		
	}

	this.inputFromStageRam = function () {
		function fromStageRam(sid) {
			var input = STAGE_RAM;
			for (var p in input) {
				for (var i = 0; i < input[p].length; i++) {
					var tmp = input[p][i];
					"PIPELINE_HERE"
				}
			}
			stage_cnt[sid]++;
		}
		return fromStageRam.toString() + '; fromStageRam(' + this.stageIdx + ');'
	}

	this.map = function (mapper, mapperArgs) {
		var args = mapperArgs || [];
		var a = new UgridArray(grid, worker, [this], 'narrow', 'map', args);
		a.pipelineSource = function () {
			var s = '';
			for (var i = 0; i < this.args.length; i++)
				s += ', node[' + this.num + '].args[' + i + ']';
			return 'tmp = transform[' + this.num + '](tmp' + s + ');';
		}
		a.transformSource = function () {
			return 'transform[' + this.num + '] = ' + mapper.toString() + ';';
		}
		return a;
	};

	this.filter = function (filter, filterArgs) {
		var args = filterArgs || [];
		var a = new UgridArray(grid, worker, [this], 'narrow', 'filter', args);
		a.pipelineSource = function () {
			var s = '';
			for (var i = 0; i < this.args.length; i++)
				s += ', node[' + this.num + '].args[' + i + ']';
			return 'if (!transform[' + this.num + '](tmp' + s + ')) continue;';
		}
		a.transformSource = function () {
			return 'transform[' + this.num + '] = ' + filter.toString() + ';';
		}
		return a;
	};

	// il faut généraliser la gestion de l'indice de l'entrée du grid array
	// car un flatMap provoque une expension, si on a plusieurs flatmap
	// consecutifs la variable f du flatmap sera ecrasée, il faut donc
	// la stocker dans un espace dédié
	this.flatMap = function (mapper, mapperArgs) {
		// throw 'this.flatMap not yet implemented';
		var args = mapperArgs || [];
		var a = new UgridArray(grid, worker, [this], 'narrow', 'flatMap', args);
		a.pipelineSource = function () {
			var s = '';
			for (var i = 0; i < this.args.length; i++)
				s += ', node[' + this.num + '].args[' + i + ']';
			return ' var flat_tmp = transform[' + this.num + '](tmp' + s + ');' +
				'for (var f = 0; f < flat_tmp.length; f++) {' +
					'var saved_i = i;' +
					'i = i * flat_tmp.length + f;' +
					'var tmp = flat_tmp[f];' +
					'"PIPELINE_HERE"' +
					'i = saved_i;' +
				'}';
		}
		a.transformSource = function () {
			return 'transform[' + this.num + '] = ' + mapper.toString() + ';';
		}		
		return a;
	};

	this.union = function (withArray) {
		if (withArray.id == this.id) return this;
		return new UgridArray(grid, worker, [this, withArray], 'narrow', 'union', [withArray.id]);
	};

	this.reduceByKey = function (key, reducer, initVal) {
		var a = new UgridArray(grid, worker, [this], 'wide', 'reduceByKey', [key, initVal]);
		a.pipelineSource = function () {
			var key = 'node[' + this.num + '].args[0]';
			var tmp = 'if (tmp[' + key + '] == undefined) throw "key unknown by worker";';
			tmp += 'if (res[tmp[' + key + ']] == undefined)' +
				'res[tmp[' + key + ']] = [JSON.parse(JSON.stringify(node[' + this.num + '].args[1]))];';
			tmp += 'res[tmp[' + key + ']] = [transform[' + this.num + '](res[tmp[' + key + ']][0], tmp)];';
			return tmp;
		}
		a.transformSource = function () {
			return 'transform[' + this.num + '] = ' + reducer.toString() + ';';
		}
		a.shuffleSource = function () {
			var shuffle = function () {
				console.log('Received shuffle');
				nShuffle++;
				var data = msg.data.args;
				// Reduce intermediates results (to be generated programmatically)
				for (var p in data) {
					if (!STAGE_RAM[p] || !data[p]) continue;
					for (var i = 0; i < STAGE_RAM[p][0].acc.length; i++)
						STAGE_RAM[p][0].acc[i] += data[p][0].acc[i];
					STAGE_RAM[p][0].sum += data[p][0].sum;
				}

				grid.reply(msg, null, 'Shuffle done by worker ' + msg.id);

				if (nShuffle == worker.length) {
					nShuffle = 0;
					runStage(++stageIdx);
				}
			}
			return 'shuffle[' + this.num + '] = ' + shuffle.toString() + ';';	
		}
		return a;
	};

	this.reduce_cb = function (reducer, aInit, callback) {
		UgridTask.buildTask(grid, worker, this, {
			fun: 'reduce',
			args: [reducer.toString(), aInit],
			init: 'var reducer = ' + reducer.toString() + ';var res = action.args[1];',
			run: 'res = reducer(res, tmp);'
		}, function (task) {
			var nAnswer = 0, result = aInit;
			sendTask(task, function (err, res) {
				result = reducer(result, res);
				if (++nAnswer == worker.length)
					callback(null, result);
			});
		});
	};

	this.count_cb = function (callback) {
		UgridTask.buildTask(grid, worker, this, {
			fun: 'count',
			args: [],
			init: 'var res = 0;\n',
			run: 'res++;\n'
		}, function (task) {
			var nAnswer = 0, result = 0;
			sendTask(task, function(err, res) {
				result += res;
				if (++nAnswer == worker.length)
					callback(null, result);
			});
		});
	};

	this.collect_cb = function (callback) {
		var self = this;
		UgridTask.buildTask(grid, worker, this, {
			fun: 'collect',
			args: [],
			init: 'var res = {};\n',
			run: 'if (res[p] == undefined) res[p] = []; res[p].push({idx: i, value: tmp});\n'
		}, function (task) {
			var nAnswer = 0, tmp = {};			
			sendTask(task, function (err, res) {
				for (var p in res) tmp[p] = res[p];
				if (++nAnswer == worker.length) {
					var result = [];
					var partitionNames = Object.keys(tmp);
					var nPart = partitionNames.length;
					for (p = 0; p < nPart; p++) {
						for (var i = 0; i < tmp[partitionNames[p]].length; i++) {
							tmp[partitionNames[p]][i].idx = tmp[partitionNames[p]][i].idx * nPart + p;
							result[tmp[partitionNames[p]][i].idx] = tmp[partitionNames[p]][i].value;
						}
					}
					// remove undefined entries and call result callback
					callback(null, result.filter(function (n) {if (n !== undefined) return true;}));
				}
			});			
		});
	};

	// this.collect_cb = function (callback) {
	// 	var self = this;
	// 	UgridTask.buildTask(grid, worker, this, {
	// 		fun: 'collect',
	// 		args: [],
	// 		init: 'var res = {};\n',
	// 		run: 'if (res[p] == undefined) res[p] = []; res[p].push({idx: i, value: tmp});\n'
	// 	}, function (task) {
	// 		var nAnswer = 0, tmp = {};			
	// 		sendTask(task, function (err, res) {
	// 			for (var p in res) tmp[p] = res[p];
	// 			if (++nAnswer == worker.length) {
	// 				// build dataset 
	// 				var sub = {};
	// 				for (var i in self.oldestChild)
	// 					sub[self.oldestChild[i]] = {};					
	// 				for (var p in tmp) {
	// 				}
	// 				var nParts = {};
	// 				for (var i = 0; i < nParts.length; i++)
	// 					nParts[i] = 0;
	// 				// build partitions
	// 				for (var p in tmp) {
	// 					var t0 = p.split('.');
	// 					if (nParts[t0[0]] == undefined)
	// 						nParts[t0[0]] = 0;
	// 					if (sub[t0[0]][t0[1]] == undefined) {
	// 						sub[t0[0]][t0[1]] = [];
	// 						nParts[t0[0]]++;
	// 					}
	// 					for (var i = 0; i < tmp[p].length; i++) {
	// 						sub[t0[0]][t0[1]][tmp[p][i].idx] = tmp[p][i].value;
	// 					}
	// 				}
	// 				var result = [];
	// 				for (var p in self.oldestChild) {
	// 					var tmp_res = [];
	// 					var parts = Object.keys(sub[self.oldestChild[p]]);
	// 					for (var p2 in sub[self.oldestChild[p]]) {
	// 						var t = sub[self.oldestChild[p]][p2];
	// 						for (var i = 0; i < t.length; i++) {
	// 							tmp_res[i * nParts[self.oldestChild[p]] + parts.indexOf(p2)] = t[i];
	// 						}

	// 					}
	// 					result = result.concat(tmp_res);						
	// 				}
	// 				// remove undefined entries and call result callback
	// 				callback(null, result.filter(function (n) {if (n !== undefined) return true;}));
	// 			}
	// 		});			
	// 	});
	// };

	this.sample_cb = function (fraction, seed, callback) {
		function sample(v, frac, seed) {
			var rng = new ml.Random(seed);
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
			init: 'var res = [];\n',
			run: 'res.push(tmp);\n',
			post: sample.toString() + '; res = {N: res.length, data: sample(res, action.args[0], action.args[1])};'
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

	// this.lookup_cb = function(key, callback) {
	// 	throw 'this.lookup not yet implemented'
	// 	var task = UgridTask.buildTask(worker, this, {
	// 		fun: 'lookup',
	// 		args: [key]
	// 	});

	// 	var nAnswer = 0, result = [];
	// 	for (var i = 0; i < worker.length; i++)
	// 		rpc(grid, i, task[i], function(err, res) {
	// 			result = result.concat(res);
	// 			if (++nAnswer == worker.length)
	// 				callback(null, result);
	// 		})
	// }

	this.reduce = thunkify(this.reduce_cb);
	this.count = thunkify(this.count_cb);
	this.collect = thunkify(this.collect_cb);
	this.takeSample = thunkify(this.takeSample_cb);
	this.sample = thunkify(this.sample_cb);
	// this.lookup = thunkify(this.lookup_cb);
}

module.exports = UgridArray;
