'use strict';

var fs = require('fs');
var Lines = require('../lib/lines.js');
var ml = require('../lib/ugrid-ml.js');

function Stage(grid, STAGE_RAM, RAM, stageData, worker, node) {
	var lineages = stageData.lineages; 	// Lineages vector
	var state = this.state = {
		cnt: 0,							// Number of finished lineages
		target_cnt: lineages.length,	// Number of lineages
		locked: false,					// Stage can be locked because of an asynchonous lineage
		nShuffle: 0						// Number of shuffle received
	}
	var action = stageData.action;
	var shuffleType = stageData.shuffleType;
	var shuffleNum = stageData.shuffleNum;

	function Lineage (transform, callback) {
		var source = {}, lib = {};

		source.fromRAM = function (callback) {
			var input = RAM[transform[0].src_id];
			for (var p in input) {
				var partition = input[p];
				for (var i = 0; i < partition.length; i++) {
					tmp.v = [partition[i]];
					pipeline(p);
				}
			}
			if (!state.locked && (state.target_cnt == ++state.cnt)) 
				callback(res.v);
		};

		source.fromSTAGERAM = function (callback) {
			var input = STAGE_RAM.v;
			var persistent = transform[0].persistent;
			if (persistent) var output = RAM[transform[0].dest_id] = {};
			for (var p in input) {
				var partition = input[p];
				for (var i = 0; i < partition.length; i++) {
					tmp.v = [partition[i]];
					if (persistent) {
						if (output[p] == undefined) output[p] = [];
						output[p].push(partition[i]);
					}
					pipeline(p);				
				}
			}
			if (!state.locked && (state.target_cnt == ++state.cnt)) 
				callback(res.v);
		};

		source.parallelize = function (callback) {
			var partition = node[transform[0].num].args[0];
			var persistent = transform[0].persistent;
			if (persistent) {
				var output = RAM[transform[0].dest_id] = {};
				for (var p in partition) output[p] = [];
			}
			for (var p in partition) {
				for (var i = 0; i < partition[p].length; i++) {
					tmp.v = [partition[p][i]];
					if (persistent) output[p].push(tmp.v[0]);					
					pipeline(p);
				}
			}			
			if (!state.locked && (state.target_cnt == ++state.cnt)) 
				callback(res.v);
		};

		source.randomSVMData = function (callback) {
			var num = transform[0].num;
			var D = node[num].args[0];
			var partition = node[num].args[1];
			var persistent = transform[0].persistent;
			if (persistent) {
				var output = RAM[transform[0].dest_id] = {};
				for (var p in partition) output[p] = [];
			}
			for (var p in partition) {
				var rng = new ml.Random(partition[p].seed);
				for (var i = 0; i < partition[p].n; i++) {
					tmp.v = [ml.randomSVMLine(rng, D)];
					if (persistent) output[p].push(tmp.v[0]);
					pipeline(p);
				}
			}
			if (!state.locked && (state.target_cnt == ++state.cnt)) 
				callback(res.v);
		};

		source.textFile = function(callback) {
			var num = transform[0].num;
			var persistent = transform[0].persistent;
			var dest_id = transform[0].dest_id;
			var file = node[num].args[0];
			var P = node[num].args[1];
			var partitionIdx = node[num].args[2];
			var l = 0;
			var lines = new Lines();
			if (persistent) {
				var output = RAM[dest_id] = {};
				for (var p = 0; p < partitionIdx.length; p++) output[partitionIdx[p]] = [];
			}
			state.locked = true;
			fs.createReadStream(file).pipe(lines);
			lines.on("data", function(line) {
				tmp.v = [line];
				var p = l++ % P;
				if (partitionIdx.indexOf(dest_id + '.' + p) != -1) {
					if (persistent) {
						if (output[p] == undefined) output[p] = [];
						output[p].push(line);
					}
					pipeline(p);
				}
			});
			lines.on("end", function() {
				state.locked = false;
				if (state.target_cnt == ++state.cnt) callback(res.v);
			});
		};

		// WARNING: only valid for one additional user parameter
		lib.map = function (t) {
			var n = node[transform[t].num];
			var data = tmp.v;
			for (var i = 0; i < data.length; i++)
				data[i] = n.src(data[i], n.args[0]);
		};

		lib.filter = function (t) {
			var n = node[transform[t].num];
			var data = tmp.v;
			var t0 = [];
			for (var i = 0; i < data.length; i++)
				if (n.src(data[i])) t0.push(data[i]);
			tmp.v = t0;
			return (data.length > 0);
		};

		lib.flatMap = function (t) {
			var n = node[transform[t].num];
			var data = tmp.v;
			var t0 = [];
			for (var i = 0; i < data.length; i++) t0 = t0.concat(n.src(data[i]));
			tmp.v = t0;
		};

		lib.sample = function (t, p) {
			// Ici sample represente l'indice de la partition dans le tableau des partitions
			// il sert à remplir la structure intermédiaire pour le poststage
			var num = transform[t].num;
			var data = tmp.v;
			var frac = node[num].args[0];
			var seed = node[num].args[1];

			if (res.len == undefined) res.len = {};
			if (res.rng == undefined) res.rng = new ml.Random(seed);
			if (res.v[p] == undefined) {
				res.v[p] = [];
				res.len[p] = 0;
			}
			var dest_p = res.v[p];
			var len = res.len;
			for (var i = 0; i < data.length; i++) {
				len[p]++;
				var current_frac = dest_p.length / len[p];
				if (current_frac < frac) dest_p.push(data[i]);
				else {
					var idx = Math.round(Math.abs(res.rng.next()) * len[p]);
					if (idx < dest_p.length) dest_p[idx] = data[i];
				}
			}
		};

		lib.groupByKey = function () {
			var data = tmp.v;
			var dest = res.v;		
			for (var i = 0; i < data.length; i++) {
				var key = data[i][0];
				if (dest[key] == undefined) dest[key] = [[key, []]];
				dest[key][0][1].push(data[i][1]);
			}
		};

		lib.reduceByKey = function (t) {
			var n = node[transform[t].num];
			var data = tmp.v;
			var dest = res.v;
			var initVal = n.args[0];
			var reduce = n.src;
			for (var i = 0; i < data.length; i++) {
				var key = data[i][0];
				if (dest[key] == undefined)
					dest[key] = [[key, JSON.parse(JSON.stringify(initVal))]];
				dest[key][0][1] = reduce(dest[key][0][1], data[i][1]);
			}
		};

		lib.union = function () {};

		lib.join = function (t) {
			var data = tmp.v;
			var dest = res.v;
			var src_id = transform[t].src_id;
			for (var i = 0; i < data.length; i++) {
				var key = data[i][0];
				if (dest[key] == undefined) dest[key] = {};
				if (dest[key][src_id] == undefined) dest[key][src_id] = [];
				dest[key][src_id].push(data[i][1]);
			}
		};

		lib.coGroup = function (t) {
			var data = tmp.v;
			var dest = res.v;
			var src_id = transform[t].src_id;
			for (var i = 0; i < data.length; i++) {
				var key = data[i][0];
				if (dest[key] == undefined) dest[key] = {};
				if (dest[key][src_id] == undefined) dest[key][src_id] = [];
				dest[key][src_id].push(data[i][1]);
			}
		};

		lib.crossProduct = function (t) {};

		// ATTENTION: valable pour un mapper avec au maximum un argument
		lib.mapValues = function (t) {
			var num = transform[t].num;
			var data = tmp.v;
			var mapper = node[num].src;
			var arg0 = node[num].args[0];
			for (var i = 0; i < data.length; i++) 
				data[i][1] = mapper(data[i][1], arg0);
		};

		lib.count = function () {
			res.v += tmp.v.length;
		};

		// ici p est utilise pour indexer la structure de donnee de resultat
		lib.collect = function (p) {
			var data = tmp.v;
			if (res.v[p] == undefined) res.v[p] = [];
			var dest = res.v[p];
			for (var i = 0; i < data.length; i++) 
				dest.push(data[i]);
		};

		lib.reduce = function () {
			var reduce = action.src;
			var data = tmp.v;
			for (var i = 0; i < data.length; i++)
				res.v = reduce(res.v, data[i]);
		}

		// ici p est utilise pour indexer la structure de donnee de resultat
		lib.lookup = function (p) {			
			var data = tmp.v;
			if (res.v[p] == undefined) res.v[p] = [];
			var dest = res.v[p];
			var arg0 = action.args[0];
			for (var i = 0; i < data.length; i++)
				if (data[i][0] == arg0) dest.push(data[i]);
		};

		function pipeline (p) {			
			for (var t = 1; t < transform.length; t++) {				
				if (fun[t](t, p) == false) return;
				if (transform[t].persistent && (transform[t].dependency == 'narrow')) {
					if (RAM[transform[t].dest_id] == undefined) RAM[transform[t].dest_id] = {};
					if (RAM[transform[t].dest_id][p] == undefined) RAM[transform[t].dest_id][p] = tmp.v;
					else RAM[transform[t].dest_id][p] = RAM[transform[t].dest_id][p].concat(tmp.v);
				}
			}
			action && fun_action(p);
		}

		var fun = [];
		for (var t = 1; t < transform.length; t++)
			fun[t] = lib[transform[t].type];
		var fun_action = action ? lib[action.fun] : null;
		var tmp = {v : []};
		var res = action ? {v: action.init} : {v: {}};

		source[transform[0].type](callback);
	}

	this.run = function (callback) {
		for (var l = 0; l < lineages.length; l++) {
			try {new Lineage(lineages[l], callback);} 
			catch (e) {throw e;}
		}
	}

	var pre_shuffle_lib = {
		sample: function (data) {
			STAGE_RAM.v = data;
			return false;
		}
	}

	this.preShuffle = function (data) {
		if (pre_shuffle_lib[shuffleType])
			return pre_shuffle_lib[shuffleType](data);
		return true;
	}

	var shuffle_lib = {};
	shuffle_lib.groupByKey = function (node, msg, callback) {
		var data = msg.data.args;
		for (var key in data) {
			if (STAGE_RAM.v[key] == undefined) STAGE_RAM.v[key] = data[key];
			else STAGE_RAM.v[key][0][1] = STAGE_RAM.v[key][0][1].concat(data[key][0][1]);
		}
		grid.reply(msg, null, 'Shuffle done by worker ' + msg.id);
		callback();
	};

	shuffle_lib.reduceByKey = function (node, msg, callback) {
		var data = msg.data.args;
		for (var key in data) {
			if (STAGE_RAM.v[key] == undefined) STAGE_RAM.v[key] = data[key];
			else STAGE_RAM.v[key][0][1] = node.src(STAGE_RAM.v[key][0][1], data[key][0][1]);
		}
		grid.reply(msg, null, 'Shuffle done by worker ' + msg.id);
		callback();
	};

	shuffle_lib.join = function (node, msg, callback) {
		var data = msg.data.args;
		for (var key in data)
			for (var dataset in data[key]) {
				if (STAGE_RAM.v[key][dataset] == undefined) STAGE_RAM.v[key][dataset] = [];
				STAGE_RAM.v[key][dataset] = STAGE_RAM.v[key][dataset].concat(data[key][dataset]);
			}
		grid.reply(msg, null, 'Shuffle done by worker ' + msg.id);
		callback();
	};

	shuffle_lib.coGroup = function (node, msg, callback) {
		var data = msg.data.args;
		for (var key in data) {
			for (var dataset in data[key]) {
				if (STAGE_RAM.v[key][dataset] == undefined) STAGE_RAM.v[key][dataset] = [];
				STAGE_RAM.v[key][dataset] = STAGE_RAM.v[key][dataset].concat(data[key][dataset]);
			}
		}
		grid.reply(msg, null, 'Shuffle done by worker ' + msg.id);
		callback();
	};

	this.shuffle = function (msg, callback) {
		shuffle_lib[shuffleType](node[shuffleNum], msg, callback);
	}

	var post_shuffle_lib = {};
	post_shuffle_lib.join = function (node) {
		var res = {};
		for (var key in STAGE_RAM.v) {
			var datasets = Object.keys(STAGE_RAM.v[key]);
			if (datasets.length != 2) continue;
			res[key] = [];
			for (var i = 0; i < STAGE_RAM.v[key][node.child[0]].length; i++)
				for (var j = 0; j < STAGE_RAM.v[key][node.child[1]].length; j++)
					res[key].push([key, [STAGE_RAM.v[key][node.child[0]][i], STAGE_RAM.v[key][node.child[1]][j]]]);
		}
		STAGE_RAM.v = res;
	};

	post_shuffle_lib.coGroup = function (node) {
		var res = {};
		for (var key in STAGE_RAM.v) {
			var datasets = Object.keys(STAGE_RAM.v[key]);
			if (datasets.length != 2)
				continue;
			res[key] = [[key, [STAGE_RAM.v[key][node.child[0]], STAGE_RAM.v[key][node.child[1]]]]];
		}
		STAGE_RAM.v = res;
	}	

	this.postShuffle = function (msg, callback) {
		post_shuffle_lib[shuffleType] && post_shuffle_lib[shuffleType](node[shuffleNum]);
	}
}

function UgridTask(grid, STAGE_RAM, RAM, msg) {
	var node = msg.data.args.node;
	var worker = msg.data.args.workerData;	
	var stageIdx = 0, finalCallback, stage = [];

	function recompile(s) {
		var args = s.match(/\(([^)]*)/)[1];
		var body = s.replace(/^function *[^)]*\) *{/, '').replace(/}$/, '');
		return new Function(args, body);
	}

	for (var i in node)
		if (node[i].src) node[i].src = recompile(node[i].src);

	var action = msg.data.args.stageData[msg.data.args.stageData.length - 1].action;
	if (action.src) {
		action.ml = ml;
		action.src = recompile(action.src);
	}

	for (var i = 0; i < msg.data.args.stageData.length; i++)
		stage[i] = new Stage(grid, STAGE_RAM, RAM, msg.data.args.stageData[i], worker, node);	

	function shuffleRPC(host, args, callback) {
		grid.request_cb(host, {
			cmd: 'shuffle',
			args: args
		}, callback);
	}

	function postStage (res) {		
		if (stageIdx == (stage.length - 1)) {
			STAGE_RAM = {v: undefined};
			finalCallback(res);
		} else {
			// IF preshuffle to be done, it's for sample, then no shuffle needed
			// go to next stage immediatly
			if (stage[stageIdx].preShuffle(res) == false) {
				stage[++stageIdx].run(postStage);
				return;
			}

			// Map partitions to workers
			var map = worker.map(function() {return {};});
			// WARNING: Ok if partition name is a Number for now, use hashcoding later
			//for (var p in res) map[p % worker.length][p] = res[p];
			for (var p in res) map[ml.cksum(p) % worker.length][p] = res[p];

			for (var i = 0; i < map.length; i++) {
				if (grid.host.uuid == worker[i].uuid) {
					stage[stageIdx].state.nShuffle++;
					STAGE_RAM.v = map[i];
				} else shuffleRPC(worker[i], map[i], function(err) {if (err) throw err;});
			}
			// Run next stage if needed
			if (worker.length == 1) {
				stage[stageIdx].postShuffle();
				stage[++stageIdx].run(postStage);
			}
		}
	}

	this.run = function(callback) {
		finalCallback = callback;
		stage[stageIdx].run(postStage);
	};

	this.processShuffle = function(msg) {
		stage[stageIdx].shuffle(msg, function () {
			if (++stage[stageIdx].state.nShuffle == worker.length) {
				stage[stageIdx].postShuffle();
				stage[++stageIdx].run(postStage);
			}
		});
	};
}

module.exports.UgridTask = UgridTask;
