'use strict';

var fs = require('fs');
var Lines = require('../lib/lines.js');
var ml = require('../lib/ugrid-ml.js');

/*	NOUVELLE GESTION DES PARTITIONS
	On boucle sur les partitions avec indice p
	dans un lineage quand on veux sauver la partition, on regarde
	si l'indice de partition target_pid[lid][pid] existe,
	si (pid[lid][p] == undefined)
		pid[lid][p] = createNewPartition();	// on crée une nouvelle partition
	var pid = pid[lid][p];
	on sauve dans la partition pid */

function UgridTask(grid, RAM, msg) {
	var node = msg.data.args.node;
	var worker = msg.data.args.workerData;	
	var stageIdx = 0, finalCallback, stage = [];
	var STAGE_RAM = {v: undefined};

	function Stage (stageData) {
		var lineages = stageData.lineages; 	// Lineages vector
		var state = this.state = {
			cnt: 0,							// Number of finished lineages
			target_cnt: lineages.length,	// Number of lineages
			locked: false,					// Because of an asynchonous lineage
			nShuffle: 0						// Number of shuffle received
		}
		var action = stageData.action;
		var shuffleType = stageData.shuffleType;
		var shuffleNum = stageData.shuffleNum;

		function Lineage (lid, transform, callback) {
			var source = {}, lib = {};
			var fun = [], tmp = [];
			var res = action ? {v: action.init} : {v: {}};

			function save (t, p) {
				if (RAM[transform[t].dest_id] == undefined)
					RAM[transform[t].dest_id] = {};
				if (RAM[transform[t].dest_id][p] == undefined) {
					RAM[transform[t].dest_id][p] = tmp;
				} else {
					var t0 = RAM[transform[t].dest_id][p];
					var L = t0.length;
					for (var i = 0; i < tmp.length; i++)
						t0[L + i] = tmp[i];
				}
			}

			function pipeline (p) {
				for (var t = 1; t < transform.length; t++) {
					if (fun[t](t, p) == false) return;
					if (transform[t].persistent && (transform[t].dependency == 'narrow'))
						save(t, p);
				}
				action && fun_action(p);
			}

			source.fromRAM = function () {
				var input = RAM[transform[0].src_id];
				for (var p in input) {
					var partition = input[p];
					for (var i = 0; i < partition.length; i++) {
						tmp = [partition[i]];
						pipeline(p);
					}
				}
				callback(res);
			};

			source.fromSTAGERAM = function () {
				var input = STAGE_RAM.v;
				var persistent = transform[0].persistent;
				for (var p in input) {
					var partition = input[p];
					for (var i = 0; i < partition.length; i++) {
						tmp = [partition[i]];
						if (persistent) save(0, p);
						pipeline(p);
					}
				}
				callback(res);
			};

			source.parallelize = function () {
				var partition = node[transform[0].num].args[0];
				var persistent = transform[0].persistent;
				for (var p in partition) {
					for (var i = 0; i < partition[p].length; i++) {
						tmp = [partition[p][i]];
						if (persistent) save(0, p);
						pipeline(p);
					}
				}
				callback(res);
			};

			source.randomSVMData = function () {
				var num = transform[0].num;
				var D = node[num].args[0];
				var partition = node[num].args[1];
				var persistent = transform[0].persistent;
				for (var p in partition) {
					var rng = new ml.Random(partition[p].seed);
					for (var i = 0; i < partition[p].n; i++) {
						tmp = [ml.randomSVMLine(rng, D)];
						if (persistent) save(0, p);
						pipeline(p);
					}
				}
				callback(res);
			};

			source.textFile = function() {
				var num = transform[0].num;
				var persistent = transform[0].persistent;
				var dest_id = transform[0].dest_id;
				var file = node[num].args[0];
				var P = node[num].args[1];
				var partitionIdx = node[num].args[2];
				var l = 0;
				var lines = new Lines();
				state.locked = true;
				fs.createReadStream(file).pipe(lines);
				lines.on("data", function(line) {
					tmp = [line];
					var p = l++ % P;
					if (partitionIdx.indexOf(dest_id + '.' + p) != -1) {
						if (persistent) save(0, p);
						pipeline(p);
					}
				});
				lines.on("end", function() {
					state.locked = false;
					callback(res);
				});
			};

			// WARNING: only valid for one additional user parameter
			lib.map = function (t) {
				var n = node[transform[t].num];
				for (var i = 0; i < tmp.length; i++)
					tmp[i] = n.src(tmp[i], n.args[0]);
			};

			lib.filter = function (t) {
				var n = node[transform[t].num];
				var t0 = [];
				for (var i = 0; i < tmp.length; i++)
					if (n.src(tmp[i])) t0.push(tmp[i]);
				tmp = t0;
				return (tmp.length > 0);
			};

			lib.flatMap = function (t) {
				var n = node[transform[t].num];
				var t0 = [];
				for (var i = 0; i < tmp.length; i++) 
					t0 = t0.concat(n.src(tmp[i]));
				tmp = t0;
			};

			lib.sample = function (t, p) {
				// Ici sample represente l'indice de la partition dans le tableau des partitions
				// il sert à remplir la structure intermédiaire pour le poststage
				var num = transform[t].num;
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
				for (var i = 0; i < tmp.length; i++) {
					len[p]++;
					var current_frac = dest_p.length / len[p];
					if (current_frac < frac) dest_p.push(tmp[i]);
					else {
						var idx = Math.round(Math.abs(res.rng.next()) * len[p]);
						if (idx < dest_p.length) dest_p[idx] = tmp[i];
					}
				}
			};

			lib.groupByKey = function () {
				var dest = res.v;		
				for (var i = 0; i < tmp.length; i++) {
					var key = tmp[i][0];
					if (dest[key] == undefined) dest[key] = [[key, []]];
					dest[key][0][1].push(tmp[i][1]);
				}
			};

			lib.reduceByKey = function (t) {
				var n = node[transform[t].num];
				var dest = res.v;
				var initVal = n.args[0];
				var reduce = n.src;
				for (var i = 0; i < tmp.length; i++) {
					var key = tmp[i][0];
					if (dest[key] == undefined)
						dest[key] = [[key, JSON.parse(JSON.stringify(initVal))]];
					dest[key][0][1] = reduce(dest[key][0][1], tmp[i][1]);
				}
			};

			lib.union = function () {};

			lib.join = function (t) {
				var dest = res.v;
				var src_id = transform[t].src_id;
				for (var i = 0; i < tmp.length; i++) {
					var key = tmp[i][0];
					if (dest[key] == undefined) dest[key] = {};
					if (dest[key][src_id] == undefined) dest[key][src_id] = [];
					dest[key][src_id].push(tmp[i][1]);
				}
			};

			lib.coGroup = function (t) {
				var dest = res.v;
				var src_id = transform[t].src_id;
				for (var i = 0; i < tmp.length; i++) {
					var key = tmp[i][0];
					if (dest[key] == undefined) dest[key] = {};
					if (dest[key][src_id] == undefined) dest[key][src_id] = [];
					dest[key][src_id].push(tmp[i][1]);
				}
			};

			lib.crossProduct = function (t) {};

			// ATTENTION: valable pour un mapper avec au maximum un argument
			lib.mapValues = function (t) {
				var num = transform[t].num;
				var mapper = node[num].src;
				var arg0 = node[num].args[0];
				for (var i = 0; i < tmp.length; i++) 
					tmp[i][1] = mapper(tmp[i][1], arg0);
			};

			lib.count = function () {
				res.v += tmp.length;
			};

			// ici p est utilise pour indexer la structure de donnee de resultat
			lib.collect = function (p) {
				if (res.v[p] == undefined) res.v[p] = [];
				var dest = res.v[p];
				for (var i = 0; i < tmp.length; i++) 
					dest.push(tmp[i]);
			};

			lib.reduce = function () {
				var reduce = action.src;
				for (var i = 0; i < tmp.length; i++)
					res.v = reduce(res.v, tmp[i]);
			}

			// ici p est utilise pour indexer la structure de donnee de resultat
			lib.lookup = function (p) {			
				if (res.v[p] == undefined) res.v[p] = [];
				var dest = res.v[p];
				var arg0 = action.args[0];
				for (var i = 0; i < tmp.length; i++)
					if (tmp[i][0] == arg0)
						dest.push(tmp[i]);
			};

			for (var t = 1; t < transform.length; t++)
				fun[t] = lib[transform[t].type];
			var fun_action = action ? lib[action.fun] : null;

			source[transform[0].type]();
		}

		this.run = function (callback) {
			for (var l = 0; l < lineages.length; l++)
				new Lineage(l, lineages[l], function (res) {
					// if a lineage is not finished return
					if (state.locked || (++state.cnt < state.target_cnt)) return;
					// if action exists, it's the last stage return result
					if (action) return callback(res.v);

					// if preshuffle to be done, it's for sample, then no shuffle needed
					if (action) {
						pre_shuffle_lib[transform[transform.length - 1].type](res);
						return callback();
					}
					// if not the last stage, shuffle
					// Map partitions to workers
					var map = worker.map(function() {return {};});
					// shuffle using hash function
					for (var p in res.v)
						map[ml.cksum(p) % worker.length][p] = res.v[p];

					for (var i = 0; i < map.length; i++) {
						if (grid.host.uuid == worker[i].uuid) STAGE_RAM.v = map[i];
						else grid.request_cb(worker[i], {cmd: 'shuffle',args: map[i]}, function(err) {if (err) throw err;});
					}
					callback();
				});
		}

		var pre_shuffle_lib = {
			sample: function (data) {
				STAGE_RAM.v = data;
				return false;
			}
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
		stage[i] = new Stage(msg.data.args.stageData[i]);

	function nextStage(res) {
		// if last stage call finalCallback
		if (stageIdx == (stage.length - 1)) {
			STAGE_RAM = {v: undefined};
			finalCallback(res);
			return;
		}

		if (++stage[stageIdx].state.nShuffle == worker.length) {
			stage[stageIdx].postShuffle();
			stage[++stageIdx].run(nextStage);
		}
	}

	this.run = function(callback) {
		finalCallback = callback;
		stage[stageIdx].run(nextStage);
	};

	this.processShuffle = function(msg) {
		stage[stageIdx].shuffle(msg, function () {
			stage[stageIdx].postShuffle();
			stage[++stageIdx].run(nextStage);
		});
	};	
}

module.exports.UgridTask = UgridTask;
