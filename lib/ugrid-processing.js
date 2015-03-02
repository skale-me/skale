'use strict';

var fs = require('fs');
var Lines = require('../lib/lines.js');
var ml = require('../lib/ugrid-ml.js');

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
		var res = action ? {v: action.init} : {v: {}};
		
		function Lineage (lid, transform, callback) {
			var fun = [], tmp = [], partitionMapper = {};

			function save (t) {
				var dest_id = transform[t].dest_id;
				if (partitionMapper[dest_id] == undefined) {	// le dataset dest_id n'existe pas, on le crée					
					partitionMapper[dest_id] = [];				// Nouveau vecteur associé au lineage
					partitionMapper[dest_id][t] = 0;
					RAM[dest_id] = [{data: []}];							// nouveau vecteur de partition dans la RAM					
				} else if (partitionMapper[dest_id][t] == undefined) {
					// la partition n'existe pas on la crée
					partitionMapper[dest_id][t] = RAM[dest_id].length;
					RAM[dest_id].push({data: []});						// nouvelle partition
				}
				// on récupère l'indice de la partition dans laquelle stocker les datas
				var idx = partitionMapper[dest_id][t];				
				var t0 = RAM[dest_id][idx].data;
				var L = t0.length;
				for (var i = 0; i < tmp.length; i++)
					t0[L + i] = tmp[i];
			}

			function pipeline (p) {
				for (var t = 1; t < transform.length; t++) {
					if (fun[t](t, p) == false) return;
					if (transform[t].persistent && (transform[t].dependency == 'narrow'))
						save(t);
				}
				action && fun_action(p);
			}

			var source = {
				fromRAM: function () {
					var input = RAM[transform[0].src_id];
					if (input == undefined) { // work around, sometime empty partitions are created
						callback(lid, res);
						return;
					}
					for (var p = 0; p < input.length; p++) {
						var partition = input[p].data;
						for (var i = 0; i < partition.length; i++) {
							tmp = [partition[i]];
							pipeline(p);
						}
					}					
					callback(lid, res);
				},
				fromSTAGERAM: function () {
					var input = STAGE_RAM.v;					
					var persistent = transform[0].persistent;
					for (var p = 0; p < input.length; p++) {
						var partition = input[p].data;
						for (var i = 0; i < partition.length; i++) {
							tmp = [partition[i]];
							if (persistent) save(0);
							pipeline(p);
						}
					}
					callback(lid, res);
				},
				parallelize: function () {
					var input = node[transform[0].num].args[0];
					var persistent = transform[0].persistent;
					for (var p = 0; p < input.length; p++) {
						var partition = input[p];
						for (var i = 0; i < partition.length; i++) {
							tmp = [partition[i]];
							if (persistent) save(0);
							pipeline(p);
						}
					}
					callback(lid, res);
				},
				randomSVMData: function () {
					var num = transform[0].num;
					var D = node[num].args[0];
					var partition = node[num].args[1];
					var persistent = transform[0].persistent;
					for (var p = 0; p < partition.length; p++) {
						var rng = new ml.Random(partition[p].seed);
						for (var i = 0; i < partition[p].n; i++) {
							tmp = [ml.randomSVMLine(rng, D)];
							if (persistent) save(0);
							pipeline(p);
						}
					}
					callback(lid, res);
				},
				textFile: function() {
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
							if (persistent) save(0);
							pipeline(p);
						}
					});
					lines.on("end", function() {
						state.locked = false;
						callback(lid, res);
					});
				}
			}

			// WARNING: only valid for one additional user parameter
			var lib = {
				map: function (t) {
					var n = node[transform[t].num];
					for (var i = 0; i < tmp.length; i++)
						tmp[i] = n.src(tmp[i], n.args[0]);
				}, 
				filter: function (t) {
					var n = node[transform[t].num];
					var t0 = [];
					for (var i = 0; i < tmp.length; i++)
						if (n.src(tmp[i])) t0.push(tmp[i]);
					tmp = t0;
					return (tmp.length > 0);
				},
				flatMap: function (t) {
					var n = node[transform[t].num];
					var t0 = [];
					for (var i = 0; i < tmp.length; i++) 
						t0 = t0.concat(n.src(tmp[i]));
					tmp = t0;
				},
				sample: function (t, p) {							// passer l'indice du lineage en plus de p
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
				},
				groupByKey: function () {
					var dest = res.v;
					for (var i = 0; i < tmp.length; i++) {
						var key = tmp[i][0];
						if (dest[key] == undefined) dest[key] = [[key, []]];
						dest[key][0][1].push(tmp[i][1]);
					}
				},
				reduceByKey: function (t) {
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
				},
				union: function () {},
				join: function (t) {
					var dest = res.v;
					var src_id = transform[t].src_id;
					for (var i = 0; i < tmp.length; i++) {
						var key = tmp[i][0];
						if (dest[key] == undefined) dest[key] = {key: key};
						if (dest[key][src_id] == undefined) dest[key][src_id] = [];
						dest[key][src_id].push(tmp[i][1]);
					}
				},
				coGroup: function (t) {
					var dest = res.v;
					var src_id = transform[t].src_id;
					for (var i = 0; i < tmp.length; i++) {
						var key = tmp[i][0];
						if (dest[key] == undefined) dest[key] = {};
						if (dest[key][src_id] == undefined) dest[key][src_id] = [];
						dest[key][src_id].push(tmp[i][1]);
					}
				},
				crossProduct: function (t) {},
				// ATTENTION: valable pour un mapper avec au maximum un argument
				mapValues: function (t) {
					var num = transform[t].num;
					var mapper = node[num].src;
					var arg0 = node[num].args[0];
					for (var i = 0; i < tmp.length; i++) 
						tmp[i][1] = mapper(tmp[i][1], arg0);
				},
				count: function () {
					res.v += tmp.length;
				},
				collect: function (p) {
					if (res.v[p] == undefined) res.v[p] = [];
					var dest = res.v[p];
					for (var i = 0; i < tmp.length; i++) 
						dest.push(tmp[i]);
				},
				reduce: function () {
					var reduce = action.src;
					for (var i = 0; i < tmp.length; i++)
						res.v = reduce(res.v, tmp[i]);
				},
				lookup: function (p) {
					if (res.v[p] == undefined) res.v[p] = [];
					var dest = res.v[p];
					var arg0 = action.args[0];
					for (var i = 0; i < tmp.length; i++)
						if (tmp[i][0] == arg0)
							dest.push(tmp[i]);
				}
			};

			for (var t = 1; t < transform.length; t++)
				fun[t] = lib[transform[t].type];
			var fun_action = action ? lib[action.fun] : null;

			source[transform[0].type]();
		}

		this.run = function (callback) {
			for (var l = 0; l < lineages.length; l++)
				new Lineage(l, lineages[l], function (lid, res) {
					// if a lineage is not finished return
					if (state.locked || (++state.cnt < state.target_cnt)) return;
					// if action exists, it's the last stage return result
					if (action) return callback(res.v);
					tx_shuffle_lib[shuffleType]();					
					callback();
				});
		}

		var tx_shuffle_lib = {
			sample: function () {
				STAGE_RAM.v = [];
				for (var i in res.v) STAGE_RAM.v.push({data: res.v[i]});
				state.nShuffle += worker.length - 1;	// make progress
			},
			groupByKey: function () {
				STAGE_RAM.v = [];
				var map = worker.map(function() {return {};});
				// shuffle using hash function
				for (var p in res.v)
					map[ml.cksum(p) % worker.length][p] = res.v[p];

				for (var i = 0; i < map.length; i++) {
					if (grid.host.uuid == worker[i].uuid) {
						for (var key in map[i])
							STAGE_RAM.v.push({key: key, data: map[i][key]});
					} else
						grid.request_cb(worker[i], {cmd: 'shuffle', args: map[i]}, function(err) {if (err) throw err;});
				}
			},
			reduceByKey: function () {
				STAGE_RAM.v = [];
				var map = worker.map(function() {return {};});
				// shuffle using hash function
				for (var p in res.v)
					map[ml.cksum(p) % worker.length][p] = res.v[p];

				for (var i = 0; i < map.length; i++) {
					if (grid.host.uuid == worker[i].uuid) {
						for (var key in map[i])
							STAGE_RAM.v.push({key: key, data: map[i][key]});
					} else
						grid.request_cb(worker[i], {cmd: 'shuffle', args: map[i]}, function(err) {if (err) throw err;});
				}
			},
			join: function () {
				STAGE_RAM.v = {};
				var map = worker.map(function() {return {};});
				// shuffle using hash function
				for (var p in res.v)
					map[ml.cksum(p) % worker.length][p] = res.v[p];

				// Need to handle order of (V, W) pairs
				for (var i = 0; i < map.length; i++) {
					if (grid.host.uuid == worker[i].uuid) STAGE_RAM.v = map[i];
					else
						grid.request_cb(worker[i], {cmd: 'shuffle', args: map[i]}, function(err) {if (err) throw err;});
				}				
			}
		}

		var rx_shuffle_lib = {
			groupByKey: function (node, msg, callback) {
				var data = msg.data.args;
				for (var key in data) {
					var idx = -1;
					for (var i = 0; i < STAGE_RAM.v.length; i++) {
						if (STAGE_RAM.v[i].key == key) {
							idx = i;
							break;
						}
					}
					if (idx == -1) STAGE_RAM.v.push({key: key, data: data[key]});
					else STAGE_RAM.v[i].data[0][1] = STAGE_RAM.v[i].data[0][1].concat(data[key][0][1]);
				}
				grid.reply(msg, null, 'Shuffle done by worker ' + msg.id);
				callback();
			},
			reduceByKey: function (node, msg, callback) {
				var data = msg.data.args;
				for (var key in data) {
					var idx = -1;
					for (var i = 0; i < STAGE_RAM.v.length; i++) {
						if (STAGE_RAM.v[i].key == key) {
							idx = i;
							break;
						}
					}
					if (idx == -1) STAGE_RAM.v.push({key: key, data: data[key]});
					else STAGE_RAM.v[i].data[0][1] = node.src(STAGE_RAM.v[i].data[0][1], data[key][0][1]);
				}
				grid.reply(msg, null, 'Shuffle done by worker ' + msg.id);
				callback();	
			},
			join: function (node, msg, callback) {
				var data = msg.data.args;
				for (var i in data) {
					if (i in STAGE_RAM.v) {
						// console.log(STAGE_RAM.v[i])
						for (var k in data[i]) {
							if (k == 'key') continue;
							if (STAGE_RAM.v[i][k] == undefined) STAGE_RAM.v[i][k] = data[i][k];
							else STAGE_RAM.v[i][k] = STAGE_RAM.v[i][k].concat(data[i][k]);
						}
					} else STAGE_RAM.v[i] = data[i];
				}
				grid.reply(msg, null, 'Shuffle done by worker ' + msg.id);
				callback();
			},
			coGroup: function (node, msg, callback) {
				throw 'error1234';
				var data = msg.data.args;
				for (var key in data) {
					for (var dataset in data[key]) {
						if (STAGE_RAM.v[key][dataset] == undefined) STAGE_RAM.v[key][dataset] = [];
						STAGE_RAM.v[key][dataset] = STAGE_RAM.v[key][dataset].concat(data[key][dataset]);
					}
				}
				grid.reply(msg, null, 'Shuffle done by worker ' + msg.id);
				callback();
			}
		}

		this.shuffle = function (msg, callback) {
			rx_shuffle_lib[shuffleType](node[shuffleNum], msg, callback);
		}

		var post_shuffle_lib = {
			join: function (node) {
				var res = [];
				for (var i in STAGE_RAM.v) {
					var datasets = Object.keys(STAGE_RAM.v[i]);
					if (datasets.length != 3) continue;
					var t0 = [];
					for (var j = 0; j < STAGE_RAM.v[i][node.child[0]].length; j++)
						for (var k = 0; k < STAGE_RAM.v[i][node.child[1]].length; k++)
							t0.push([STAGE_RAM.v[i].key, [STAGE_RAM.v[i][node.child[0]][j], STAGE_RAM.v[i][node.child[1]][k]]])
					res.push({key: STAGE_RAM.v[i].key, data: t0})
				}
				STAGE_RAM.v = res;
			},
			coGroup: function (node) {
				throw 'error';
				var res = {};
				for (var key in STAGE_RAM.v) {
					var datasets = Object.keys(STAGE_RAM.v[key]);
					if (datasets.length != 2) continue;
					res[key] = [[key, [STAGE_RAM.v[key][node.child[0]], STAGE_RAM.v[key][node.child[1]]]]];
				}
				STAGE_RAM.v = res;
			}
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
		stage[stageIdx].shuffle(msg, nextStage);
	};	
}

module.exports.UgridTask = UgridTask;
