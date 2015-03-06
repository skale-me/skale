'use strict';

var fs = require('fs');
var Lines = require('../lib/lines.js');
var ml = require('../lib/ugrid-ml.js');

function UgridTask(grid, RAM, msg) {
	var node = msg.data.args.node;
	var worker = msg.data.args.workerData;	
	var stageIdx = 0, finalCallback, stage = [];
	var SRAM = {};

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

		function Lineage (transform, callback) {
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
					var input = RAM[transform[0].src_id] || [];
					//if (input == undefined) { // work around, sometime empty partitions are created
					//	callback(res);
					//	return;
					//}
					for (var p = 0; p < input.length; p++) {
						var partition = input[p].data;
						for (var i = 0; i < partition.length; i++) {
							tmp = [partition[i]];
							pipeline(p);
						}
					}					
					callback(res);
				},
				fromSTAGERAM: function () {
					var persistent = transform[0].persistent;
					for (var p = 0; p < SRAM.length; p++) {
						var partition = SRAM[p].data;
						for (var i = 0; i < partition.length; i++) {
							tmp = [partition[i]];
							if (persistent) save(0);
							pipeline(p);
						}
					}
					callback(res);
				},
				parallelize: function () {
					var input = node[transform[0].num].args[0] || [];
					var persistent = transform[0].persistent;
					for (var p = 0; p < input.length; p++) {
						var partition = input[p];
						for (var i = 0; i < partition.length; i++) {
							tmp = [partition[i]];
							if (persistent) save(0);
							pipeline(p);
						}
					}
					callback(res);
				},
				randomSVMData: function () {
					var num = transform[0].num;
					var D = node[num].args[0];
					var partition = node[num].args[1] || [];
					var persistent = transform[0].persistent;
					for (var p = 0; p < partition.length; p++) {
						var rng = new ml.Random(partition[p].seed);
						for (var i = 0; i < partition[p].n; i++) {
							tmp = [ml.randomSVMLine(rng, D)];
							if (persistent) save(0);
							pipeline(p);
						}
					}
					callback(res);
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
						callback(res);
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
				sample: function (t, p) {
					if (res.v[p] == undefined) res.v[p] = [];
					for (var i = 0; i < tmp.length; i++)
						res.v[p].push(tmp[i]);
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
						if (dest[key] == undefined) dest[key] = {key: key};
						if (dest[key][src_id] == undefined) dest[key][src_id] = [];
						dest[key][src_id].push(tmp[i][1]);
					}
				},
				crossProduct: function (t, p) {
					var dest = res.v;
					var src_id = transform[t].src_id;
					if (dest[src_id] == undefined) dest[src_id] = [];
					if (dest[src_id][p] == undefined) dest[src_id][p] = [];
					for (var i = 0; i < tmp.length; i++)
						dest[src_id][p].push(tmp[i]);
				},
				// ATTENTION: valable pour un mapper avec au maximum un argument
				mapValues: function (t) {
					var num = transform[t].num;
					var mapper = node[num].src;
					var arg0 = node[num].args[0];
					for (var i = 0; i < tmp.length; i++) 
						tmp[i][1] = mapper(tmp[i][1], arg0);
				},
				distinct: function (t, p) {
					loop1:
					for (var i = 0; i < tmp.length; i++) {
						var str = JSON.stringify(tmp[i]);
						var wid = ml.cksum(str) % worker.length;
						if (res.v[wid] == undefined) {
							res.v[wid] = [];
							res.v[wid][p] = {data: [str]};
						} else {
							for (var j = 0; j < res.v[wid].length; j++)
								if (res.v[wid][j].data.indexOf(str) != -1) continue loop1;
							res.v[wid][p].data.push(str);
						}
					}
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
						if (tmp[i][0] == arg0) dest.push(tmp[i]);
				}
			};

			for (var t = 1; t < transform.length; t++)
				fun[t] = lib[transform[t].type];
			var fun_action = action ? lib[action.fun] : null;

			try {
				source[transform[0].type]();
			} catch (err) {
				throw "Lineage error, " + transform[0].type + ": " + err;
			}
		}

		this.run = function (callback) {
			for (var l = 0; l < lineages.length; l++)
				new Lineage(lineages[l], function (res) {
					// if a lineage is not finished return
					if (state.locked || (++state.cnt < state.target_cnt)) return;
					// if action exists, it's the last stage return result
					if (action) return callback(res.v);
					try {
						tx_shuffle_lib[shuffleType](node[shuffleNum]);
						callback();
					} catch (err) {
						throw "Lineage tx shuffle " + shuffleType + ": " + err;
					}
				});
		}

		var tx_shuffle_lib = {
			sample: function (node) {
				var withReplacement = node.args[0];
				var frac = node.args[1], seed = node.args[2], p = 0;
				SRAM = [];
				var rng = new ml.Random(seed);
				for (var i in res.v) {
					var L = res.v[i].length;
					var L = Math.ceil(L * frac);
					SRAM[p] = {data: []};
					var idxVect = [];
					while (SRAM[p].data.length != L) {
						var idx = Math.round(Math.abs(rng.next()) * (L - 1));
						if (!withReplacement && (idxVect.indexOf(idx) != -1))
							continue;	// if already picked but no replacement mode
						idxVect.push[idx];
						SRAM[p].data.push(res.v[i][idx]);
					}
					p++;
				}
				state.nShuffle = worker.length;	// Cancel shuffle
			},
			groupByKey: function () {
				SRAM = [];
				var map = worker.map(function() {return {};});
				for (var p in res.v) map[ml.cksum(p) % worker.length][p] = res.v[p];
				for (var i = 0; i < map.length; i++)
					if (grid.host.uuid == worker[i].uuid) rx_shuffle_lib.groupByKey(node[shuffleNum], map[i]);
					else grid.request_cb(worker[i], {cmd: 'shuffle', args: map[i]}, function(err) {if (err) throw err;});
			},
			reduceByKey: function () {
				SRAM = [];
				var map = worker.map(function() {return {};});
				for (var p in res.v) map[ml.cksum(p) % worker.length][p] = res.v[p];
				for (var i = 0; i < map.length; i++)
					if (grid.host.uuid == worker[i].uuid) rx_shuffle_lib.reduceByKey(node[shuffleNum], map[i]);
					else grid.request_cb(worker[i], {cmd: 'shuffle', args: map[i]}, function(err) {if (err) throw err;});
			},
			join: function () {
				var map = worker.map(function() {return {};});
				for (var p in res.v) map[ml.cksum(p) % worker.length][p] = res.v[p];
				for (var i = 0; i < map.length; i++)
					if (grid.host.uuid == worker[i].uuid) rx_shuffle_lib.join(node[shuffleNum], map[i]);
					else grid.request_cb(worker[i], {cmd: 'shuffle', args: map[i]}, function(err) {if (err) throw err;});
			},
			coGroup: function () {
				var map = worker.map(function() {return {};});
				for (var p in res.v)
					map[ml.cksum(p) % worker.length][p] = res.v[p];
				for (var i = 0; i < map.length; i++)
					if (grid.host.uuid == worker[i].uuid) rx_shuffle_lib.coGroup(node[shuffleNum], map[i]);
					else grid.request_cb(worker[i], {cmd: 'shuffle', args: map[i]}, function(err) {if (err) throw err;});
			},
			crossProduct: function () {
				var map = [], k = 0;
				var keys = Object.keys(res.v);
				var t0 = res.v[keys[0]].length;
				var t1 = res.v[keys[1]].length;
				var map = new Array(worker.length);
				while (t0 && t1) {
					if (map[k] == undefined) {
						map[k] = {};
						map[k][keys[0]] = [];
						map[k][keys[1]] = [];
					}
					map[k][keys[0]].push(res.v[keys[0]][res.v[keys[0]].length - t0--]);
					map[k][keys[1]].push(res.v[keys[1]][res.v[keys[1]].length - t1--]);
					k = (k + 1) % worker.length;
				}
				for (var i = 0; i < map.length; i++)
					if (grid.host.uuid == worker[i].uuid) rx_shuffle_lib.crossProduct(node[shuffleNum], map[i] || {});
					else grid.request_cb(worker[i], {cmd: 'shuffle', args: map[i] || {}}, function(err) {if (err) throw err;});
			},
			distinct: function () {
				SRAM = [];	// BUG
				for (var i = 0; i < worker.length; i++)
					if (worker[i].uuid == grid.host.uuid) rx_shuffle_lib.distinct(node[shuffleNum], res.v[i]);
					else grid.request_cb(worker[i], {cmd: 'shuffle', args: res.v[i] || {}}, function(err) {if (err) throw err;});
			}
		}

		var rx_shuffle_lib = {
			groupByKey: function (node, data) {
				for (var key in data) {
					for (var i = 0; i < SRAM.length; i++)
						if (SRAM[i].key == key) break;
					if (i == SRAM.length) SRAM.push({key: key, data: data[key]});
					else SRAM[i].data[0][1] = SRAM[i].data[0][1].concat(data[key][0][1]);
				}
				state.nShuffle++;
			},
			reduceByKey: function (node, data) {
				for (var key in data) {
					for (var i = 0; i < SRAM.length; i++)
						if (SRAM[i].key == key) break;
					if (i == SRAM.length) SRAM.push({key: key, data: data[key]});
					else SRAM[i].data[0][1] = node.src(SRAM[i].data[0][1], data[key][0][1]);
				}
				state.nShuffle++;
			},
			join: function (node, data) {
				for (var i in data)
					if (i in SRAM)
						for (var k in data[i]) {
							if (k == 'key') continue;
							if (SRAM[i][k] == undefined) SRAM[i][k] = data[i][k];
							else SRAM[i][k] = SRAM[i][k].concat(data[i][k]);
						}
					else SRAM[i] = data[i];
				// When all shuffle has been received
				if (++state.nShuffle < worker.length) return;
				var res = [];
				for (var i in SRAM) {
					var datasets = Object.keys(SRAM[i]);
					if (datasets.length != 3) continue;
					var t0 = [];
					for (var j = 0; j < SRAM[i][node.child[0]].length; j++)
						for (var k = 0; k < SRAM[i][node.child[1]].length; k++)
							t0.push([SRAM[i].key, [SRAM[i][node.child[0]][j], SRAM[i][node.child[1]][k]]])
					res.push({key: SRAM[i].key, data: t0})
				}
				SRAM = res;
			},
			coGroup: function (node, data) {
				for (var i in data)
					if (i in SRAM) {
						for (var k in data[i]) {
							if (k == 'key') continue;
							if (SRAM[i][k] == undefined) SRAM[i][k] = data[i][k];
							else SRAM[i][k] = SRAM[i][k].concat(data[i][k]);
						}
					} else SRAM[i] = data[i];
				// When all shuffle has been received
				if (++state.nShuffle < worker.length) return;
				var res = [];
				for (var i in SRAM) {
					var datasets = Object.keys(SRAM[i]);
					if (datasets.length != 3) continue;
					res.push({
						key: SRAM[i].key,
						data:[[SRAM[i].key, [SRAM[i][node.child[0]], SRAM[i][node.child[1]]]]]
					})
				}
				SRAM = res;
			},
			crossProduct: function (node, data) {
				for (var i in data)
					if (i in SRAM)
						for (var j = 0; j < data[i].length; j++) SRAM[i].push(data[i][j]);
					else SRAM[i] = data[i];
				// When all shuffle has been received
				if (++state.nShuffle < worker.length) return;
				if (Object.keys(SRAM).length == 0) return;
				var res = [];
				function crossProduct(a, b) {
					var t0 = [];
					for (var i = 0; i < a.length; i++)
						for (var j = 0; j < b.length; j++)
							t0.push([a[i], b[j]]);
					return t0;
				}
				var a = SRAM[node.child[0]];
				var b = SRAM[node.child[1]];

				for (var i = 0; i < a.length; i++)
					for (var j = 0; j < b.length; j++)
						res.push({data: crossProduct(a[i], b[j])})

				SRAM = res;
			},
			distinct: function (node, data) {
				var t0 = [];
				for (var i = 0; i < data.length; i++) {
					var elements = [];
					loop:
					for (var j = 0; j < data[i].data.length; j++) {
						for (var k = 0; k < SRAM.length; k++)
							if (SRAM[k].data.indexOf(data[i].data[j]) != -1) continue loop;
						elements.push(data[i].data[j]);
					}
					if (elements.length) t0.push({data: elements});
				}
				console.log(SRAM)
				SRAM = SRAM.concat(t0);
				// When all shuffle has been received
				if (++state.nShuffle < worker.length) return;
				for (var i = 0; i < SRAM.length; i++)
					for (var j = 0; j < SRAM[i].data.length; j++)
						SRAM[i].data[j] = JSON.parse(SRAM[i].data[j]);
			}
		}

		this.shuffle = function (msg, callback) {
			try {
				rx_shuffle_lib[shuffleType](node[shuffleNum], msg.data.args);
				grid.reply(msg, null, 'Shuffle done by worker ' + msg.id);
				callback();
			} catch (err) {
				throw "Lineage rx shuffle " + shuffleType + ": " + err;
			}
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
		if (stageIdx == (stage.length - 1)) {
			SRAM = undefined;			// flush SRAM
			finalCallback(res); 		// if last stage call finalCallback
			return;
		}
		if (stage[stageIdx].state.nShuffle == worker.length) stage[++stageIdx].run(nextStage);
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
