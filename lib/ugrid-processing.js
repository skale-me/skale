'use strict';

var fs = require('fs');
var Lines = require('../lib/lines.js');
var ml = require('../lib/ugrid-ml.js');

function pipeline (trans, fun, node, tmp, p, res, RAM, action, fun_action) {
	for (var t = 1; t < trans.length; t++) {
		if (fun[t](trans[t], node, tmp, p, res) == false) return;
		if (trans[t].persistent && (trans[t].dependency == 'narrow')) {
			if (RAM[trans[t].dest_id] == undefined) RAM[trans[t].dest_id] = {};
			if (RAM[trans[t].dest_id][p] == undefined) RAM[trans[t].dest_id][p] = tmp.v;
			else RAM[trans[t].dest_id][p] = RAM[trans[t].dest_id][p].concat(tmp.v);
		}
	}

	action && fun_action(action, res, tmp, p);
}

var API = {
	fromRAM: function (STAGE_RAM, RAM, state, node, res, trans, callback, action) {
		var fun = [];
		for (var t = 1; t < trans.length; t++) fun[t] = API[trans[t].type];
		var fun_action = action ? API[action.fun] : null;
		var tmp = {v : []};

		var input = RAM[trans[0].src_id];
		for (var p in input) {
			var partition = input[p];
			for (var i = 0; i < partition.length; i++) {
				tmp.v = [partition[i]];
				pipeline(trans, fun, node, tmp, p, res, RAM, action, fun_action);
			}
		}
		state.cnt++;
	},
	fromSTAGERAM: function  (STAGE_RAM, RAM, state, node, res, trans, callback, action) {
		var fun = [];
		for (var t = 1; t < trans.length; t++) fun[t] = API[trans[t].type];
		var fun_action = action ? API[action.fun] : null;
		var tmp = {v : []};

		var input = STAGE_RAM.v;
		var persistent = trans[0].persistent;
		if (persistent) var output = RAM[trans[0].dest_id] = {};
		for (var p in input) {
			var partition = input[p];
			for (var i = 0; i < partition.length; i++) {
				tmp.v = [partition[i]];
				if (persistent) {
					if (output[p] == undefined) output[p] = [];
					output[p].push(partition[i]);
				}
				pipeline(trans, fun, node, tmp, p, res, RAM, action, fun_action);				
			}
		}
		state.cnt++;
	},
	parallelize : function (STAGE_RAM, RAM, state, node, res, trans, callback, action) {
		var fun = [];
		for (var t = 1; t < trans.length; t++) fun[t] = API[trans[t].type];
		var fun_action = action ? API[action.fun] : null;
		var tmp = {v : []};

		var partition = node[trans[0].num].args[0];
		var persistent = trans[0].persistent;
		if (persistent) {
			var output = RAM[trans[0].dest_id] = {};
			for (var p in partition) output[p] = [];
		}
		for (var p in partition) {
			for (var i = 0; i < partition[p].length; i++) {
				tmp.v = [partition[p][i]];
				if (persistent) output[p].push(tmp.v[0]);
				pipeline(trans, fun, node, tmp, p, res, RAM, action, fun_action);
			}
		}
		state.cnt++;
	},
	randomSVMData: function (STAGE_RAM, RAM, state, node, res, trans, callback, action) {
		var fun = [];
		for (var t = 1; t < trans.length; t++) fun[t] = API[trans[t].type];
		var fun_action = action ? API[action.fun] : null;
		var tmp = {v : []};

		var num = trans[0].num;
		var D = node[num].args[0];
		var partition = node[num].args[1];
		var persistent = trans[0].persistent;
		if (persistent) {
			var output = RAM[trans[0].dest_id] = {};
			for (var p in partition) output[p] = [];
		}
		for (var p in partition) {
			var rng = new ml.Random(partition[p].seed);
			for (var i = 0; i < partition[p].n; i++) {
				tmp.v = [ml.randomSVMLine(rng, D)];
				if (persistent) output[p].push(tmp.v[0]);
				pipeline(trans, fun, node, tmp, p, res, RAM, action, fun_action);
			}
		}
		state.cnt++;
	},
	textFile: function (STAGE_RAM, RAM, state, node, res, trans, callback, action) {
		var fun = [];
		for (var t = 1; t < trans.length; t++) fun[t] = API[trans[t].type];
		var fun_action = action ? API[action.fun] : null;
		var tmp = {v : []};

		var num = trans[0].num;
		var persistent = trans[0].persistent;
		var dest_id = trans[0].dest_id;
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
				pipeline(trans, fun, node, tmp, p, res, RAM, action, fun_action);
			}
		});
		lines.on("end", function() {
			state.locked = false;
			if (state.length == ++state.cnt) callback(res.v);
		});
	},
	// hdfsTextFile: function (blockIdx, num, persistent, id, stageIdx) {
	// 	var file = node[num].args[0][blockIdx].file;
	// 	var partitionIdx = [node[num].args[0][blockIdx].blockNum];
	// 	var partitionLength = {};
	// 	for (var p = 0; p < partitionIdx.length; p++)
	// 		partitionLength[partitionIdx[p]] = 0;
	// 	if (persistent) {
	// 		RAM[id] = {};
	// 		for (var p = 0; p < partitionIdx.length; p++)
	// 			RAM[id][partitionIdx[id + '.' + p]] = [];
	// 	}
	// 	stage_locked[stageIdx] = true;
	// 	var lines = new Lines();
	// 	fs.createReadStream(file).pipe(lines);

	// 	var skipFirstLine = (node[num].args[0][blockIdx].blockNum == 0) ? false : true;
	// 	var shuffleLastLine = (node[num].args[0][blockIdx].blockNum == (node[num].args[0].length - 1)) ? false : true;
	// 	var lastline, firstline, tmp;

	// 	function processLine(line) {
	// 		lastline = line;
	// 		lines.on("data", function (line) {							
	// 			var i = partitionLength[partitionIdx[0]]++;
	// 			tmp = lastline;
	// 			"PIPELINE_HERE"
	// 			lastline = line;
	// 		});
	// 	}

	// 	// Skip first line PIPELINE and introduce one line delay
	// 	if (skipFirstLine) {
	// 		console.log('skipping first line of block ' + node[num].args[0][blockIdx].blockNum)
	// 		lines.once("data", function (line) {
	// 			// do something with the first line here
	// 			firstline = line;
	// 			lines.once("data", processLine);
	// 		});
	// 	} else
	// 		lines.once("data", processLine);

	// 	lines.on("end", function() {
	// 		// Shuffle last line if needed and continue to next block
	// 		if (shuffleLastLine)
	// 			console.log('block: ' + node[num].args[0][blockIdx].blockNum + ', Need to shuffle ' + lastline);

	// 		stage_locked[stageIdx] = false;
	// 		if (++blockIdx < node[num].args[0].length) {
	// 			hdfsTextFile(blockIdx, num, persistent, id, stageIdx);
	// 		} else if (stage_length[stageIdx] == ++stage_cnt[stageIdx])
	// 			callback(res.v);
	// 	});
	// },
	// WARNING: only valid for one additional user parameter
	map: function (trans, nodes, tmp, p) {
		var node = nodes[trans.num];
		var data = tmp.v;
		for (var i = 0; i < data.length; i++)
			data[i] = node.src(data[i], node.args[0]);
	},
	filter : function (trans, nodes, tmp, p) {
		var node = nodes[trans.num];
		var data = tmp.v;
		var t0 = [];
		for (var i = 0; i < data.length; i++)
			if (node.src(data[i])) t0.push(data[i]);
		tmp.v = t0;
		return (data.length > 0);
	},
	flatMap : function (trans, nodes, tmp, p) {
		var node = nodes[trans.num];
		var data = tmp.v;
		var t0 = [];
		for (var i = 0; i < data.length; i++) t0 = t0.concat(node.src(data[i]));
		tmp.v = t0;
	},
	sample: function (trans, node, tmp, p, res) {
		var num = trans.num;
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
	},
	groupByKey: function (trans, nodes, tmp, p, res) {		
		var data = tmp.v;
		var dest = res.v;		
		for (var i = 0; i < data.length; i++) {
			var key = data[i][0];
			if (dest[key] == undefined) dest[key] = [[key, []]];
			dest[key][0][1].push(data[i][1]);
		}
	},
	reduceByKey: function (trans, nodes, tmp, p, res) {
		var node = nodes[trans.num];
		var data = tmp.v;
		var dest = res.v;
		var initVal = node.args[0];
		var reduce = node.src;
		for (var i = 0; i < data.length; i++) {
			var key = data[i][0];
			if (dest[key] == undefined)
				dest[key] = [[key, JSON.parse(JSON.stringify(initVal))]];
			dest[key][0][1] = reduce(dest[key][0][1], data[i][1]);
		}
	},
	union : function (trans, nodes, tmp, p) {},
	join: function (trans, nodes, tmp, p, res) {
		var data = tmp.v;
		var dest = res.v;
		var src_id = trans.src_id;
		for (var i = 0; i < data.length; i++) {
			var key = data[i][0];
			if (dest[key] == undefined) dest[key] = {};
			if (dest[key][src_id] == undefined) dest[key][src_id] = [];
			dest[key][src_id].push(data[i][1]);
		}
	},
	coGroup: function (trans, nodes, tmp, p, res) {
		var data = tmp.v;
		var dest = res.v;
		var src_id = trans.src_id;
		for (var i = 0; i < data.length; i++) {
			var key = data[i][0];
			if (dest[key] == undefined) dest[key] = {};
			if (dest[key][src_id] == undefined) dest[key][src_id] = [];
			dest[key][src_id].push(data[i][1]);
		}
	},
	crossProduct: function (trans, nodes, tmp, p, res) {
		// for (var i = 0; i < tmp.length; i++) {
		// 	var key = tmp[i][0];
		// 	var value = tmp[i][1];
		// 	if (res.v[key] == undefined)
		// 		res.v[key] = {};
		// 	if (res.v[key][trans.src_id] == undefined)
		// 		res.v[key][trans.src_id] = [];
		// 	res.v[key][trans.src_id].push(value);
		// }
	},
	mapValues : function (trans, nodes, tmp, p) {
		// ATTENTION: valable pour un mapper avec au maximum un argument		
		var num = trans.num;
		var data = tmp.v;
		var mapper = nodes[num].src;
		var arg0 = nodes[num].args[0];
		for (var i = 0; i < data.length; i++) 
			data[i][1] = mapper(data[i][1], arg0);
	},
	count: function (action, res, tmp, p) {res.v += tmp.v.length;},
	collect: function (action, res, tmp, p) {
		var data = tmp.v;
		if (res.v[p] == undefined) res.v[p] = [];
		var dest = res.v[p];
		for (var i = 0; i < data.length; i++) 
			dest.push(data[i]);
	},
	reduce: function (action, res, tmp, p) {
		var reduce = action.src;
		var data = tmp.v;
		for (var i = 0; i < data.length; i++)
			res.v = reduce(res.v, data[i]);
	},
	lookup: function (action, res, tmp, p) {
		var data = tmp.v;
		if (res.v[p] == undefined) res.v[p] = [];
		var dest = res.v[p];
		var arg0 = action.args[0];
		for (var i = 0; i < data.length; i++)
			if (data[i][0] == arg0) dest.push(data[i]);
	}
}

var SHUFFLE_LIB = {
	groupByKey: function (grid, STAGE_RAM, node, msg, callback) {
		var data = msg.data.args;
		for (var p in data) {
			if (STAGE_RAM.v[p] == undefined) STAGE_RAM.v[p] = data[p];
			else STAGE_RAM.v[p][0][1] = STAGE_RAM.v[p][0][1].concat(data[p][0][1]);
		}
		grid.reply(msg, null, 'Shuffle done by worker ' + msg.id);
		callback();
	},
	reduceByKey: function (grid, STAGE_RAM, node, msg, callback) {
		var data = msg.data.args;
		// for (var p in data) {
		// 	if (!STAGE_RAM.v[p] || !data[p]) continue;
		// 	if (!STAGE_RAM.v[p]) STAGE_RAM.v[p] = data[p];
		// 	STAGE_RAM.v[p][0][1] = node.src(STAGE_RAM.v[p][0][1], data[p][0][1]);
		// }
		for (var p in data) {
			if (STAGE_RAM.v[p] == undefined) STAGE_RAM.v[p] = data[p];
			else STAGE_RAM.v[p][0][1] = node.src(STAGE_RAM.v[p][0][1], data[p][0][1]);
		}		
		grid.reply(msg, null, 'Shuffle done by worker ' + msg.id);
		callback();
	},
	join: function (grid, STAGE_RAM, node, msg, callback) {
		var data = msg.data.args;
		for (var p in data) {
			for (var dataset in data[p]) {
				if (STAGE_RAM.v[p][dataset] == undefined) STAGE_RAM.v[p][dataset] = [];
				STAGE_RAM.v[p][dataset] = STAGE_RAM.v[p][dataset].concat(data[p][dataset]);
			}
		}
		grid.reply(msg, null, 'Shuffle done by worker ' + msg.id);
		callback();
	},
	coGroup: function (grid, STAGE_RAM, node, msg, callback) {
		var data = msg.data.args;
		for (var p in data) {
			for (var dataset in data[p]) {
				if (STAGE_RAM.v[p][dataset] == undefined) STAGE_RAM.v[p][dataset] = [];
				STAGE_RAM.v[p][dataset] = STAGE_RAM.v[p][dataset].concat(data[p][dataset]);
			}
		}
		grid.reply(msg, null, 'Shuffle done by worker ' + msg.id);
		callback();
	}	
}

var PRE_SHUFFLE_LIB = {
	sample: function (res, STAGE_RAM, node) {
		STAGE_RAM.v = res;
		return false;
	}
}

var POST_SHUFFLE_LIB = {
	join: function (STAGE_RAM, node) {
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
	},
	coGroup: function (STAGE_RAM, node) {
		var res = {};
		for (var key in STAGE_RAM.v) {
			var datasets = Object.keys(STAGE_RAM.v[key]);
			if (datasets.length != 2)
				continue;
			res[key] = [[key, [STAGE_RAM.v[key][node.child[0]], STAGE_RAM.v[key][node.child[1]]]]];
		}
		STAGE_RAM.v = res;
	}	
}

function Stage(grid, STAGE_RAM, RAM, stageData, worker, node) {
	var lineages = stageData.lineages; 	// Lineages vector
	var state = this.state = {
		cnt: 0,							// Number of finished lineages
		locked: false,					// Stage is locked because of an asynchonous lineage
		length: lineages.length,
		nShuffle: 0
	}
	var action = stageData.action;
	var shuffleType = stageData.shuffleType;
	var shuffleNum = stageData.shuffleNum;

	this.run = function (callback) {
		var res = action ? {v: action.init} : {v: {}};
		for (var l = 0; l < lineages.length; l++) {
			try {
				API[lineages[l][0].type](STAGE_RAM, RAM, state, node, res, lineages[l], callback, action);
			} catch (e) {
				throw 'Failed to run lineage ' + l;
			}
		}
		if (!state.locked && (lineages.length == state.cnt))
			callback(res.v);	// Call poststage
	}
	this.preShuffle = function (res) {
		if (PRE_SHUFFLE_LIB[shuffleType])
			return PRE_SHUFFLE_LIB[shuffleType](res, STAGE_RAM, node[shuffleNum]);
		return true;
	}
	this.shuffle = function (msg, callback) {
		SHUFFLE_LIB[shuffleType](grid, STAGE_RAM, node[shuffleNum], msg, callback);
	}
	this.postShuffle = function (msg, callback) {
		POST_SHUFFLE_LIB[shuffleType] && POST_SHUFFLE_LIB[shuffleType](STAGE_RAM, node[shuffleNum]);
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
			for (var p in res) map[p % worker.length][p] = res[p];

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
