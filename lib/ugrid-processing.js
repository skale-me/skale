'use strict';

var fs = require('fs');
var Lines = require('../lib/lines.js');
var ml = require('../lib/ugrid-ml.js');

var API = {
	fromRAM: function (STAGE_RAM, RAM, state, node, res, trans, callback, action) {
		var input = RAM[trans[0].src_id];
		var tmp = [];
		for (var p in input) {
			var partition = input[p];
			loop1:
			for (var i = 0; i < partition.length; i++) {
				tmp = [partition[i]];
				for (var t = 1; t < trans.length; t++)
					if (API[trans[t].type](trans[t], node, tmp, p, res) == false)
						continue loop1;
				action && API[action.fun](action, res, tmp, p);
			}
		}
		state.cnt++;
	},
	fromSTAGERAM: function  (STAGE_RAM, RAM, state, node, res, trans, callback, action) {
		var input = STAGE_RAM.v;
		var persistent = trans[0].persistent;
		if (persistent)
			var output = RAM[trans[0].dest_id] = {};
		var tmp = [];
		for (var p in input) {
			var partition = input[p];
			loop1:
			for (var i = 0; i < partition.length; i++) {
				tmp = [partition[i]];
				if (persistent) {
					if (output[p] == undefined)
						output[p] = [];
					output[p].push(tmp[0]);
				}
				for (var t = 1; t < trans.length; t++)
					if (API[trans[t].type](trans[t], node, tmp, p, res) == false)
						continue loop1;
				action && API[action.fun](action, res, tmp, p);
			}
		}
		state.cnt++;
	},
	parallelize : function (STAGE_RAM, RAM, state, node, res, trans, callback, action) {
		var partition = node[trans[0].num].args[0];
		var persistent = trans[0].persistent;
		if (persistent) {
			var output = RAM[trans[0].dest_id] = {};
			for (var p in partition)
				output[p] = [];
		}
		var tmp = [];		
		for (var p in partition) {
			loop1:			
			for (var i = 0; i < partition[p].length; i++) {
				tmp = [partition[p][i]];
				if (persistent)
					output[p].push(tmp[0]);
				for (var t = 1; t < trans.length; t++)
					if (API[trans[t].type](trans[t], node, tmp, p, res) == false)
						continue loop1;
				action && API[action.fun](action, res, tmp, p);
			}
		}
		state.cnt++;
	},
	randomSVMData: function (STAGE_RAM, RAM, state, node, res, trans, callback, action) {
		var num = trans[0].num;
		var D = node[num].args[0];
		var partition = node[num].args[1];
		var persistent = trans[0].persistent;
		if (persistent) {
			var output = RAM[trans[0].dest_id] = {};
			for (var p in partition)
				output[p] = [];
		}
		var tmp = [];
		var rng;
		for (var p in partition) {
			rng = new ml.Random(partition[p].seed);
			loop1:
			for (var i = 0; i < partition[p].n; i++) {
				tmp[0] = ml.randomSVMLine(rng, D);
				if (persistent)
					output[p].push(tmp[0]);
				for (var t = 1; t < trans.length; t++)
					if (API[trans[t].type](trans[t], node, tmp, p, res) == false)
						continue loop1;
				action && API[action.fun](action, res, tmp, p);
			}
		}
		state.cnt++;
	},	
	textFile: function (STAGE_RAM, RAM, state, node, res, trans, callback, action) {
		var num = trans[0].num;
		var persistent = trans[0].persistent;
		var id = trans[0].dest_id;
		var tmp = [];
		var file = node[num].args[0];
		var P = node[num].args[1];
		var partitionIdx = node[num].args[2];				
		var partitionLength = {};
		for (var p = 0; p < partitionIdx.length; p++)
			partitionLength[partitionIdx[p]] = 0;				
		if (persistent) {
			RAM[id] = {};
			for (var p = 0; p < partitionIdx.length; p++)
				RAM[id][partitionIdx[p]] = [];
		}
		var l = 0;
		state.locked = true;
		var lines = new Lines();
		fs.createReadStream(file).pipe(lines);
		lines.on("data", function(line) {
			tmp[0] = line;
			var p = l++ % P;
			if (partitionIdx.indexOf(id + '.' + p) != -1) {
				var i = partitionLength[id + '.' + p] ++;
				if (persistent) {
					if (RAM[id] == undefined)
						RAM[id] = {};
					if (RAM[id][p] == undefined)
						RAM[id][p] = [];
					RAM[id][p].push(tmp[0]);
				}
				for (var t = 1; t < trans.length; t++)
					if (API[trans[t].type](trans[t], node, tmp, p, res) == false)
						return;
				action && API[action.fun](action, res, tmp, p);				
			}
		});
		lines.on("end", function() {
			state.locked = false;
			if (state.length == ++state.cnt)
				callback(res.v);
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
	reduceByKey: function (trans, node, tmp, p, res) {
		var num = trans.num;
		for (var i = 0; i < tmp.length; i++) {
			if (res.v[tmp[i][0]] == undefined)
				res.v[tmp[i][0]] = [[tmp[i][0], JSON.parse(JSON.stringify(node[num].args[0]))]];
			res.v[tmp[i][0]][0][1] = node[num].src(res.v[tmp[i][0]][0][1], tmp[i][1]);
		}
		return true;
	},
	groupByKey: function (trans, node, tmp, p, res) {
		for (var i = 0; i < tmp.length; i++) {
			if (res.v[tmp[i][0]] == undefined) 
				res.v[tmp[i][0]] = [[tmp[i][0], []]];
			res.v[tmp[i][0]][0][1].push(tmp[i][1]);
		}
		return true;
	},
	join: function (trans, node, tmp, p, res) {
		for (var i = 0; i < tmp.length; i++) {
			var key = tmp[i][0];
			var value = tmp[i][1];
			if (res.v[key] == undefined)
				res.v[key] = {};
			if (res.v[key][trans.src_id] == undefined)
				res.v[key][trans.src_id] = [];
			res.v[key][trans.src_id].push(value);
		}
		return true;
	},
	coGroup: function (trans, node, tmp, p, res) {
		for (var i = 0; i < tmp.length; i++) {
			var key = tmp[i][0];
			var value = tmp[i][1];
			if (res.v[key] == undefined)
				res.v[key] = {};
			if (res.v[key][trans.src_id] == undefined)
				res.v[key][trans.src_id] = [];
			res.v[key][trans.src_id].push(value);
		}
		return true;
	},
	crossProduct: function (trans, node, tmp, p, res) {
		// for (var i = 0; i < tmp.length; i++) {
		// 	var key = tmp[i][0];
		// 	var value = tmp[i][1];
		// 	if (res.v[key] == undefined)
		// 		res.v[key] = {};
		// 	if (res.v[key][trans.src_id] == undefined)
		// 		res.v[key][trans.src_id] = [];
		// 	res.v[key][trans.src_id].push(value);
		// }
		return true;
	},
	mapValues : function (trans, node, tmp, p) {
		var num = trans.num;
		// ATTENTION: valable pour un mapper avec au maximum un argument
		for (var i = 0; i < tmp.length; i++)
			tmp[i][1] = node[num].src(tmp[i][1], node[num].args[0]);
		return true;
	},	
	map : function (trans, node, tmp, p) {
		var num = trans.num;
		// ATTENTION: valable pour un mapper avec au maximum un argument
		for (var i = 0; i < tmp.length; i++)
			tmp[i] = node[num].src(tmp[i], node[num].args[0]);
		return true;
	},
	// On peux eviter les copies de t0 vers tmp en utilisant un objet plutot qu'un vecteur
	// tmp = [] --> tmp = {v: []};
	// de cette façon il suffit de faire tmp.v = t0;, le GC faisant le ménage derrière	
	filter : function (trans, node, tmp, p) {
		var num = trans.num;
		var t0 = [];
		for (var i = 0; i < tmp.length; i++) {
			if (node[num].src(tmp[i]))
				t0.push(tmp[i]);
		}
		tmp = [];
		for (var i = 0; i < t0.length; i++)
			tmp[i] = t0[i];
		return tmp.length ? true : false;
	},
	flatMap : function (trans, node, tmp, p) {
		var num = trans.num;
		var t0 = [];
		for (var i = 0; i < tmp.length; i++)
			t0 = t0.concat(node[num].src(tmp[i]));
		for (var i = 0; i < t0.length; i++)
			tmp[i] = t0[i];
		return true;
	},
	// CURRENT
	sample: function (trans, node, tmp, p, res) {
		var frac = node[trans.num].args[0];
		var seed = node[trans.num].args[1];

		if (res.len == undefined)
			res.len = {};
		if (res.rng == undefined)
			res.rng = new ml.Random(seed);

		for (var i = 0; i < tmp.length; i++) {
			if (res.v[p] == undefined) {
				res.v[p] = [];
				res.len[p] = 0;
			}
			res.len[p]++; // Incremente le nombre d'elements reçus pour la partition en cours
			var current_frac = res.v[p].length / res.len[p];
			if (current_frac < frac)
				res.v[p].push(tmp[i]);
			else {
				// on tire un nombre entre 0 et res.len[p] // TODO uniformement !!!!!
				// var idx = Math.round(Math.random() * res.len[p]);
				var idx = Math.round(Math.abs(res.rng.next()) * res.len[p]);
				if (idx < res.v[p].length)
					res.v[p][idx] = tmp[i];
			}
		}

		return true;
	},
	union : function (trans, node, tmp, p) {
		return true;
	},
	count: function (action, res, tmp, p) {
		res.v += tmp.length;
	},
	collect: function (action, res, tmp, p) {
		if (res.v[p] == undefined)
			res.v[p] = [];
		for (var i = 0; i < tmp.length; i++) {
			res.v[p].push(tmp[i]);
		}
	},	
	reduce: function (action, res, tmp, p) {
		for (var i = 0; i < tmp.length; i++)
			res.v = action.src(res.v, tmp[i]);
	},
	lookup: function (action, res, tmp, p) {
		if (res.v[p] == undefined)
			res.v[p] = [];
		for (var i = 0; i < tmp.length; i++) {
			if (tmp[i][0] == action.args[0])
				res.v[p].push(tmp[i]);
		}
	}	
}

var SHUFFLE_LIB = {
	reduceByKey: function (grid, STAGE_RAM, node, msg, callback) {
		var data = msg.data.args;
		for (var p in data) {
			if (!STAGE_RAM.v[p] || !data[p]) continue;
			if (!STAGE_RAM.v[p])
				STAGE_RAM.v[p] = data[p];
			STAGE_RAM.v[p][0][1] = node.src(STAGE_RAM.v[p][0][1], data[p][0][1]);
		}
		grid.reply(msg, null, 'Shuffle done by worker ' + msg.id);
		callback();
	},
	groupByKey: function (grid, STAGE_RAM, node, msg, callback) {
		var data = msg.data.args;
		for (var p in data) {
			if (!STAGE_RAM.v[p] || !data[p]) continue;
			if (!STAGE_RAM.v[p])
				STAGE_RAM.v[p] = data[p];
			else
				STAGE_RAM.v[p][0][1] = STAGE_RAM.v[p][0][1].concat(data[p][0][1]);
		}
		grid.reply(msg, null, 'Shuffle done by worker ' + msg.id);
		callback();		
	},
	join: function (grid, STAGE_RAM, node, msg, callback) {
		var data = msg.data.args;
		for (var p in data) {
			for (var dataset in data[p]) {
				if (STAGE_RAM.v[p][dataset] == undefined)
					STAGE_RAM.v[p][dataset] = [];
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
				if (STAGE_RAM.v[p][dataset] == undefined)
					STAGE_RAM.v[p][dataset] = [];
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
			// console.log(STAGE_RAM.v[key])
			var datasets = Object.keys(STAGE_RAM.v[key]);
			if (datasets.length != 2)
				continue;
			res[key] = [];
			for (var i = 0; i < STAGE_RAM.v[key][node.child[0]].length; i++) {
				for (var j = 0; j < STAGE_RAM.v[key][node.child[1]].length; j++) {
					res[key].push([key, [STAGE_RAM.v[key][node.child[0]][i], STAGE_RAM.v[key][node.child[1]][j]]]);
				}
			}
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
	var stageIdx = 0;
	var finalCallback;
	var stage = [];

	function recompile(s) {
		var args = s.match(/\(([^)]*)/)[1];
		var body = s.replace(/^function *[^)]*\) *{/, '').replace(/}$/, '');
		return new Function(args, body);
	}

	// Build function from user code when needed
	for (var i in node) {
		if (node[i].src)
			node[i].src = recompile(node[i].src);
	}

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
			var map = worker.map(function() {
				return {};
			});
			for (var p in res)
				map[p % worker.length][p] = res[p]; // Ok if partition name is a Number for now, use hashcoding later

			for (var i = 0; i < map.length; i++) {
				if (grid.host.uuid == worker[i].uuid) {
					stage[stageIdx].state.nShuffle++;
					STAGE_RAM.v = map[i];
				} else {
					shuffleRPC(worker[i], map[i], function(err) {
						if (err) throw err;
					});
				}
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
