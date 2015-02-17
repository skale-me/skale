var fs = require('fs');
var Lines = require('../lib/lines.js');
var ml = require('../lib/ugrid-ml.js');

// La persistence ne doit etre effecuée que sur les narrow dependencies 
var API = {
	fromRAM: function (STAGE_RAM, RAM, state, node, res, trans, num, persistent, id, callback, action) {
		var input = RAM[id], tmp = [];
		for (var p in input) {
			loop1:
			for (var i = 0; i < input[p].length; i++) {
				tmp = [input[p][i]];
				for (var t = 1; t < trans.length; t++)
					if (API[trans[t].type](node[trans[t].num], tmp, res) == false)
						continue loop1;
				action && API[action.fun](action, res, tmp, i, p);
			}
		}
		if (action && action.post_src) {
			var tmp = {N: res.v.length, data: action.post_src(res.v, action.args[0], action.args[1])};
			res.v = tmp;
		}		
		state.cnt++;
	},
	fromSTAGERAM: function  (STAGE_RAM, RAM, state, node, res, trans, num, persistent, id, callback, action) {
		var input = STAGE_RAM.v, tmp = [];
		for (var p in input) {
			loop1:
			for (var i = 0; i < input[p].length; i++) {
				tmp = [input[p][i]];
				// if (persistent) {
				// 	if (RAM[id] == undefined)
				// 		RAM[id] = {};
				// 	if (RAM[id][p] == undefined)
				// 		RAM[id][p] = [];
				// 	RAM[id][p].push(tmp.v);
				// }
				for (var t = 1; t < trans.length; t++)
					if (API[trans[t].type](node[trans[t].num], tmp, res) == false)
						continue loop1;
				action && API[action.fun](action, res, tmp, i, p);
			}
		}
		if (action && action.post_src) {
			var tmp = {N: res.v.length, data: action.post_src(res.v, action.args[0], action.args[1])};
			res.v = tmp; 
		}
		state.cnt++;
	},
	parallelize : function (STAGE_RAM, RAM, state, node, res, trans, num, persistent, id, callback, action) {	// Ok pour flatMap.js
		var tmp = [];
		var partition = node[num].args[0];
		if (persistent) {
			RAM[id] = {};
			for (var p in partition) {
				RAM[id][p] = [];
			}
		}
		for (var p in partition) {
			loop1:			
			for (var i = 0; i < partition[p].length; i++) {
				tmp = [partition[p][i]];
				if (persistent) {
					if (RAM[id] == undefined)
						RAM[id] = {};
					if (RAM[id][p] == undefined)
						RAM[id][p] = [];
					RAM[id][p].push(tmp[0]);
				}
				// Loop over transformations
				for (var t = 1; t < trans.length; t++)
					if (API[trans[t].type](node[trans[t].num], tmp, res) == false)
						continue loop1;
				action && API[action.fun](action, res, tmp, i, p);
			}
		}
		if (action && action.post_src) {
			var tmp = {N: res.v.length, data: action.post_src(res.v, action.args[0], action.args[1])};
			res.v = tmp; 
		}
		state.cnt++;
	},
	textFile: function (STAGE_RAM, RAM, state, node, res, trans, num, persistent, id, callback, action) {
		// var tmp = {v: undefined};
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
					if (API[trans[t].type](node[trans[t].num], tmp, res) == false)
						return;
				action && API[action.fun](action, res, tmp, i, p);				
			}
		});
		lines.on("end", function() {
			state.locked = false;
			if (state.length == ++state.cnt) {
				if (action && action.post_src) {
					var tmp = {N: res.v.length, data: action.post_src(res.v, action.args[0], action.args[1])};
					res.v = tmp; 
				}
				callback(res.v);
			}
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
	randomSVMData: function (STAGE_RAM, RAM, state, node, res, trans, num, persistent, id, callback, action) {
		var tmp = [], p, i, rng;
		var D = node[num].args[0];
		var partition = node[num].args[1];
		if (persistent) {
			RAM[id] = {};
			for (p in partition) 
				RAM[id][p] = [];
		}
		for (p in partition) {
			rng = new ml.Random(partition[p].seed);
			loop1:
			for (i = 0; i < partition[p].n; i++) {
				tmp[0] = ml.randomSVMLine(rng, D);
				if (persistent) {
					if (RAM[id] == undefined)
						RAM[id] = {};
					if (RAM[id][p] == undefined)
						RAM[id][p] = [];
					RAM[id][p].push(tmp[0]);
				}
				for (var t = 1; t < trans.length; t++)
					if (API[trans[t].type](node[trans[t].num], tmp, res) == false)
						continue loop1;
				action && API[action.fun](action, res, tmp, i, p);
			}
		}
		if (action && action.post_src) {
			var tmp = {N: res.v.length, data: action.post_src(res.v, action.args[0], action.args[1])};
			res.v = tmp; 
		}		
		state.cnt++;
	},	
	map : function (node, tmp) {
		for (var i = 0; i < tmp.length; i++)
			tmp[i] = node.src(tmp[i]);
		// for (var i = 0; i < tmp.length; i++)
		// 	tmp[i] = node.src(tmp[i], node.args[0]);
		return true;
	},
	reduceByKey: function (node, tmp, res) {
		for (var i = 0; i < tmp.length; i++) {
			if (res.v[tmp[i][0]] == undefined)
				res.v[tmp[i][0]] = [[tmp[i][0], JSON.parse(JSON.stringify(node.args))]];
			res.v[tmp[i][0]][0][1] = node.src(res.v[tmp[i][0]][0][1], tmp[i][1]);
		}
		return true;
	},
	groupByKey: function (node, tmp, res) {
		for (var i = 0; i < tmp.length; i++) {
			if (res.v[tmp[i][0]] == undefined) 
				res.v[tmp[i][0]] = [[tmp[i][0], []]];
			res.v[tmp[i][0]][0][1].push(tmp[i][1]);
		}
		return true;
	},
	// On peux eviter les copies de t0 vers tmp en utilisant un objet plutot qu'un vecteur
	// tmp = [] --> tmp = {v: []};
	// de cette façon il suffit de faire tmp.v = t0;, le GC faisant le ménage derrière
	flatMap : function (node, tmp) {
		var t0 = [];
		for (var i = 0; i < tmp.length; i++)
			t0 = t0.concat(node.src(tmp[i]));
		for (var i = 0; i < t0.length; i++)
			tmp[i] = t0[i];
		return true;
	},
	filter : function (node, tmp) {
		var t0 = [];
		for (var i = 0; i < tmp.length; i++) {
			if (node.src(tmp[i]))
				t0.push(tmp[i]);
		}
		tmp = [];
		for (var i = 0; i < t0.length; i++)
			tmp[i] = t0[i];
		return tmp.length ? true : false;
	},
	union : function (node, tmp) {
		return;
	},
	collect: function (action, res, tmp, idx, p) {
		if (res.v[p] == undefined)
			res.v[p] = [];
		for (var i = 0; i < tmp.length; i++) {
			res.v[p].push(tmp[i]);
		}
	},
	sample: function (action, res, tmp, idx, p) {
		for (var i = 0; i < tmp.length; i++)
			res.v.push(tmp[i]);
	},
	count: function (action, res, tmp, idx, p) {
		res.v++;
	},
	reduce: function (action, res, tmp, idx, p) {
		for (var i = 0; i < tmp.length; i++)
			res.v = action.src(res.v, tmp[i]);
	}
}

function Stage(STAGE_RAM, RAM, stageData, node) {
	var lineages = stageData.lineages; 	// Lineages vector
	var state = {
		cnt: 0,							// Number of finished lineages
		locked: false,					// Stage is locked because of an asynchonous lineage
		length: lineages.length
	}
	var action = stageData.action; 

	// Build function from user code when needed
	for (var i in node) {
		if (node[i].src)
			node[i].src = eval('node[i].src = ' + node[i].src);
	}
	if (action && action.src)
		action.src = eval('action.src = ' + action.src);
	if (action && action.post_src)
		action.post_src = eval('action.post_src = ' + action.post_src);

	this.run = function (callback) {
		var res = action ? {v: action.init} : {v: {}};
		for (var l = 0; l < lineages.length; l++) {
			try {
				API[lineages[l][0].type](STAGE_RAM, RAM, state, node, res, lineages[l], lineages[l][0].num, lineages[l][0].persistent, lineages[l][0].id, callback, action);
			} catch (e) {
				console.log(lineages[l]);
				throw 'run lineage failed';
			}
		}
		if (!state.locked && (lineages.length == state.cnt))
			callback(res.v);
	}
}

function UgridTask(grid, STAGE_RAM, RAM, msg) {
	var node = msg.data.args.node;
	var action = msg.data.args.action;
	var worker = msg.data.args.workerData;	
	var stageIdx = 0;
	var nShuffle = 0;
	var finalCallback;
	var stage = [];

	for (var i = 0; i < msg.data.args.stageData.length; i++)
		stage[i] = new Stage(STAGE_RAM, RAM, msg.data.args.stageData[i], node);	

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
			// Map partitions to workers
			var map = worker.map(function() {
				return {};
			});
			for (var p in res)
				map[p % worker.length][p] = res[p]; // Ok if partition name is a Number for now, use hashcoding later

			for (var i = 0; i < map.length; i++) {
				if (grid.host.uuid == worker[i].uuid) {
					nShuffle++;
					STAGE_RAM.v = map[i];
				} else {
					shuffleRPC(worker[i], map[i], function(err) {
						if (err) throw err;
					});
				}
			}
			// Run next stage if needed
			if (worker.length == 1) {
				nShuffle = 0;
				stage[++stageIdx].run(postStage);
			}
		}
	}

	this.run = function(callback) {
		finalCallback = callback;
		stage[stageIdx].run(postStage);
	};

	this.processShuffle = function(msg) {
		stage[stageIdx].shuffle();
	};
}

module.exports.UgridTask = UgridTask;