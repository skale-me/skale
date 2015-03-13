'use strict';

var fs = require('fs');
var Lines = require('../lib/lines.js');
var ml = require('../lib/ugrid-ml.js');
var rdd = require('./ugrid-transformation.js');

function UgridTask(grid, RAM, msg) {
	var node = msg.data.args.node;
	var worker = msg.data.args.workerData;
	var stageData = msg.data.args.stageData;	
	var stageIdx = 0, stage = [];

	function recompile(s) {
		var args = s.match(/\(([^)]*)/)[1];
		var body = s.replace(/^function *[^)]*\) *{/, '').replace(/}$/, '');
		return new Function(args, body);
	}

	for (var i in node) {
		if (node[i].src) node[i].src = recompile(node[i].src);
		if (node[i].transform)
			node[i].transform = new rdd[node[i].transform](grid, node[i], worker);		
	}

	var action = stageData[stageData.length - 1].action;
	if (action.src) {
		action.ml = ml;
		action.src = recompile(action.src);
	}

	function nextStage() {
		if (stage[stageIdx].state.nShuffle == worker.length) 
			stage[++stageIdx].run(nextStage);
	}

	for (var i = 0; i < stageData.length; i++)
		stage[i] = new Stage(worker, grid, node, RAM, stageData[i]);

	this.run = function(callback) {
		action.callback = callback;
		stage[stageIdx].run(nextStage);
	};

	this.processShuffle = function(msg) {
		stage[stageIdx].shuffle(msg, nextStage);
	};

	this.processAction = function(msg) {
		stage[stage.length - 1].processAction(msg);
	};
}

function Stage(worker, grid, node, RAM, stageData) {
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
	var fun_action = action ? new rdd[action.fun](grid, action) : null;

	this.run = function (callback) {
		for (var l = 0; l < lineages.length; l++) 
			new Lineage(grid, worker, state, node, RAM, lineages[l], action, fun_action, callback);
	}

	this.processAction = function (msg) {
		for (var wid = 0; wid < worker.length; wid++)
			if (worker[wid].uuid == grid.host.uuid) break;
		if (action.finished) {
			action.callback(fun_action.result);
			if (worker[wid + 1])
				grid.request_cb(worker[wid + 1], {cmd: 'action', args: 'TEST'}, function(err) {if (err) throw err;});
		}
		action.unlocked = true;
	}

	this.shuffle = function (msg, callback) {
		try {
			node[shuffleNum].transform.rx_shuffle(msg.data.args, state);
			grid.reply(msg, null, 'Shuffle done by worker ' + msg.id);
			callback();
		} catch (err) {throw "Lineage rx shuffle " + shuffleType + ": " + err;}
	}
}

function Lineage (grid, worker, state, node, RAM, transform, action, fun_action, callback) {
	var tmp = [], partitionMapper = {};

	function save (t) {
		var dest_id = transform[t].dest_id;
		if (partitionMapper[dest_id] == undefined) {			// le dataset dest_id n'existe pas, on le crée
			partitionMapper[dest_id] = [];						// Nouveau vecteur associé au lineage
			partitionMapper[dest_id][t] = 0;
			RAM[dest_id] = [{data: []}];						// nouveau vecteur de partition dans la RAM
		} else if (partitionMapper[dest_id][t] == undefined) {
			partitionMapper[dest_id][t] = RAM[dest_id].length;
			RAM[dest_id].push({data: []});						// la partition n'existe pas on la crée
		}
		// on récupère l'indice de la partition dans laquelle stocker les datas
		var idx = partitionMapper[dest_id][t];
		var t0 = RAM[dest_id][idx].data;
		var L = t0.length;
		for (var i = 0; i < tmp.length; i++) t0[L + i] = tmp[i];
	}

	function pipeline (p) {
		for (var t = 1; t < transform.length; t++) {					
			tmp = node[transform[t].num].transform.pipeline(tmp, p, transform[t].src_id);					
			if (tmp && (tmp.length == 0)) return;
			if (transform[t].persistent && (transform[t].dependency == 'narrow'))
				save(t);
		}
		action && fun_action.pipeline(tmp, p);
	}

	var source = {
		fromRAM: function () {
			var input = RAM[transform[0].src_id] || [];
			for (var p = 0; p < input.length; p++) {
				var partition = input[p].data;
				for (var i = 0; i < partition.length; i++) {
					tmp = [partition[i]];
					pipeline(p);
				}
			}
			if (state.locked || (++state.cnt < state.target_cnt)) return;
			if (fun_action) {
				action.finished = true;
				for (var wid = 0; wid < worker.length; wid++)
					if (worker[wid].uuid == grid.host.uuid) break;
				if ((wid == 0) || action.unlocked) {
					action.callback(fun_action.result);
					if (worker[wid + 1])
						grid.request_cb(worker[wid + 1], {cmd: 'action', args: 'TEST'}, function(err) {if (err) throw err;});
				}
			} else {
				try {
					node[transform[transform.length - 1].num].transform.tx_shuffle(state);
					callback();
				} catch (err) {
					throw "Lineage tx shuffle " + transform[transform.length - 1].type + ": " + err;
				}
			}
		},
		fromSTAGERAM: function () {
			var input = node[transform[0].num].transform.SRAM || [];
			var persistent = transform[0].persistent;
			for (var p = 0; p < input.length; p++) {
				var partition = input[p].data;
				for (var i = 0; i < partition.length; i++) {
					tmp = [partition[i]];
					if (persistent) save(0);
					pipeline(p);
				}
			}
			if (state.locked || (++state.cnt < state.target_cnt)) return;
			if (fun_action) {
				action.finished = true;
				for (var wid = 0; wid < worker.length; wid++)
					if (worker[wid].uuid == grid.host.uuid) break;
				if ((wid == 0) || action.unlocked) {
					action.callback(fun_action.result);
					if (worker[wid + 1])
						grid.request_cb(worker[wid + 1], {cmd: 'action', args: 'TEST'}, function(err) {if (err) throw err;});
				}
			} else {
				try {
					node[transform[transform.length - 1].num].transform.tx_shuffle(state);
					callback();
				} catch (err) {
					throw "Lineage tx shuffle " + transform[transform.length - 1].type + ": " + err;
				}
			}
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
			if (state.locked || (++state.cnt < state.target_cnt)) return;
			if (fun_action) {
				action.finished = true;
				for (var wid = 0; wid < worker.length; wid++)
					if (worker[wid].uuid == grid.host.uuid) break;
				if ((wid == 0) || action.unlocked) {
					action.callback(fun_action.result);
					if (worker[wid + 1])
						grid.request_cb(worker[wid + 1], {cmd: 'action', args: 'TEST'}, function(err) {if (err) throw err;});
				}
			} else {
				try {
					node[transform[transform.length - 1].num].transform.tx_shuffle(state);
					callback();
				} catch (err) {
					throw "Lineage tx shuffle " + transform[transform.length - 1].type + ": " + err;
				}
			}
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
			if (state.locked || (++state.cnt < state.target_cnt))
				return;
			if (fun_action) {
				action.finished = true;
				for (var wid = 0; wid < worker.length; wid++)
					if (worker[wid].uuid == grid.host.uuid) break;
				if ((wid == 0) || action.unlocked) {
					action.callback(fun_action.result);
					if (worker[wid + 1])
						grid.request_cb(worker[wid + 1], {cmd: 'action', args: 'TEST'}, function(err) {if (err) throw err;});
				}
			} else {
				try {
					node[transform[transform.length - 1].num].transform.tx_shuffle(state);
					callback();
				} catch (err) {
					throw "Lineage tx shuffle " + transform[transform.length - 1].type + ": " + err;
				}
			}
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
				if (state.locked || (++state.cnt < state.target_cnt))
					return;
				if (fun_action) {
					action.finished = true;
					for (var wid = 0; wid < worker.length; wid++)
						if (worker[wid].uuid == grid.host.uuid) break;
					if ((wid == 0) || action.unlocked) {
						action.callback(fun_action.result);
						if (worker[wid + 1])
							grid.request_cb(worker[wid + 1], {cmd: 'action', args: 'TEST'}, function(err) {if (err) throw err;});
					}
				} else {
					try {
						node[transform[transform.length - 1].num].transform.tx_shuffle(state);
						callback();
					} catch (err) {
						throw "Lineage tx shuffle " + transform[transform.length - 1].type + ": " + err;
					}
				}
			});
		}
	}

	try {source[transform[0].type]();}
	catch (err) {throw "Lineage error, " + transform[0].type + ": " + err;}
}

module.exports.UgridTask = UgridTask;
