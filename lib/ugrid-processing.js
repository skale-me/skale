'use strict';

var ml = require('./ugrid-ml.js');
var sources = require('./ugrid-sources.js');
var rdd = require('./ugrid-transformation.js');

module.exports.UgridTask = function(grid, RAM, msg) {
	var node = msg.data.args.node;
	var worker = msg.data.args.workerData;
	var stageData = msg.data.args.stageData;
	var action = stageData[stageData.length - 1].action;	
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

	if (action.src) {
		action.ml = ml;
		action.src = recompile(action.src);
	}

	for (var i = 0; i < stageData.length; i++)
		stage[i] = new Stage(worker, grid, node, RAM, stageData[i]);

	function nextStage() {
		if (stage[stageIdx].state.nShuffle == worker.length)
			stage[++stageIdx].run(nextStage);
	}

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

	this.processLastLine = function(msg) {
		stage[stageIdx].processLastLine(msg);
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
	var lineage = [];

	for (var l = 0; l < lineages.length; l++)
		lineage[l] = new sources[lineages[l][0].type](l, grid, worker, state, node, RAM, lineages[l], action, fun_action);

	this.run = function (callback) {
		for (var l = 0; l < lineage.length; l++) {
			try {lineage[l].run(callback);}
			catch (err) {throw "Lineage error, " + transform[0].type + ": " + err;}
		}
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

	this.processLastLine = function (msg) {
		lineage[msg.data.args.lid].processLastLine(msg.data.args.lastLine);
	}
}
