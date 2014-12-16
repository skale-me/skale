
worker[0] = {"uuid":"7fa0ec40-852e-11e4-bfbe-a7718a7b37f0","id":3};
stage[0] = function() {
var res = {};
var input = RAM[924208360];
for (var p in input) {for (var i = 0; i < input[p].length; i++) {var tmp = input[p][i];		tmp = transform[1](tmp, node[1].args[1][0]);
if (tmp[node[2].args[0]] == undefined)
	throw "key unknown by worker"
if (res[tmp[node[2].args[0]]] == undefined)
	res[tmp[node[2].args[0]]] = [JSON.parse(JSON.stringify(node[2].args[2]))];
res[tmp[node[2].args[0]]] = [transform[2](res[tmp[node[2].args[0]]][0], tmp)];
}}return res;
}
stage[1] = function() {
var res = {};
var input = STAGE_RAM; for (var p in input) {for (var i = 0; i < input[p].length; i++) {var tmp = input[p][i];"PIPELINE HERE"}}return res;
}


	function shuffleRPC(host, args, callback) {
		grid.request_cb(host, {cmd: 'shuffle', args: args}, callback);
	}

	function runStage(idx) {
		var lastStage = (stageIdx == (stage.length - 1));
		var res = stage[idx]();

		if (lastStage) {
			STAGE_RAM = [];
			finalCallback(res);
		} else {
			// Map partitions to workers			
			var map = worker.map(function(n) {return {};});
			for (var p in res)				
				map[p % worker.length][p] = res[p]; // Ok if partition name is a Number for now, use hashcoding later

			for (var i = 0; i < map.length; i++) {
				if (grid.host.uuid == worker[i].uuid) {
					nShuffle++;
					STAGE_RAM = map[i];
				} else
					shuffleRPC(worker[i], map[i], function(err, res) {if (err) throw err;});
			}
			// Run next stage if needed
			if (worker.length == 1) {
				nShuffle = 0;
				runStage(++stageIdx);
			}
		}
	}

	this.run = function(callback) {
		finalCallback = callback;
		runStage(0);
	}

	this.processShuffle = function(msg) {
		nShuffle++;		
		var data = msg.data.args;
		// Reduce intermediates results (to be generated programmatically)
		for (p in data) {
			if (!STAGE_RAM[p] || !data[p]) continue;
			for (var i = 0; i < STAGE_RAM[p][0].acc.length; i++)
				STAGE_RAM[p][0].acc[i] += data[p][0].acc[i];
			STAGE_RAM[p][0].sum += data[p][0].sum;
		}

		msg.cmd = 'reply';
		msg.data = 'Shuffle done by worker ' + msg.id;
		msg.id = msg.from;		
		grid.send_cb(msg, function(err, res) {if (err) throw err;});

		if (nShuffle == worker.length) {
			nShuffle = 0;
			runStage(++stageIdx);
		}
	}
}
