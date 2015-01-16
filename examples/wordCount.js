#!/usr/local/bin/node --harmony

// Word count, stream mode

'use strict';

var co = require('co');
var grid = require('../lib/ugrid-context.js')();

var file = process.argv[2] || '/etc/hosts';

function rpc(grid, cmd, worker, args, callback) {
	grid.request_cb(worker, {cmd: cmd, args: args}, callback);
}

function workerTask(grid, fs, readline, ml, STAGE_RAM, RAM, node, action) {
	this.run = function (callback, rank) {
		grid.devices_cb({type: 'master'}, function (err, res) {
			var msg = {id: res[0].id, cmd: 'line'};
			var rl = readline.createInterface({
				input: fs.createReadStream(node, {encoding: 'utf8'}),
				output: process.stdout, terminal: false
			});
			rl.on('line', function (line) {
				var w, words = line.split(/\W+/), res = {};
				for (var i in words) {
					w = words[i];
					if (!w) continue;
					res[w] = res[w] ? res[w] + 1: 1;
				}
				msg.data = res;
				grid.send_cb(0, msg);
			});
			rl.on('close', function () {
				msg.cmd = 'end';
				msg.data = '';
				grid.send_cb(0, msg);
			});
		});
	};
}

co(function *() {
	var words = {};
	yield grid.init();
	rpc(grid, 'setTask', grid.worker[0], {task: workerTask.toString(), node: file}, function () {
		rpc(grid, 'runTask', grid.worker[0], null, function (err, res) {
			//console.log('started');
		});
	});
	grid.on('line', function (msg) {
		for (var i in msg.data) {
			if (!words[i])
				words[i] = msg.data[i];
			else
				words[i] += msg.data[i];
		}
	});
	grid.on('end', function (msg) {
		console.log(words);
		process.exit(0);
	});
})();
