#!/usr/local/bin/node

'use strict';

var cluster = require('cluster');
var vm = require('vm');
var readline = require('readline');
var fs = require('fs');
var exec = require('child_process').exec;
var Connection = require('ssh2');

var UgridClient = require('../lib/ugrid-client.js');
var ml = require('../lib/ugrid-ml.js');

var opt = require('node-getopt').create([
	['h', 'help', 'print this help text'],
	['n', 'num=ARG', 'number of instances (default 1)'],
	['H', 'Host=ARG', 'server hostname (default localhost)'],
	['P', 'Port=ARG', 'server port (default 12346)']
]).bindHelp().parseSystem();

var host = opt.options.Host || 'localhost';
var port = opt.options.Port || 12346;
var num = opt.options.num || 1;

if (cluster.isMaster) {
	for (var i = 0; i < num; i++) {
		var t0 = cluster.fork();
		//exec('taskset -p -c ' + (i % 4) + ' ' + t0.process.pid, function (err, stdout, stdin) {
		//	if (err) throw 'taskset error'
		//});
	}
} else {
	runWorker(host, port);
}

function runWorker(host, port) {
	var RAM = {}, STAGE_RAM = [], task;

	var grid = new UgridClient({
		host: host,
		port: port,
		data: {type: 'worker'}
	});

	var request = {
		setTask: function (msg) {
			vm.runInThisContext('var Task = ' + msg.data.args.task);
			task = new Task(grid, fs, readline, ml, STAGE_RAM, RAM, msg.data.args.node, msg.data.args.action);	// jshint ignore:line
			grid.reply(msg, null, 'worker ready to process task');
		},
		runTask: function (msg) {
			task.run(function(res) {
				grid.reply(msg, null, res);
			});
		},
		shuffle: function (msg) {
			task.processShuffle(msg);
		},
		hdfs: function(msg) {
			hdfs(msg.data.args, function (err, res) {
				grid.reply(msg, err, res);
			})
		}
	};

	grid.connect_cb(function (err, res) {
		console.log('id: ' + res.id + ', uuid: ' + res.uuid);
		grid.host = {uuid: res.uuid, id: res.id};
		grid.on('request', function (msg) {
			try {request[msg.data.cmd](msg);}
			catch (error) {
				console.log(msg.data.fun + ' error : ' + error);
				grid.reply(msg, error, null);
			}
		});
	});
}

function hdfs(args, callback) {
	// Ici il faut parser la chaine de caractère args.file
	// pour récupérer l'host et tenter la connexion en ssh
	// si la connexion n'est pas possible, on passe alors ar webhdfs
	// sans exploiter la localisation des données
	// Recuperer valeur de host au sein de 'hdfs://localhost:9000/test/data.txt', arg de l'API hdfs
	var hdfsTextFile = args.file;
	var host = process.env.HDFS_HOST;
	var username = process.env.HDFS_USER;
	var privateKey = process.env.HOME + '/.ssh/id_rsa';
	var bd = process.env.HADOOP_PREFIX;
	var fsck_cmd = bd + '/bin/hadoop fsck ' + hdfsTextFile + ' -files -blocks -locations';
	var regexp = /(\d+\. blk_-*\d+_\d+ len=\d+ repl=\d+ [[][0-9]+\.[0-9]+\.[0-9]+\.[0-9]+:[0-9]+\])/i;
	var data_dir = process.env.HDFS_DATA_DIR;
	var blocks = [];
	var conn = new Connection();
	conn.on('ready', function() {
		conn.exec(fsck_cmd, function(err, stream) {
			if (err) throw err;
			var rl = readline.createInterface({
				input: stream.stdout,
				output: process.stdout,
				terminal: false
			});
			stream.stdout.setEncoding('utf8');
			stream.stderr.setEncoding('utf8');
			rl.on("line", function(line) {
				if (line.search(regexp) == -1) return;
				var v = line.split(' ');
				blocks.push({
					blockNum: parseFloat(v[0]),
					file: data_dir + '/' + v[1].substr(0, v[1].lastIndexOf('_')),
					host: v[4].substr(0, v[4].lastIndexOf(':')).replace('[', '')
				});
			});
			rl.on('close', function() {
				conn.end();
				callback(null, blocks);
			});
		})
	}).connect({
		host: host,
		username: username,
		privateKey: fs.readFileSync(privateKey)
	});
}