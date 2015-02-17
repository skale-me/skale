#!/usr/local/bin/node

'use strict';

var os = require('os');
var cluster = require('cluster');
var vm = require('vm');
var fs = require('fs');
var util = require('util');
var exec = require('child_process').exec;
var Connection = require('ssh2');

var UgridClient = require('../lib/ugrid-client.js');
var Lines = require('../lib/lines.js');
var UgridTask = require('../lib/ugrid-processing.js').UgridTask;

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
	for (var i = 0; i < num; i++)
		cluster.fork();
} else {
	runWorker(host, port);
}

function runWorker(host, port) {
	var RAM = {}, STAGE_RAM = {v: undefined}, task;

	var grid = new UgridClient({
		host: host,
		port: port,
		data: {
			ncpu: os.cpus().length,
			os: os.type(),
			arch: os.arch(),
			totalmem: os.totalmem(),
			hostname: os.hostname(),
			type: 'worker'
		}
	}, function (err, res) {
		console.log('id: ' + res.id + ', uuid: ' + res.uuid);
		grid.host = {uuid: res.uuid, id: res.id};
	});

	var request = {
		setTask: function (msg) {
			task = new UgridTask(grid, STAGE_RAM, RAM, msg);
			grid.reply(msg, null, 'worker ready to process task');
		},
		runTask: function (msg) {
			task.run(function(res) {
				grid.reply(msg, null, res);
			}, msg);
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

	grid.on('request', function (msg) {
		try {
			request[msg.data.cmd](msg);
		} catch (error) {
			console.error(msg.data.fun + ' error : ' + error);
			grid.reply(msg, error, null);
		}
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
	var regexp = /(\d+\. blk_-*\d+_\d+ len=\d+ repl=\d+ [[][0-9]+\.[0-9]+\.[0-9]+\.[0-9]+:[0-9]+.*\])/i;
	var data_dir = process.env.HDFS_DATA_DIR;
	var blocks = [];
	var conn = new Connection();
	
	conn.on('ready', function() {
		conn.exec(fsck_cmd, function(err, stream) {
			if (err) throw err;
			var lines = new Lines();
			stream.stdout.pipe(lines);
			lines.on('data', function(line) {
				if (line.search(regexp) == -1) return;
				var v = line.split(' ');
				var host = [];
				for (var i = 4; i < v.length; i++)
					host.push(v[i].substr(0, v[i].lastIndexOf(':')).replace('[', ''));
				blocks.push({
					blockNum: parseFloat(v[0]),
					file: data_dir + '/' + v[1].substr(0, v[1].lastIndexOf('_')),
					host: host
				});
			});
			lines.on('end', function() {
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
