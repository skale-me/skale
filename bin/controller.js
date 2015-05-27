#!/usr/local/bin/node --harmony

'use strict';

var fork = require('child_process').fork;
var trace = require('line-trace');
var Lines = require('../lib/lines.js');

var opt = require('node-getopt').create([
	['h', 'help', 'print this help text'],
	['H', 'Host=ARG', 'server hostname (default localhost)'],
	['P', 'Port=ARG', 'server port (default 12346)']
]).bindHelp().parseSystem();

var host = opt.options.Host || 'localhost';
var port = opt.options.Port || 12346;
var shells = {};
var workerControllers;

var ugrid = require('../lib/ugrid-client.js')({
	host: host,
	port: port,
	data: {type: 'controller'}
}, function (err, result) {
	ugrid.devices({type: 'worker-controller'}, 0, function (err2, res2) {
		workerControllers = res2;
	});
});

ugrid.on('start', function (msg) {
	var cmd = __dirname + '/../examples/web/' + msg.data.app + '.js';
	var prog = fork(cmd, [JSON.stringify(msg.data)], {silent: true});
	prog.stdout.on('data', function(data) {
		console.log('# stdout: ' + data);
	});
	prog.stderr.on('data', function(data) {
		console.log('# stderr: ' + data);
	});
	prog.on('close', function (code) {
		console.log('child process exited with code ' + code);
	});
});

ugrid.on('remoteClose', function (msg) {
	if (msg.data in shells) {
		console.log('remoteClose ' + msg.data + ', terminate ' + shells[msg.data].pid);
		shells[msg.data].kill();
		delete shells[msg.data];
	}
});

var sessions = {};

ugrid.on('shell', function (msg) {
	var id = msg.userId + '.' + msg.appId;
	if (sessions[id]) {
		ugrid.send(0, {cmd: 'shell', id: msg.from});
		return;
	}
	process.env.UGRID_WEBID = msg.from;
	process.env.appid = id;
	// Ask worker-controllers to fork new workers for this app
	for (var i = 0; i < workerControllers.length; i++)
		ugrid.send(workerControllers[i].uuid, {cmd: 'newapp', data: id});

	//var shell = fork(__dirname + '/ugrid-shell.js', {silent: true});
	var shell = fork(__dirname + '/../lib/copro.js', {silent: true});
	var lines = new Lines();
	shells[msg.data] = shell;
	sessions[id] = msg.from;
	ugrid.send(0, {cmd: 'shell', id: msg.from});
	ugrid.send(0, {cmd: 'notify', data: msg.data});
	shell.stdout.pipe(lines);

	lines.on('data', function (data) {
		data = JSON.parse(data);
		ugrid.send(0, {cmd: 'stdout', id: msg.from, file: data.file, data: data.data + '\n'});
	});
	shell.stderr.on('data', function (data) {
		console.log("# shell pid %d stderr: %s", shell.pid, data);
	});
	shell.on('close', function (code) {
		console.log("# shell %d exited with code: %d", shell.pid, code);
	});
	ugrid.on('stdin-' + msg.from, function (msg) {
		//shell.stdin.write(msg.data + "\n");
		shell.stdin.write(JSON.stringify(msg));
	});
	console.log('forked ugrid-shell.js pid ' + shell.pid);
});
