#!/usr/local/bin/node --harmony

'use strict';

var fork = require('child_process').fork;

var opt = require('node-getopt').create([
	['h', 'help', 'print this help text'],
	['H', 'Host=ARG', 'server hostname (default localhost)'],
	['P', 'Port=ARG', 'server port (default 12346)']
]).bindHelp().parseSystem();

var host = opt.options.Host || 'localhost';
var port = opt.options.Port || 12346;
var node = process.env.NODE || 'node';

var ugrid = require('../lib/ugrid-client.js')({
	host: host,
	port: port,
	data: {type: 'controller'}
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

ugrid.on('shell', function (msg) {
	process.env.UGRID_WEBID = msg.from;
	var shell = fork(__dirname + '/ugrid-shell.js', {silent: true});
	ugrid.send(0, {cmd: 'shell', id: msg.from});
	shell.stdout.on('data', function (data) {
		ugrid.send(0, {cmd: 'stdout', id: msg.from, data: data.toString()});
	});
	shell.stderr.on('data', function (data) {
		console.log("# shell pid %d stderr: %s", shell.pid, data);
	});
	shell.on('close', function (code) {
		console.log("# shell %d exited with code: %d", shell.pid, code);
	});
	ugrid.on('stdin-' + msg.from, function (msg) {
		shell.stdin.write(msg.data + "\n");
	});
	console.log('forked ugrid-shell.js pid ' + shell.pid);
});
